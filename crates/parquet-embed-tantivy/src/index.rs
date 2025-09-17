use crate::custom_index::data_block::DataBlock;
use crate::custom_index::header::Header;
use crate::directory::ReadOnlyArchiveDirectory;
use crate::doc::Doc;
use crate::error::Result;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::physical_expr::create_physical_expr;
use datafusion_catalog::TableProvider;
use datafusion_common::{DFSchema, DataFusionError, ScalarValue};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{col, lit, Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::directory::ManagedDirectory;
use tantivy::query::PhraseQuery;
use tantivy::schema::{Field, Schema, Value};
use tantivy::{Index, IndexReader, TantivyDocument, Term};
use tracing::trace;

pub const INDEX_WRITER_MEMORY_BUDGET_IN_BYTES: usize = 50_000_000;

pub struct TantivyDocIndexBuilder {
    index: Index,
}

impl TantivyDocIndexBuilder {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            index: Index::create_in_ram(schema.as_ref().clone()),
        }
    }

    pub fn write_docs(self, docs: impl Iterator<Item = Doc>) -> Result<Self> {
        let mut writer = self.index.writer(INDEX_WRITER_MEMORY_BUDGET_IN_BYTES)?;
        let schema = self.index.schema();

        let id_field = schema.get_field("id")?;
        let title_field = schema.get_field("title")?;

        for doc in docs {
            let mut item = TantivyDocument::default();

            item.add_u64(id_field, doc.id());
            item.add_text(title_field, doc.title());

            writer.add_document(item)?;
        }

        writer.commit()?;

        Ok(self)
    }
    pub fn build(self) -> TantivyDocIndex {
        TantivyDocIndex::new(self.index)
    }
}

pub struct TantivyDocIndex {
    index: Index,
}

impl TantivyDocIndex {
    pub fn new(index: Index) -> Self {
        Self { index }
    }

    pub fn reader(&self) -> Result<IndexReader> {
        Ok(self.index.reader()?)
    }

    pub fn schema(&self) -> Schema {
        self.index.schema()
    }

    pub fn directory(&self) -> &ManagedDirectory {
        self.index.directory()
    }
}

pub const FULL_TEXT_INDEX_KEY: &str = "tantivy_index_offset";
pub struct FullTextIndex {
    path: PathBuf,
    index: Index,
    index_schema: Arc<Schema>,
    arrow_schema: SchemaRef,
}

impl FullTextIndex {
    pub fn try_open(
        path: &Path,
        index_schema: Arc<Schema>,
        arrow_schema: SchemaRef,
    ) -> Result<Self> {
        let _span = tracing::span!(tracing::Level::TRACE, "open").entered();
        let dir = Self::try_read_directory(path)?;
        let index = Index::open_or_create(dir, index_schema.as_ref().clone())?;

        Ok(Self {
            path: path.to_path_buf(),
            index,
            index_schema,
            arrow_schema,
        })
    }

    fn try_read_directory(path: &Path) -> Result<ReadOnlyArchiveDirectory> {
        let _span = tracing::span!(tracing::Level::TRACE, "read_directory").entered();
        let mut file = File::open(path)?;

        let reader = SerializedFileReader::new(file.try_clone()?)?;

        let index_offset = Self::try_index_offset(path, reader)?;

        file.seek(SeekFrom::Start(index_offset))?;
        let header = {
            let _span = tracing::span!(tracing::Level::TRACE, "deserialize_header").entered();

            Header::from_reader(&file)?
        };

        let data_block = {
            let _span = tracing::span!(tracing::Level::TRACE, "deserialize_data_block",).entered();

            let mut data_block_buffer = vec![0u8; header.total_data_block_size as usize];
            file.read_exact(&mut data_block_buffer)?;

            DataBlock::new(data_block_buffer)
        };

        Ok(ReadOnlyArchiveDirectory::new(header, data_block))
    }

    fn try_index_offset(path: &Path, reader: SerializedFileReader<File>) -> Result<u64> {
        let _span = tracing::span!(tracing::Level::TRACE, "index_offset").entered();

        let index_offset = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "Could not find key_value_metadata in file: {}",
                    path.display()
                ))
            })?
            .iter()
            .find(|kv| kv.key == FULL_TEXT_INDEX_KEY)
            .and_then(|kv| kv.value.as_deref())
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "Could not find a valid value for key `{}` in file {}",
                    FULL_TEXT_INDEX_KEY,
                    path.display()
                ))
            })?
            .parse::<u64>()
            .map_err(|e| ParquetError::General(e.to_string()))?;

        Ok(index_offset)
    }
}

impl Debug for FullTextIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullTextIndex")
            .field("path", &self.path)
            .field("index", &self.index)
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

impl FullTextIndex {
    /// Extract inner search phrase from within leading and trailing wildcards.
    ///
    /// Returns `Some(phrase)` or None.
    ///
    /// This implementation is concrete and not general purpose. It handles only a single filter
    /// expression from a text column named: `title`.
    fn extract_search_phrase(filter: &Expr) -> Option<&str> {
        match filter {
            Expr::Like(like) if !like.negated => {
                if let (Expr::Column(col), Expr::Literal(ScalarValue::Utf8(Some(pattern)), None)) =
                    (&*like.expr, &*like.pattern)
                {
                    if col.name == "title" {
                        return pattern
                            .strip_prefix('%')
                            .and_then(|rest| rest.strip_suffix('%'));
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn get_index_field(&self, name: &str) -> Result<Field> {
        Ok(self
            .index_schema
            .get_field(name)
            .map_err(|e| DataFusionError::External(e.into()))?)
    }

    fn create_index_query(&self, phrase: &str) -> Result<PhraseQuery> {
        let title_field = self.get_index_field("title")?;

        let terms = phrase
            .split_whitespace()
            .map(|part| Term::from_field_text(title_field, part))
            .collect::<Vec<_>>();

        Ok(PhraseQuery::new(terms))
    }

    fn search_index(&self, query: PhraseQuery) -> Result<(Vec<u64>, usize)> {
        let reader = self
            .index
            .reader()
            .map_err(|e| DataFusionError::External(e.into()))?;
        let searcher = reader.searcher();

        let (hits, count) = searcher
            .search(&query, &(DocSetCollector, Count))
            .map_err(|e| DataFusionError::External(e.into()))?;

        let id_field = self.get_index_field("id")?;

        // TODO: retrieve the id (FAST) without loading the full document
        let hits = hits
            .iter()
            .flat_map(|addr| {
                let indexed_doc = searcher.doc::<TantivyDocument>(*addr).ok()?; // Discards the error if doc is not found in the index
                let id = indexed_doc.get_first(id_field).and_then(|x| x.as_u64());

                id
            })
            .collect::<Vec<_>>();

        Ok((hits, count))
    }

    /// Creates a pushdown filter expression
    ///
    /// The matching ids from full-text search is used to construct an `id IN (...)` filter
    /// expression.
    fn create_predicate(
        &self,
        props: &ExecutionProps,
        ids: &[u64],
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        let list = ids.iter().map(|id| lit(*id)).collect::<Vec<_>>();
        let filter = col("id").in_list(list, false);

        let schema = DFSchema::try_from(self.arrow_schema.clone())?;

        let predicate = create_physical_expr(&filter, &schema, props)?;

        Ok(predicate)
    }
}

#[async_trait]
impl TableProvider for FullTextIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let _span = tracing::span!(tracing::Level::TRACE, "scan").entered();

        let phrase = if filters.len() == 1 {
            Self::extract_search_phrase(filters.first().unwrap())
        } else {
            unimplemented!("Supports only a single LIKE filter expression.")
        };

        let matching_doc_ids = match phrase {
            None => {
                trace!("matching count: 0");
                vec![]
            }
            Some(inner) => {
                let title_phrase_query = self
                    .create_index_query(inner)
                    .map_err(|e| DataFusionError::External(e.into()))?;

                let (hits, count) = self
                    .search_index(title_phrase_query.clone())
                    .map_err(|e| DataFusionError::External(e.into()))?;

                trace!("matching count: {count}");
                hits
            }
        };

        if matching_doc_ids.is_empty() {
            trace!("Skipping parquet data source (EmptyExec).");
            return Ok(Arc::new(EmptyExec::new(self.arrow_schema.clone())));
        }

        let predicate = self.create_predicate(state.execution_props(), &matching_doc_ids)?;

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let source = Arc::new(
            ParquetSource::default()
                .with_enable_page_index(true)
                .with_predicate(predicate)
                .with_pushdown_filters(true),
        );

        let absolute_path = std::fs::canonicalize(&self.path)?;
        let len = std::fs::metadata(&absolute_path)?.len();
        let partitioned_file = PartitionedFile::new(absolute_path.to_string_lossy(), len);

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, self.schema().clone(), source)
                .with_file(partitioned_file)
                .build();

        Ok(Arc::new(DataSourceExec::new(Arc::new(file_scan_config))))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}
