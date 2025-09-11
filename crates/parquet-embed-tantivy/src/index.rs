use crate::common::SchemaFields;
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
use datafusion_expr::{col, lit, Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::ExecutionPlan;
use log::info;
use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::collector::DocSetCollector;
use tantivy::directory::ManagedDirectory;
use tantivy::query::{PhraseQuery, Query};
use tantivy::schema::{Schema, Value};
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument, Term};

pub struct IndexBuilder {
    index: Index,
}

impl IndexBuilder {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            index: Index::create_in_ram(schema.as_ref().clone()),
        }
    }

    pub fn index_and_commit(
        self,
        memory_budget_in_bytes: usize,
        fields: &SchemaFields,
        docs: &[Doc],
    ) -> Result<Self> {
        let mut index_writer: IndexWriter = self.index.writer(memory_budget_in_bytes)?;

        for doc in docs {
            let mut tantivy_doc = TantivyDocument::default();
            tantivy_doc.add_u64(fields.id, doc.id());
            tantivy_doc.add_text(fields.title, doc.title());
            if let Some(body) = doc.body() {
                tantivy_doc.add_text(fields.body, body);
            }

            index_writer.add_document(tantivy_doc)?;
        }

        index_writer.commit()?;

        Ok(self)
    }

    pub fn build(self) -> ImmutableIndex {
        ImmutableIndex::new(self.index)
    }
}

pub struct ImmutableIndex {
    index: Index,
}

impl ImmutableIndex {
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
        let mut file = File::open(path)?;

        let reader = SerializedFileReader::new(file.try_clone()?)?;

        let index_offset = Self::try_index_offset(path, reader)?;

        file.seek(SeekFrom::Start(index_offset))?;
        let serialized_header = Header::from_reader(&file)?;

        let mut data_block_buffer = vec![0u8; serialized_header.total_data_block_size as usize];
        file.read_exact(&mut data_block_buffer)?;
        let serialized_data = DataBlock::new(data_block_buffer);

        Ok(ReadOnlyArchiveDirectory::new(
            serialized_header,
            serialized_data,
        ))
    }

    fn try_index_offset(path: &Path, reader: SerializedFileReader<File>) -> Result<u64> {
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
        let mut phrase: Option<&str> = None;

        // Currently handles only a single wildcard LIKE query on the `title` column. A generalized
        // implementation will use: [`PruningPredicate`]
        //
        // [`PruningPredicate`]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/pruning/struct.PruningPredicate.html
        if filters.len() == 1 {
            if let Expr::Like(like) = filters.first().unwrap() {
                if !like.negated {
                    if let (
                        Expr::Column(col),
                        Expr::Literal(ScalarValue::Utf8(Some(pattern)), None),
                    ) = (&*like.expr, &*like.pattern)
                    {
                        info!("Column: {}, Pattern: {pattern}", col.name);
                        if col.name == "title" {
                            if let Some(inner) =
                                pattern.strip_prefix('%').and_then(|s| s.strip_suffix('%'))
                            {
                                phrase = Some(inner);
                            }
                        }
                    }
                }
            }
        }

        let mut matching_doc_ids = Vec::new();
        if let Some(inner) = phrase {
            info!("phrase: {inner}");

            let title_field = self
                .index_schema
                .get_field("title")
                .map_err(|e| DataFusionError::External(e.into()))?;
            let id_field = self
                .index_schema
                .get_field("id")
                .map_err(|e| DataFusionError::External(e.into()))?;
            let title_phrase_query = Box::new(PhraseQuery::new(
                inner
                    .split_whitespace()
                    .map(|s| Term::from_field_text(title_field, s))
                    .collect(),
            )) as Box<dyn Query>;

            let reader = self
                .index
                .reader()
                .map_err(|e| DataFusionError::External(e.into()))?;
            let searcher = reader.searcher();

            if let Ok(matches) = searcher.search(&*title_phrase_query, &DocSetCollector) {
                let matched_ids = matches
                    .iter()
                    .filter_map(|doc_address| {
                        searcher
                            .doc::<TantivyDocument>(*doc_address)
                            .ok()? // discard the error if doc doesn't exist in the index
                            .get_first(id_field)
                            .and_then(|v| v.as_u64())
                    })
                    .collect::<Vec<_>>();

                matching_doc_ids.extend(matched_ids);
            }
        }

        info!("Matching doc ids from full-text index: {matching_doc_ids:?}");

        // constructing the `id IN (...)` expression to pushdown into parquet file
        let ids: Vec<Expr> = matching_doc_ids.iter().map(|doc_id| lit(*doc_id)).collect();
        let id_filter = col("id").in_list(ids, false);
        info!("filter expr: {id_filter}");

        let df_schema = DFSchema::try_from(self.arrow_schema.clone())?;
        let physical_predicate =
            create_physical_expr(&id_filter, &df_schema, state.execution_props())?;
        info!("physical expr: {physical_predicate}");

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let source = Arc::new(
            ParquetSource::default()
                .with_enable_page_index(true)
                .with_predicate(physical_predicate)
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
