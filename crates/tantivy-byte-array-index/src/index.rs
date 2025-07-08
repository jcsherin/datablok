use crate::config::Config;
use crate::doc::Doc;
use crate::error::Result;
use tantivy::schema::Schema;
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument};

pub struct IndexBuilder {
    index: Index,
}

impl IndexBuilder {
    pub fn new(schema: Schema) -> Self {
        Self {
            index: Index::create_in_ram(schema),
        }
    }

    pub fn add_docs(self, config: &Config, docs: &[Doc]) -> Result<Self> {
        let mut index_writer: IndexWriter = self
            .index
            .writer(config.index_writer_memory_budget_in_bytes)?;

        let schema = self.index.schema();
        let id_field = schema.get_field(config.id_field_name.as_str())?;
        let title_field = schema.get_field(config.title_field_name.as_str())?;
        let body_field = schema.get_field(config.body_field_name.as_str())?;

        for doc in docs {
            let mut tantivy_doc = TantivyDocument::default();
            tantivy_doc.add_u64(id_field, doc.id());
            tantivy_doc.add_text(title_field, doc.title());
            if let Some(body) = doc.body() {
                tantivy_doc.add_text(body_field, body);
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
}
