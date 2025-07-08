use crate::common::SchemaFields;
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
}
