use crate::doc::Doc;
use tantivy::schema::Schema;
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument};

pub struct IndexBuilder {
    schema: Schema,
    index: Index,
}

impl IndexBuilder {
    pub fn new(schema: Schema) -> Self {
        let index = Index::create_in_ram(schema.clone());

        Self { schema, index }
    }

    const MEMORY_BUDGET_IN_BYTES: usize = 50_000_000;
    pub fn add_docs(self, docs: &[Doc]) -> tantivy::Result<Self> {
        let mut index_writer: IndexWriter = self.index.writer(Self::MEMORY_BUDGET_IN_BYTES)?;

        let id_field = self.schema.get_field("id")?;
        let title_field = self.schema.get_field("title")?;
        let body_field = self.schema.get_field("body")?;

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
        ImmutableIndex::new(self.schema, self.index)
    }
}

pub struct ImmutableIndex {
    schema: Schema,
    index: Index,
}

impl ImmutableIndex {
    pub fn new(schema: Schema, index: Index) -> Self {
        Self { schema, index }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn reader(&self) -> tantivy::Result<IndexReader> {
        self.index.reader()
    }
}
