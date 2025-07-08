use crate::config::Config;
use crate::error::Result;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use tantivy::schema::{Field, INDEXED, STORED, Schema, SchemaBuilder, TEXT, Value};
use tantivy::{DocAddress, Searcher, TantivyDocument};

/// A global counter to ensure each ID is unique for this process.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct Doc {
    id: u64,
    title: String,
    body: Option<String>,
}

impl Doc {
    pub fn new(title: String, body: Option<String>) -> Self {
        Self {
            id: NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            title,
            body,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn body(&self) -> Option<&str> {
        self.body.as_deref()
    }
}

pub struct DocSchema(Schema);
impl DocSchema {
    pub fn new(config: &Config) -> Self {
        let mut schema_builder = SchemaBuilder::new();

        schema_builder.add_u64_field(config.id_field_name.as_str(), INDEXED | STORED);
        schema_builder.add_text_field(config.title_field_name.as_str(), TEXT);
        schema_builder.add_text_field(config.body_field_name.as_str(), TEXT);

        Self(schema_builder.build())
    }

    pub fn into_schema(self) -> Schema {
        self.0
    }
}

static DOCS: Lazy<Vec<Doc>> = Lazy::new(|| {
    vec![
        ("The Name of the Wind".to_string(), None),
        ("The Diary of Muadib".to_string(), None),
        ("A Dairy Cow".to_string(), Some("hidden".to_string())),
        ("A Dairy Cow".to_string(), Some("found".to_string())),
        ("The Diary of a Young Girl".to_string(), None),
    ]
    .into_iter()
    .map(|(title, body)| Doc::new(title, body))
    .collect()
});

pub fn examples() -> &'static [Doc] {
    &DOCS
}

pub trait DocIdMapper<'a> {
    fn get_doc_id(&self, doc_address: DocAddress) -> Result<Option<u64>>;

    fn get_original_doc(&self, doc_id: u64) -> Option<&'a Doc>;
}

pub struct DocMapper<'a> {
    searcher: &'a Searcher,
    id_field: Field,
    doc_map: HashMap<u64, &'a Doc>,
}

impl<'a> DocMapper<'a> {
    pub fn new(searcher: &'a Searcher, config: &Config, docs: &'a [Doc]) -> Self {
        let id_field = searcher
            .schema()
            .get_field(config.id_field_name.as_str())
            .unwrap();
        let doc_map: HashMap<u64, &Doc> = docs.iter().map(|doc| (doc.id(), doc)).collect();

        Self {
            searcher,
            id_field,
            doc_map,
        }
    }
}

impl<'a> DocIdMapper<'a> for DocMapper<'a> {
    fn get_doc_id(&self, doc_address: DocAddress) -> Result<Option<u64>> {
        Ok(self
            .searcher
            .doc::<TantivyDocument>(doc_address)?
            .get_first(self.id_field)
            .and_then(|v| v.as_u64()))
    }

    fn get_original_doc(&self, doc_id: u64) -> Option<&'a Doc> {
        self.doc_map.get(&doc_id).copied()
    }
}
