use once_cell::sync::Lazy;
use std::sync::atomic::AtomicU64;
use tantivy::schema::{INDEXED, STORED, Schema, SchemaBuilder, TEXT};

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
impl Default for DocSchema {
    fn default() -> Self {
        let mut schema_builder = SchemaBuilder::new();
        schema_builder.add_u64_field("id", INDEXED | STORED);
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("body", TEXT);

        Self(schema_builder.build())
    }
}

impl DocSchema {
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

pub fn examples() -> &'static Vec<Doc> {
    &DOCS
}
