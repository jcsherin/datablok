use crate::common::Config;
use crate::data_generator::title::TitleGenerator;
use crate::error::Result;
use datafusion::arrow::array::{ArrayRef, StringBuilder, UInt64Builder};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::arrow::array::{RecordBatch, StringArray, UInt64Array};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tantivy::schema::{Field, Schema, SchemaBuilder, Value, INDEXED, STORED, TEXT};
use tantivy::{DocAddress, Searcher, TantivyDocument};

/// A global counter to ensure each ID is unique for this process.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, PartialEq)]
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

pub struct DocTantivySchema(Schema);
impl DocTantivySchema {
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

static DOCS_DATA_SOURCE: Lazy<Vec<Doc>> = Lazy::new(|| {
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

pub fn tiny_docs() -> &'static [Doc] {
    &DOCS_DATA_SOURCE
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

pub struct ArrowDocSchema(SchemaRef);

impl Default for ArrowDocSchema {
    fn default() -> Self {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let id_field = Field::new("id", DataType::UInt64, false);
        let title_field = Field::new("title", DataType::Utf8, true);
        let body_field = Field::new("body", DataType::Utf8, true);

        ArrowDocSchema(std::sync::Arc::new(Schema::new(vec![
            id_field,
            title_field,
            body_field,
        ])))
    }
}

impl Deref for ArrowDocSchema {
    type Target = SchemaRef;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(dead_code)]
const MAX_TITLE_SIZE: usize = 120;
#[allow(dead_code)]
const MAX_BODY_SIZE: usize = 80;
#[allow(dead_code)]
fn generate_record_batch(start_id: u64, size: usize, schema: SchemaRef) -> Result<RecordBatch> {
    let mut id_builder = UInt64Builder::with_capacity(size);
    let mut title_builder = StringBuilder::with_capacity(size, size * MAX_TITLE_SIZE);
    let mut body_builder = StringBuilder::with_capacity(size, size * MAX_BODY_SIZE);

    for (i, title) in (0..size).zip(TitleGenerator::default()) {
        id_builder.append_value(start_id + i as u64);
        title_builder.append_value(title);

        if rand::random_bool(0.5) {
            body_builder.append_value("This is a sample body text")
        } else {
            body_builder.append_null();
        }
    }

    let id_array = Arc::new(id_builder.finish()) as ArrayRef;
    let title_array = Arc::new(title_builder.finish()) as ArrayRef;
    let body_array = Arc::new(body_builder.finish()) as ArrayRef;

    RecordBatch::try_new(schema, vec![id_array, title_array, body_array]).map_err(Into::into)
}

pub fn generate_record_batch_for_docs(schema: SchemaRef, source: &[Doc]) -> Result<RecordBatch> {
    let id_array: ArrayRef = Arc::new(source.iter().map(|doc| doc.id()).collect::<UInt64Array>());
    let title_array: ArrayRef = Arc::new(
        source
            .iter()
            .map(|doc| Some(doc.title()))
            .collect::<StringArray>(),
    );
    let body_array: ArrayRef =
        Arc::new(source.iter().map(|doc| doc.body()).collect::<StringArray>());

    RecordBatch::try_new(schema, vec![id_array, title_array, body_array]).map_err(Into::into)
}
