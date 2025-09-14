use crate::data_generator::title::TitleGenerator;
use crate::error::Result;
use datafusion::arrow::array::{ArrayRef, StringBuilder, UInt64Builder};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::arrow::array::{RecordBatch, StringArray, UInt64Array};
use std::ops::Deref;
use std::sync::Arc;
use tantivy::schema::{Schema, SchemaBuilder, INDEXED, STORED, TEXT};

#[derive(Debug, PartialEq)]
pub struct Doc {
    id: u64,
    title: String,
}

impl Doc {
    pub fn new(id: u64, title: String) -> Self {
        Self { id, title }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn title(&self) -> &str {
        &self.title
    }
}

#[derive(Debug)]
pub struct DocTantivySchema(Schema);
impl DocTantivySchema {
    pub fn new() -> Self {
        let mut schema_builder = SchemaBuilder::new();

        schema_builder.add_u64_field("id", INDEXED | STORED);
        schema_builder.add_text_field("title", TEXT);

        Self(schema_builder.build())
    }

    pub fn into_schema(self) -> Schema {
        self.0
    }
}

impl Default for DocTantivySchema {
    fn default() -> Self {
        Self::new()
    }
}

pub fn tiny_docs() -> impl Iterator<Item = Doc> {
    let docs = vec![
        "The Name of the Wind".to_string(),
        "The Diary of Muadib".to_string(),
        "A Dairy Cow".to_string(),
        "A Dairy Cow".to_string(),
        "The Diary of a Young Girl".to_string(),
    ];

    (0..docs.len())
        .zip(docs)
        .map(|(id, title)| Doc::new(id as u64, title))
}

pub struct ArrowDocSchema(SchemaRef);

impl Default for ArrowDocSchema {
    fn default() -> Self {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let id_field = Field::new("id", DataType::UInt64, false);
        let title_field = Field::new("title", DataType::Utf8, true);

        ArrowDocSchema(std::sync::Arc::new(Schema::new(vec![
            id_field,
            title_field,
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

    for (i, title) in (0..size).zip(TitleGenerator::default()) {
        id_builder.append_value(start_id + i as u64);
        title_builder.append_value(title);
    }

    let id_array = Arc::new(id_builder.finish()) as ArrayRef;
    let title_array = Arc::new(title_builder.finish()) as ArrayRef;

    RecordBatch::try_new(schema, vec![id_array, title_array]).map_err(Into::into)
}

pub fn generate_record_batch_for_docs(schema: SchemaRef, source: &[Doc]) -> Result<RecordBatch> {
    let id_array: ArrayRef = Arc::new(source.iter().map(|doc| doc.id()).collect::<UInt64Array>());
    let title_array: ArrayRef = Arc::new(
        source
            .iter()
            .map(|doc| Some(doc.title()))
            .collect::<StringArray>(),
    );

    RecordBatch::try_new(schema, vec![id_array, title_array]).map_err(Into::into)
}
