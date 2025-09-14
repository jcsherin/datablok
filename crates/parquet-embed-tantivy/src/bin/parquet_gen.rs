use clap::Parser;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringBuilder, UInt64Builder};
use datafusion::arrow::datatypes::SchemaRef;
use fs::create_dir_all;
use itertools::Itertools;
use log::trace;
use parquet_embed_tantivy::common::{setup_logging, Config};
use parquet_embed_tantivy::custom_index::manifest::DraftManifest;
use parquet_embed_tantivy::data_generator::title::TitleGenerator;
use parquet_embed_tantivy::data_generator::words::SELECTIVITY_PHRASES;
use parquet_embed_tantivy::doc::{ArrowDocSchema, Doc, DocTantivySchema};
use parquet_embed_tantivy::error::Error::FieldNotFound;
use parquet_embed_tantivy::error::Result;
use parquet_embed_tantivy::index::{TantivyDocIndex, TantivyDocIndexBuilder};
use parquet_embed_tantivy::writer::ParquetWriter;
use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::query::{BooleanQuery, Occur, TermQuery};
use tantivy::schema::IndexRecordOption;
use tantivy::Term;

#[derive(Parser, Debug)]
#[command(name = "parquet_gen")]
#[command(about = "Generate input parquet files")]
#[command(
    long_about = "Generate two identical parquet files, with a full-text index embedded in one of them."
)]
#[command(version)]
struct Args {
    /// Total number of rows to generate
    #[arg(short, long, default_value_t = 100)]
    target_size: u64,

    /// Size of each RecordBatch
    #[arg(short, long, default_value_t = 8192)]
    record_batch_size: usize,

    /// Output directory for generated parquet files
    #[arg(short, long, default_value = "output")]
    output_directory: PathBuf,
}

fn get_data_source_iter(size: u64) -> impl Iterator<Item = Doc> {
    (0..size)
        .zip(TitleGenerator::new())
        .map(|(id, title)| Doc::new(id, title))
}

fn create_tantivy_doc_index(docs: impl Iterator<Item = Doc>) -> Result<TantivyDocIndex> {
    let config = Config::default();
    let schema = Arc::new(DocTantivySchema::new(&config).into_schema());

    let index = TantivyDocIndexBuilder::new(schema.clone())
        .write_docs(config.index_writer_memory_budget_in_bytes, docs)?
        .build();

    Ok(index)
}

fn phrase_queries() -> Result<Vec<BooleanQuery>> {
    let config = Config::default();
    let schema = Arc::new(DocTantivySchema::new(&config).into_schema());

    let title_field = schema
        .get_field("title")
        .map_err(|_| FieldNotFound(String::from("title")))?;

    Ok(SELECTIVITY_PHRASES
        .iter()
        .map(|(text, _)| {
            let term_query = Box::new(TermQuery::new(
                Term::from_field_text(title_field, text),
                IndexRecordOption::Basic,
            ));

            BooleanQuery::new(vec![(Occur::Should, term_query)])
        })
        .collect::<Vec<_>>())
}

const MAX_TITLE_SIZE: usize = 120;
fn create_parquet_file(
    output_directory: &Path,
    filename: &str,
    schema: SchemaRef,
    record_batch_size: usize,
    data_source: impl Iterator<Item = Doc>,
    tantivy_doc_index: Option<&TantivyDocIndex>,
) -> Result<()> {
    if !output_directory.exists() {
        create_dir_all(output_directory)?;
    }
    let output_path = output_directory.join(filename);

    let mut writer = ParquetWriter::try_new(output_path, schema.clone(), None)?;

    let mut id_builder = UInt64Builder::with_capacity(record_batch_size);
    let mut title_builder =
        StringBuilder::with_capacity(record_batch_size, record_batch_size * MAX_TITLE_SIZE);

    for chunk in &data_source.chunks(record_batch_size) {
        for doc in chunk {
            id_builder.append_value(doc.id());
            title_builder.append_value(doc.title());
        }

        let id_array = Arc::new(id_builder.finish()) as ArrayRef;
        let title_array = Arc::new(title_builder.finish()) as ArrayRef;

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, title_array])?;
        writer.write_record_batch(&batch)?
    }

    if let Some(index) = tantivy_doc_index {
        let (header, data_block) = DraftManifest::try_new(index)?.try_into(index)?;
        writer.write_index_and_close(header, data_block)?;
    } else {
        writer.close()?;
    }

    Ok(())
}

fn main() -> Result<()> {
    setup_logging();

    let args = Args::parse();

    // Tantivy Full-Text Index
    trace!("Creating: full-text index");
    let tantivy_doc_index = create_tantivy_doc_index(get_data_source_iter(args.target_size))?;

    let reader = tantivy_doc_index.reader()?;
    let searcher = reader.searcher();

    // How many times do the control terms occur?
    trace!("Histogram of control search phrases");
    for query in phrase_queries()? {
        let count = searcher.search(&query, &tantivy::collector::Count)?;
        trace!("query: {query:?}, count: {count}");
    }

    // Create a normal Parquet file from input data source
    trace!("Creating a regular parquet file");
    create_parquet_file(
        args.output_directory.as_ref(),
        "titles.parquet",
        ArrowDocSchema::default().deref().clone(),
        args.record_batch_size,
        get_data_source_iter(args.target_size),
        None,
    )?;

    // Create a Parquet file with embedded full-text index
    trace!("Creating a parquet file with embedded full-text index");
    create_parquet_file(
        args.output_directory.as_ref(),
        "titles_with_fts_index.parquet",
        ArrowDocSchema::default().deref().clone(),
        args.record_batch_size,
        get_data_source_iter(args.target_size),
        Some(&tantivy_doc_index),
    )?;
    Ok(())
}
