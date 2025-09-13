use clap::Parser;
use log::trace;
use parquet_embed_tantivy::common::{setup_logging, Config, SchemaFields};
use parquet_embed_tantivy::data_generator::title::TitleGenerator;
use parquet_embed_tantivy::data_generator::words::SELECTIVITY_PHRASES;
use parquet_embed_tantivy::doc::{Doc, DocTantivySchema};
use parquet_embed_tantivy::error::Error::FieldNotFound;
use parquet_embed_tantivy::error::Result;
use parquet_embed_tantivy::index::{TantivyDocIndex, TantivyDocIndexBuilder};
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
}

fn create_data_source(size: u64) -> Vec<Doc> {
    (0..size)
        .zip(TitleGenerator::new())
        .map(|(id, title)| Doc::new(id, title))
        .collect()
}

fn create_tantivy_doc_index(docs: &[Doc]) -> Result<TantivyDocIndex> {
    let config = Config::default();
    let schema = Arc::new(DocTantivySchema::new(&config).into_schema());

    let fields = SchemaFields::new(schema.clone(), &config)?;

    let index = TantivyDocIndexBuilder::new(schema.clone())
        .index_and_commit(config.index_writer_memory_budget_in_bytes, &fields, docs)?
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

fn main() -> Result<()> {
    setup_logging();

    let args = Args::parse();

    let data_source = create_data_source(args.target_size);
    let tantivy_doc_index = create_tantivy_doc_index(&data_source)?;

    let reader = tantivy_doc_index.reader()?;
    let searcher = reader.searcher();

    for query in phrase_queries()? {
        let count = searcher.search(&query, &tantivy::collector::Count)?;
        trace!("query: {query:?}, count: {count}");
    }

    Ok(())
}
