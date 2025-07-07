mod doc;

use crate::doc::{Doc, DocSchema, examples};
use log::info;
use std::collections::HashSet;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, Value};
use tantivy::{DocAddress, Index, IndexWriter, Searcher, TantivyDocument, Term};

fn main() -> tantivy::Result<()> {
    setup_logging();

    let schema = DocSchema::default().into_schema();
    let original_docs = examples();

    // Creates an in-memory index using `RamDirectory`
    let index = Index::create_in_ram(DocSchema::default().into_schema());

    index_docs(&index, &schema, original_docs)?;

    // Entry point to read and search the index.
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let query = create_boolean_query(&schema);

    // Count document matches for boolean query
    info!("Matches count: {}", searcher.search(&query, &Count)?);

    let fruit = searcher.search(&query, &DocSetCollector)?;
    let doc_ids = into_doc_ids(&schema, &searcher, fruit)?;

    for id in doc_ids {
        if let Some(original_doc) = original_docs.get(id as usize) {
            info!("Matched Original Doc [ID={id}]: {original_doc:?}");
        } else {
            // This is a safer way to handle a potential mismatch.
            log::warn!(
                "Found ID {id} in index, but it was out of bounds for the original collection."
            );
        }
    }

    Ok(())
}

/// Maps search results into stored `id` fields
fn into_doc_ids(
    schema: &Schema,
    searcher: &Searcher,
    fruit: HashSet<DocAddress>,
) -> tantivy::Result<Vec<u64>> {
    let id_field = schema.get_field("id")?;

    let doc_ids = fruit
        .into_iter()
        .filter_map(|doc_address| {
            searcher
                .doc::<TantivyDocument>(doc_address)
                .ok()
                .and_then(|doc| doc.get_first(id_field).and_then(|v| v.as_u64()))
        })
        .collect();

    Ok(doc_ids)
}

/// Make a boolean query equivalent to: title:+diary title:-girl
fn create_boolean_query(schema: &Schema) -> BooleanQuery {
    let title_field = schema.get_field("title").unwrap();

    // A term query matches all the documents containing a specific term.
    // The `Query` trait defines a set of documents and a way to score those documents.
    let girl_term_query: Box<dyn Query> = Box::new(TermQuery::new(
        Term::from_field_text(title_field, "girl"),
        IndexRecordOption::Basic, // records only the `DocId`s
    ));
    let diary_term_query: Box<dyn Query> = Box::new(TermQuery::new(
        Term::from_field_text(title_field, "diary"),
        IndexRecordOption::Basic,
    ));

    let subqueries = vec![
        (Occur::Must, diary_term_query.box_clone()),
        (Occur::MustNot, girl_term_query.box_clone()),
    ];

    BooleanQuery::new(subqueries)
}

const MEMORY_BUDGET_IN_BYTES: usize = 50_000_000;

fn index_docs(index: &Index, schema: &Schema, docs: &[Doc]) -> tantivy::Result<()> {
    let mut index_writer: IndexWriter = index.writer(MEMORY_BUDGET_IN_BYTES)?;

    let id_field = schema.get_field("id")?;
    let title_field = schema.get_field("title")?;
    let body_field = schema.get_field("body")?;

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
    Ok(())
}

/// Initializes the logger.
fn setup_logging() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
}
