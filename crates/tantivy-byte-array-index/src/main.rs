mod doc;

use crate::doc::{DocSchema, examples};
use log::info;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Value};
use tantivy::{Index, IndexWriter, TantivyDocument, Term};

fn main() -> tantivy::Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let schema = DocSchema::default().into_schema();
    let id_field = schema.get_field("id")?;
    let title_field = schema.get_field("title")?;
    let body_field = schema.get_field("body")?;

    let original_docs = examples();

    // Creates an in-memory index using `RamDirectory`
    let index = Index::create_in_ram(DocSchema::default().into_schema());

    {
        let mut index_writer: IndexWriter = index.writer(50_000_000)?;

        for doc in original_docs {
            let mut tantivy_doc = TantivyDocument::default();
            tantivy_doc.add_u64(id_field, doc.id());
            tantivy_doc.add_text(title_field, doc.title());
            if let Some(body) = doc.body() {
                tantivy_doc.add_text(body_field, body);
            }

            index_writer.add_document(tantivy_doc)?;
        }

        index_writer.commit()?;
    }

    // Entry point to read and search the index.
    let reader = index.reader()?;

    // Holds a list of `SegmentReader`s ready for search.
    // The `SegmentReader` is the entry point to access all data structures of the `Segment`.
    //   - term dictionary
    //   - postings
    //   - store
    //   - fast field readers
    //   - field norm reader
    let searcher = reader.searcher();

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

    // Make a boolean query equivalent to:
    // title:+diary title:-girl
    let subqueries = vec![
        (Occur::Must, diary_term_query.box_clone()),
        (Occur::MustNot, girl_term_query.box_clone()),
    ];
    let diary_must_and_girl_must_not = BooleanQuery::new(subqueries);
    let count = searcher.search(&diary_must_and_girl_must_not, &Count)?;
    let doc_matches = searcher.search(&diary_must_and_girl_must_not, &DocSetCollector)?;

    info!("Matches count: {count}");

    for doc_address in doc_matches {
        let indexed_doc: TantivyDocument = searcher.doc(doc_address)?;

        if let Some(id) = indexed_doc.get_first(id_field).and_then(|v| v.as_u64()) {
            if let Some(original_doc) = original_docs.get(id as usize) {
                info!("Matched Original Doc [ID={id}]: {original_doc:?}");
            } else {
                // This is a safer way to handle a potential mismatch.
                log::warn!(
                    "Found ID {id} in index, but it was out of bounds for the original collection."
                );
            }
        }
    }

    Ok(())
}
