mod doc;
mod indexer;
mod query;
mod query_session;

use crate::doc::{DocMapper, DocSchema, examples};
use crate::indexer::IndexBuilder;
use crate::query_session::QuerySession;
use log::info;
use query::boolean_query;
use tantivy::collector::{Count, DocSetCollector};

fn main() -> tantivy::Result<()> {
    setup_logging();

    let original_docs = examples();

    let index = IndexBuilder::new(DocSchema::default().into_schema())
        .add_docs(original_docs)?
        .build();

    let query_session = QuerySession::new(&index)?;
    let doc_mapper = DocMapper::new(query_session.searcher(), original_docs);

    let query = boolean_query::title_contains_diary_and_not_girl(&query_session.schema());

    info!("Matches count: {}", query_session.search(&query, &Count)?);

    let results = query_session.search(&query, &DocSetCollector)?;
    for doc_address in results {
        let Ok(Some(doc_id)) = doc_mapper.get_doc_id(doc_address) else {
            info!("Failed to get doc id from doc address: {doc_address:?}");
            continue;
        };

        if let Some(doc) = doc_mapper.get_original_doc(doc_id) {
            info!("Matched Doc [ID={doc_id:?}]: {doc:?}")
        } else {
            info!("Failed to reverse map id: {doc_id:?} to a document")
        }
    }

    Ok(())
}

/// Initializes the logger.
fn setup_logging() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
}
