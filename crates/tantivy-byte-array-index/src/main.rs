mod common;
mod doc;
mod error;
mod index;
mod query;
mod query_session;

use crate::common::{Config, SchemaFields};
use crate::doc::{DocIdMapper, DocMapper, DocSchema, examples};
use crate::error::Result;
use crate::index::IndexBuilder;
use crate::query_session::QuerySession;
use log::info;
use query::boolean_query;
use tantivy::collector::{Count, DocSetCollector};

fn main() -> Result<()> {
    setup_logging();

    let config = Config::default();
    let schema = DocSchema::new(&config).into_schema();
    let original_docs = examples();

    let fields = SchemaFields::new(&schema, &config)?;

    let index = IndexBuilder::new(schema)
        .index_and_commit(
            config.index_writer_memory_budget_in_bytes,
            &fields,
            original_docs,
        )?
        .build();

    let query_session = QuerySession::new(&index)?;
    let doc_mapper = DocMapper::new(query_session.searcher(), &config, original_docs);

    let query = boolean_query::title_contains_diary_and_not_girl(&query_session.schema())?;

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
