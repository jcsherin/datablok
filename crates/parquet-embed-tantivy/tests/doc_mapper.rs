//! A Tantivy full-text search query returns an internal doc address. The `DocMapper` translates
//! this internal doc address back to the corresponding row in the original data source.
//!
//! In the application, the search results from the full-text index are mapped to rows within a
//! Parquet file.

use once_cell::sync::Lazy;
use parquet_embed_tantivy::common::{Config, SchemaFields};
use parquet_embed_tantivy::doc::{Doc, DocIdMapper, DocMapper, DocSchema};
use parquet_embed_tantivy::error::Result;
use parquet_embed_tantivy::index::IndexBuilder;
use parquet_embed_tantivy::query::boolean_query::{
    combine_term_and_phrase_query, title_contains_diary_and_not_girl, title_contains_diary_or_cow,
    title_contains_phrase_diary_cow,
};
use parquet_embed_tantivy::query_session::QuerySession;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::query::BooleanQuery;
use tantivy::schema::Schema;

static SOURCE_DATASET: Lazy<Vec<Doc>> = Lazy::new(|| {
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

fn assert_search_result_matches_source_data(
    expected: &[(u64, String, Option<String>)],
    query_builder: impl FnOnce(&Schema) -> Result<BooleanQuery>,
) {
    let config = Config::default();
    let schema = Arc::new(DocSchema::new(&config).into_schema());
    let fields = SchemaFields::new(schema.clone(), &config).unwrap();

    let index = IndexBuilder::new(schema.clone())
        .index_and_commit(
            config.index_writer_memory_budget_in_bytes,
            &fields,
            &SOURCE_DATASET,
        )
        .unwrap()
        .build();

    let query_session = QuerySession::new(&index).unwrap();
    let query = query_builder(&query_session.schema()).unwrap();

    let (doc_count, matching_docs) = query_session
        .search(&query, &(Count, DocSetCollector))
        .unwrap();

    let expected_doc_count = expected.len();
    assert_eq!(doc_count, expected_doc_count);

    let doc_mapper = DocMapper::new(query_session.searcher(), &config, &SOURCE_DATASET);

    let matching_doc_ids = matching_docs
        .iter()
        .take(expected_doc_count)
        .map(|doc_addr| doc_mapper.get_doc_id(*doc_addr).unwrap().unwrap())
        .collect::<BinaryHeap<_>>()
        .into_sorted_vec();

    let matching_docs = matching_doc_ids
        .iter()
        .map(|doc_id| doc_mapper.get_original_doc(*doc_id).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(matching_docs.len(), expected_doc_count);
    for (expected, matching_doc) in expected.iter().zip(matching_docs) {
        assert_eq!(matching_doc.id(), expected.0);
        assert_eq!(matching_doc.title(), expected.1);
        assert_eq!(matching_doc.body(), expected.2.as_deref());
    }
}

#[test]
fn reverse_map_full_text_search_q1() {
    assert_search_result_matches_source_data(&[(1, "The Diary of Muadib".to_string(), None)], {
        |schema| title_contains_diary_and_not_girl(schema)
    });
}

#[test]
fn reverse_map_full_text_search_q2() {
    assert_search_result_matches_source_data(
        &[
            (1, "The Diary of Muadib".to_string(), None),
            (2, "A Dairy Cow".to_string(), Some("hidden".to_string())),
            (3, "A Dairy Cow".to_string(), Some("found".to_string())),
            (4, "The Diary of a Young Girl".to_string(), None),
        ],
        |schema| title_contains_diary_or_cow(schema),
    );
}

#[test]
fn reverse_map_full_text_search_q3() {
    assert_search_result_matches_source_data(
        &[
            (1, "The Diary of Muadib".to_string(), None),
            (2, "A Dairy Cow".to_string(), Some("hidden".to_string())),
            (3, "A Dairy Cow".to_string(), Some("found".to_string())),
            (4, "The Diary of a Young Girl".to_string(), None),
        ],
        |schema| combine_term_and_phrase_query(schema),
    );
}
