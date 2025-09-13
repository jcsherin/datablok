use once_cell::sync::Lazy;
use parquet_embed_tantivy::common::Config;
use parquet_embed_tantivy::doc::{Doc, DocIdMapper, DocMapper};
use parquet_embed_tantivy::error::Result;
use parquet_embed_tantivy::index::ImmutableIndex;
use parquet_embed_tantivy::query_session::QuerySession;
use std::collections::BinaryHeap;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::query::BooleanQuery;
use tantivy::schema::Schema;

pub static SOURCE_DATASET: Lazy<Vec<Doc>> = Lazy::new(|| {
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
        .collect()
});

pub fn create_test_docs() -> &'static [Doc] {
    &SOURCE_DATASET
}

pub fn assert_search_result_matches_source_data(
    index: &ImmutableIndex,
    config: &Config,
    expected: &[(u64, String, Option<String>)],
    query_builder: impl FnOnce(&Schema) -> Result<BooleanQuery>,
) {
    let query_session = QuerySession::new(&index).unwrap();
    let query = query_builder(&query_session.schema()).unwrap();

    let (doc_count, matching_docs) = query_session
        .search(&query, &(Count, DocSetCollector))
        .unwrap();

    let expected_doc_count = expected.len();
    assert_eq!(doc_count, expected_doc_count);

    let doc_mapper = DocMapper::new(query_session.searcher(), config, &SOURCE_DATASET);

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
    }
}
