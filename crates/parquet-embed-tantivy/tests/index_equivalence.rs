use crate::common::SOURCE_DATASET;
use parquet_embed_tantivy::common::{Config, SchemaFields};
use parquet_embed_tantivy::custom_index::manifest::DraftManifest;
use parquet_embed_tantivy::directory::ReadOnlyArchiveDirectory;
use parquet_embed_tantivy::doc::DocTantivySchema;
use parquet_embed_tantivy::index::{ImmutableIndex, IndexBuilder};
use parquet_embed_tantivy::query::boolean_query::{
    combine_term_and_phrase_query, title_contains_diary_and_not_girl, title_contains_diary_or_cow,
};
use parquet_embed_tantivy::query_session::QuerySession;
use std::sync::Arc;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::Index;

mod common;

#[test]
fn test_search_results_equivalence() {
    let config = Config::default();
    let schema = Arc::new(DocTantivySchema::new(&config).into_schema());
    let fields = SchemaFields::new(schema.clone(), &config).unwrap();

    let tantivy_in_memory_index = IndexBuilder::new(schema.clone())
        .index_and_commit(
            config.index_writer_memory_budget_in_bytes,
            &fields,
            &SOURCE_DATASET,
        )
        .unwrap()
        .build();

    let (header, data_block) = DraftManifest::try_new(&tantivy_in_memory_index)
        .unwrap()
        .try_into(&tantivy_in_memory_index)
        .unwrap();
    let byte_array_directory = ReadOnlyArchiveDirectory::new(header, data_block);
    let custom_index = ImmutableIndex::new(
        Index::open_or_create(byte_array_directory, schema.as_ref().clone()).unwrap(),
    );

    let tantivy_index_session = QuerySession::new(&tantivy_in_memory_index).unwrap();
    let custom_index_session = QuerySession::new(&custom_index).unwrap();

    let test_queries = [
        title_contains_diary_and_not_girl,
        title_contains_diary_or_cow,
        combine_term_and_phrase_query,
    ];

    for query_builder in test_queries {
        let query = query_builder(&schema).unwrap();

        let (lhs_doc_count, lhs_matching_set) = tantivy_index_session
            .search(&query, &(Count, DocSetCollector))
            .unwrap();

        let (rhs_doc_count, rhs_matching_set) = custom_index_session
            .search(&query, &(Count, DocSetCollector))
            .unwrap();

        assert_eq!(
            lhs_doc_count, rhs_doc_count,
            "Expected query to return to same doc count in both indexes"
        );
        assert_eq!(lhs_matching_set, rhs_matching_set);
    }
}
