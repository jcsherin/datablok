//! A Tantivy full-text search query returns an internal doc address. The `DocMapper` translates
//! this internal doc address back to the corresponding row in the original data source.
//!
//! In the application, the search results from the full-text index are mapped to rows within a
//! Parquet file.

mod common;

use parquet_embed_tantivy::doc::{tiny_docs, DocTantivySchema};
use parquet_embed_tantivy::index::{TantivyDocIndex, TantivyDocIndexBuilder};
use parquet_embed_tantivy::query::boolean_query::{
    combine_term_and_phrase_query, title_contains_diary_and_not_girl, title_contains_diary_or_cow,
};
use std::sync::Arc;

fn setup_full_text_search_index() -> TantivyDocIndex {
    let schema = Arc::new(DocTantivySchema::new().into_schema());

    TantivyDocIndexBuilder::new(schema.clone())
        .write_docs(tiny_docs())
        .unwrap()
        .build()
}

#[test]
fn reverse_map_full_text_search_q1() {
    let index = setup_full_text_search_index();

    common::assert_search_result_matches_source_data(
        &index,
        &[(1, "The Diary of Muadib".to_string(), None)],
        |schema| title_contains_diary_and_not_girl(schema),
    );
}

#[test]
fn reverse_map_full_text_search_q2() {
    let index = setup_full_text_search_index();

    common::assert_search_result_matches_source_data(
        &index,
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
    let index = setup_full_text_search_index();

    common::assert_search_result_matches_source_data(
        &index,
        &[
            (1, "The Diary of Muadib".to_string(), None),
            (2, "A Dairy Cow".to_string(), Some("hidden".to_string())),
            (3, "A Dairy Cow".to_string(), Some("found".to_string())),
            (4, "The Diary of a Young Girl".to_string(), None),
        ],
        |schema| combine_term_and_phrase_query(schema),
    );
}
