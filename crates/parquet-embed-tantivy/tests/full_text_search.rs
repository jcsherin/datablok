use once_cell::sync::Lazy;
use parquet_embed_tantivy::common::{Config, SchemaFields};
use parquet_embed_tantivy::doc::{Doc, DocIdMapper, DocMapper, DocSchema};
use parquet_embed_tantivy::index::{ImmutableIndex, IndexBuilder};
use parquet_embed_tantivy::query::boolean_query::title_contains_diary_and_not_girl;
use parquet_embed_tantivy::query_session::QuerySession;
use std::sync::Arc;
use tantivy::collector::{Count, DocSetCollector};

fn setup_full_text_search_index(config: &Config, docs: &[Doc]) -> ImmutableIndex {
    let schema = Arc::new(DocSchema::new(&config).into_schema());
    let fields = SchemaFields::new(schema.clone(), &config).unwrap();

    IndexBuilder::new(schema.clone())
        .index_and_commit(config.index_writer_memory_budget_in_bytes, &fields, docs)
        .unwrap()
        .build()
}

static DOCS: Lazy<Vec<Doc>> = Lazy::new(|| {
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

#[test]
fn baseline_q1() {
    let config = Config::default();
    let index = setup_full_text_search_index(&config, &DOCS);

    let query_session = QuerySession::new(&index).unwrap();
    let doc_mapper = DocMapper::new(query_session.searcher(), &config, &DOCS);
    let query = title_contains_diary_and_not_girl(&query_session.schema()).unwrap();

    let count = query_session.search(&query, &Count).unwrap();
    assert_eq!(count, 1);

    let result = query_session
        .search(&query, &DocSetCollector)
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(result.len(), 1);

    let doc_address = result[0];
    let doc_id = doc_mapper.get_doc_id(doc_address).unwrap();
    assert_eq!(doc_id, Some(1));

    let found_doc = doc_mapper.get_original_doc(doc_id.unwrap()).unwrap();
    assert_eq!(found_doc.id(), 1);
    assert_eq!(found_doc.title(), "The Diary of Muadib");
    assert_eq!(found_doc.body(), None);
}
