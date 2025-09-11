use once_cell::sync::Lazy;
use parquet_embed_tantivy::doc::Doc;

pub fn create_test_docs() -> Vec<Doc> {
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
}

pub static SOURCE_DATASET: Lazy<Vec<Doc>> = Lazy::new(create_test_docs);
