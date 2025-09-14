use itertools::Itertools;
use parquet_embed_tantivy::doc::{tiny_docs, Doc};
use parquet_embed_tantivy::error::Result;
use parquet_embed_tantivy::index::TantivyDocIndex;
use parquet_embed_tantivy::query_session::QuerySession;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::query::BooleanQuery;
use tantivy::schema::{Schema, Value};
use tantivy::{DocAddress, Searcher, TantivyDocument};

trait TantivyDocAddressResolver<'a> {
    fn find_source_doc(&self, addr: DocAddress) -> Result<Option<&'a Doc>>;
}

struct DocAddressResolver<'a> {
    searcher: &'a Searcher,
    source_docs: &'a [Doc],
}

impl<'a> DocAddressResolver<'a> {
    fn new(searcher: &'a Searcher, source_docs: &'a [Doc]) -> Self {
        Self {
            searcher,
            source_docs,
        }
    }
}

impl<'a> TantivyDocAddressResolver<'a> for DocAddressResolver<'a> {
    fn find_source_doc(&self, addr: DocAddress) -> Result<Option<&'a Doc>> {
        let id_field = self.searcher.schema().get_field("id").unwrap();

        let found_doc = self.searcher.doc::<TantivyDocument>(addr)?;
        let id_value = found_doc.get_first(id_field).and_then(|v| v.as_u64());

        Ok(self
            .source_docs
            .iter()
            .find(|doc| Some(doc.id()) == id_value))
    }
}

#[allow(dead_code)]
pub fn assert_search_result_matches_source_data(
    index: &TantivyDocIndex,
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

    let data_source = tiny_docs().collect::<Vec<_>>();
    let doc_address_resolver = DocAddressResolver::new(query_session.searcher(), &data_source);

    let matching_docs = matching_docs
        .iter()
        .take(expected_doc_count)
        .map(|addr| {
            doc_address_resolver
                .find_source_doc(*addr)
                .unwrap()
                .unwrap()
        })
        .sorted_by_key(|doc| doc.id())
        .collect::<Vec<_>>();

    assert_eq!(matching_docs.len(), expected_doc_count);
    for (expected, matching_doc) in expected.iter().zip(matching_docs) {
        assert_eq!(matching_doc.id(), expected.0);
        assert_eq!(matching_doc.title(), expected.1);
    }
}
