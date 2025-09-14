use parquet_embed_tantivy::custom_index::header::Header;
use parquet_embed_tantivy::custom_index::manifest::DraftManifest;
use parquet_embed_tantivy::doc::{tiny_docs, DocTantivySchema};
use parquet_embed_tantivy::index::TantivyDocIndexBuilder;
use std::sync::Arc;

#[test]
fn roundtrip_custom_index_header() {
    let schema = Arc::new(DocTantivySchema::new().into_schema());

    let index = TantivyDocIndexBuilder::new(schema.clone())
        .write_docs(tiny_docs())
        .unwrap()
        .build();

    let (header, _) = DraftManifest::try_new(&index)
        .unwrap()
        .try_into(&index)
        .unwrap();

    let header_bytes: Vec<u8> = header.clone().into();
    let round_tripped = Header::try_from(header_bytes.clone()).unwrap();

    assert_eq!(header, round_tripped);
}
