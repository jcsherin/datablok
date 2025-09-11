use parquet_embed_tantivy::common::{Config, SchemaFields};
use parquet_embed_tantivy::custom_index::header::Header;
use parquet_embed_tantivy::custom_index::manifest::DraftManifest;
use parquet_embed_tantivy::doc::DocSchema;
use parquet_embed_tantivy::index::IndexBuilder;
use std::sync::Arc;
mod common;

#[test]
fn roundtrip_custom_index_header() {
    let config = Config::default();
    let schema = Arc::new(DocSchema::new(&config).into_schema());

    let fields = SchemaFields::new(schema.clone(), &config).unwrap();

    let index = IndexBuilder::new(schema.clone())
        .index_and_commit(
            config.index_writer_memory_budget_in_bytes,
            &fields,
            &common::SOURCE_DATASET,
        )
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
