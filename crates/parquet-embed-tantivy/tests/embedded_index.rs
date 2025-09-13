use crate::common::{assert_search_result_matches_source_data, create_test_docs};
use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_embed_tantivy::common::{Config, SchemaFields};
use parquet_embed_tantivy::custom_index::data_block::DataBlock;
use parquet_embed_tantivy::custom_index::header::Header;
use parquet_embed_tantivy::custom_index::manifest::DraftManifest;
use parquet_embed_tantivy::directory::ReadOnlyArchiveDirectory;
use parquet_embed_tantivy::doc::{
    generate_record_batch_for_docs, ArrowDocSchema, DocTantivySchema,
};
use parquet_embed_tantivy::error::Error::ParquetMetadata;
use parquet_embed_tantivy::index::{ImmutableIndex, IndexBuilder, FULL_TEXT_INDEX_KEY};
use parquet_embed_tantivy::query::boolean_query::{
    combine_term_and_phrase_query, title_contains_diary_and_not_girl, title_contains_diary_or_cow,
};
use parquet_embed_tantivy::writer::ParquetWriter;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use std::sync::Arc;
use tantivy::Index;
use tempfile::Builder;

mod common;
#[test]
fn test_embedded_full_text_index() {
    let config = Config::default();
    let schema = Arc::new(DocTantivySchema::new(&config).into_schema());
    let fields = SchemaFields::new(schema.clone(), &config).unwrap();

    let source_dataset = create_test_docs();

    let index = IndexBuilder::new(schema.clone())
        .index_and_commit(
            config.index_writer_memory_budget_in_bytes,
            &fields,
            source_dataset,
        )
        .unwrap()
        .build();

    let (header, data_block) = DraftManifest::try_new(&index)
        .unwrap()
        .try_into(&index)
        .unwrap();

    let arrow_docs_schema = ArrowDocSchema::default();
    let batch = generate_record_batch_for_docs(arrow_docs_schema.clone(), source_dataset).unwrap();

    let temp_dir = Builder::new()
        .prefix("test_embedded_full_text_index")
        .tempdir()
        .unwrap();
    let saved_path = temp_dir.path().join("test_docs_with_index.parquet");

    let mut writer =
        ParquetWriter::try_new(saved_path.clone(), arrow_docs_schema.clone(), None).unwrap();
    writer.write_record_batch(&batch).unwrap();
    writer.write_index_and_close(header, data_block).unwrap();

    let mut file = File::open(&saved_path).unwrap();

    // +-------------------------------------+
    // | Read Offset from key_value_metadata |
    // +-------------------------------------+

    let reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
    let meta = reader.metadata().file_metadata();

    let kvs = meta
        .key_value_metadata()
        .ok_or_else(|| {
            ParquetMetadata("Could not find key_value_metadata in FileMetadata".to_string())
        })
        .unwrap();
    let kv = kvs
        .iter()
        .find(|kv| kv.key == FULL_TEXT_INDEX_KEY)
        .ok_or(ParquetMetadata(format!(
            "Could not find key:{FULL_TEXT_INDEX_KEY} in key_value_metadata",
        )))
        .unwrap();
    let full_text_index_offset = kv
        .value
        .as_deref()
        .ok_or_else(|| ParquetError::General("Missing index offset".into()))
        .unwrap()
        .parse::<u64>()
        .map_err(|e| ParquetError::General(e.to_string()))
        .unwrap();

    // +------------------------------------------+
    // | Read Full-Text Index Embedded In Parquet |
    // +------------------------------------------+

    file.seek(SeekFrom::Start(full_text_index_offset)).unwrap();
    let index_header = Header::from_reader(&file).unwrap();

    let mut data_block_buffer = vec![0u8; index_header.total_data_block_size as usize];
    file.read_exact(&mut data_block_buffer).unwrap();
    let index_data = DataBlock::new(data_block_buffer);

    let index_dir = ReadOnlyArchiveDirectory::new(index_header, index_data);

    let read_only_index = Index::open_or_create(index_dir, schema.as_ref().clone()).unwrap();
    let index_wrapper = ImmutableIndex::new(read_only_index);

    assert_search_result_matches_source_data(
        &index_wrapper,
        &config,
        &[(1, "The Diary of Muadib".to_string(), None)],
        |schema| title_contains_diary_and_not_girl(schema),
    );

    assert_search_result_matches_source_data(
        &index,
        &config,
        &[
            (1, "The Diary of Muadib".to_string(), None),
            (2, "A Dairy Cow".to_string(), Some("hidden".to_string())),
            (3, "A Dairy Cow".to_string(), Some("found".to_string())),
            (4, "The Diary of a Young Girl".to_string(), None),
        ],
        |schema| title_contains_diary_or_cow(schema),
    );

    assert_search_result_matches_source_data(
        &index,
        &config,
        &[
            (1, "The Diary of Muadib".to_string(), None),
            (2, "A Dairy Cow".to_string(), Some("hidden".to_string())),
            (3, "A Dairy Cow".to_string(), Some("found".to_string())),
            (4, "The Diary of a Young Girl".to_string(), None),
        ],
        |schema| combine_term_and_phrase_query(schema),
    );
}
