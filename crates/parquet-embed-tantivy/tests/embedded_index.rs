use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_embed_tantivy::custom_index::data_block::DataBlock;
use parquet_embed_tantivy::custom_index::header::Header;
use parquet_embed_tantivy::custom_index::manifest::DraftManifest;
use parquet_embed_tantivy::directory::ReadOnlyArchiveDirectory;
use parquet_embed_tantivy::doc::{
    generate_record_batch_for_docs, tiny_docs, ArrowDocSchema, DocTantivySchema,
};
use parquet_embed_tantivy::error::Error::ParquetMetadata;
use parquet_embed_tantivy::index::{TantivyDocIndex, TantivyDocIndexBuilder, FULL_TEXT_INDEX_KEY};
use parquet_embed_tantivy::query::boolean_query::{
    combine_term_and_phrase_query, title_contains_diary_and_not_girl, title_contains_diary_or_cow,
};
use parquet_embed_tantivy::writer::ParquetWriter;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::schema::Schema;
use tantivy::Index;
use tempfile::{Builder, TempDir};

mod common;

#[test]
fn test_embedded_full_text_index() {
    let schema = Arc::new(DocTantivySchema::new().into_schema());
    let in_memory_index = create_in_memory_index(schema.clone());
    let (_temp_dir, temp_file) = prepare_temp_file("test_embedded_full_text_index");

    write_parquet_with_index(&temp_file, &in_memory_index);

    let offset = read_index_offset_from_metadata(&temp_file);
    let embedded_index = read_index_from_parquet(&temp_file, offset, schema.clone());

    common::assert_search_result_matches_source_data(
        &embedded_index,
        &[(1, "The Diary of Muadib".to_string(), None)],
        |schema| title_contains_diary_and_not_girl(schema),
    );

    common::assert_search_result_matches_source_data(
        &embedded_index,
        &[
            (1, "The Diary of Muadib".to_string(), None),
            (2, "A Dairy Cow".to_string(), Some("hidden".to_string())),
            (3, "A Dairy Cow".to_string(), Some("found".to_string())),
            (4, "The Diary of a Young Girl".to_string(), None),
        ],
        |schema| title_contains_diary_or_cow(schema),
    );

    common::assert_search_result_matches_source_data(
        &embedded_index,
        &[
            (1, "The Diary of Muadib".to_string(), None),
            (2, "A Dairy Cow".to_string(), Some("hidden".to_string())),
            (3, "A Dairy Cow".to_string(), Some("found".to_string())),
            (4, "The Diary of a Young Girl".to_string(), None),
        ],
        |schema| combine_term_and_phrase_query(schema),
    );
}

fn create_in_memory_index(schema: Arc<Schema>) -> TantivyDocIndex {
    TantivyDocIndexBuilder::new(schema.clone())
        .write_docs(tiny_docs())
        .unwrap()
        .build()
}

fn prepare_temp_file(prefix: &str) -> (TempDir, PathBuf) {
    let temp_dir = Builder::new().prefix(prefix).tempdir().unwrap();
    let temp_file = temp_dir.path().join("test_docs_with_index.parquet");
    (temp_dir, temp_file)
}

fn write_parquet_with_index(path: &Path, index: &TantivyDocIndex) {
    let (header, data_block) = DraftManifest::try_new(index)
        .unwrap()
        .try_into(index)
        .unwrap();

    let arrow_docs_schema = ArrowDocSchema::default();
    let data_source = tiny_docs().collect::<Vec<_>>();
    let batch = generate_record_batch_for_docs(arrow_docs_schema.clone(), &data_source).unwrap();

    let mut writer =
        ParquetWriter::try_new(path.to_path_buf(), arrow_docs_schema.clone(), None).unwrap();
    writer.write_record_batch(&batch).unwrap();
    writer.write_index_and_close(header, data_block).unwrap();
}

fn read_index_offset_from_metadata(path: &Path) -> u64 {
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
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
    kv.value
        .as_deref()
        .ok_or_else(|| ParquetError::General("Missing index offset".into()))
        .unwrap()
        .parse::<u64>()
        .map_err(|e| ParquetError::General(e.to_string()))
        .unwrap()
}

fn read_index_from_parquet(path: &Path, offset: u64, schema: Arc<Schema>) -> TantivyDocIndex {
    let mut file = File::open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let index_header = Header::from_reader(&file).unwrap();

    let mut data_block_buffer = vec![0u8; index_header.total_data_block_size as usize];
    file.read_exact(&mut data_block_buffer).unwrap();
    let index_data = DataBlock::new(data_block_buffer);

    let index_dir = ReadOnlyArchiveDirectory::new(index_header, index_data);

    let read_only_index = Index::open_or_create(index_dir, schema.as_ref().clone()).unwrap();
    TantivyDocIndex::new(read_only_index)
}
