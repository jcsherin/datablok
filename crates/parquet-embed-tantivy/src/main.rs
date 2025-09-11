use datafusion::arrow::array::UInt64Array;
use datafusion::prelude::SessionContext;
use datafusion_common::arrow::array::{ArrayRef, StringArray};
use datafusion_common::arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::arrow::record_batch::RecordBatch;
use log::{info, trace};
use parquet::arrow::ArrowWriter;
use parquet::data_type::AsBytes;
use parquet::errors::ParquetError;
use parquet::file::metadata::KeyValue;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_embed_tantivy::common::{Config, SchemaFields};
use parquet_embed_tantivy::custom_index::data_block::DataBlock;
use parquet_embed_tantivy::custom_index::header::Header;
use parquet_embed_tantivy::custom_index::manifest::DraftManifest;
use parquet_embed_tantivy::directory::ReadOnlyArchiveDirectory;
use parquet_embed_tantivy::doc::{examples, DocIdMapper, DocMapper, DocSchema};
use parquet_embed_tantivy::error::Error::ParquetMetadata;
use parquet_embed_tantivy::error::Result;
use parquet_embed_tantivy::index::FULL_TEXT_INDEX_KEY;
use parquet_embed_tantivy::index::{FullTextIndex, ImmutableIndex, IndexBuilder};
use parquet_embed_tantivy::query::boolean_query;
use parquet_embed_tantivy::query_session::QuerySession;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::{HasLen, Index};

fn run_search_queries(query_session: &QuerySession, doc_mapper: &DocMapper) -> Result<()> {
    let print_search_results = |query| -> Result<()> {
        let results = query_session.search(query, &DocSetCollector)?;

        for doc_address in results {
            let Ok(Some(doc_id)) = doc_mapper.get_doc_id(doc_address) else {
                trace!("Failed to get doc id from doc address: {doc_address:?}");
                continue;
            };

            if let Some(doc) = doc_mapper.get_original_doc(doc_id) {
                trace!("Matched Doc [ID={doc_id:?}]: {doc:?}")
            } else {
                trace!("Failed to reverse map id: {doc_id:?} to a document")
            }
        }
        Ok(())
    };

    let q1 = boolean_query::title_contains_diary_and_not_girl(&query_session.schema())?;
    trace!("Q1: title:+diary AND title:-girl");
    let count = query_session.search(&q1, &Count)?;
    debug_assert_eq!(count, 1);
    trace!("Matches count: {count}");
    print_search_results(&q1)?;
    trace!("***");

    let q2 = boolean_query::title_contains_diary_or_cow(&query_session.schema())?;
    trace!("Q2: title:diary OR title:cow");
    let count = query_session.search(&q2, &Count)?;
    debug_assert_eq!(count, 4);
    trace!("Matches count: {}", query_session.search(&q2, &Count)?);
    print_search_results(&q2)?;
    trace!("***");

    let q3 = boolean_query::combine_term_and_phrase_query(&query_session.schema())?;
    trace!("Q3: title:diary OR title:\"dairy cow\"");
    let count = query_session.search(&q3, &Count)?;
    debug_assert_eq!(count, 4);
    trace!("Matches count: {}", query_session.search(&q3, &Count)?);
    print_search_results(&q3)?;
    trace!("***");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();

    let config = Config::default();
    let schema = Arc::new(DocSchema::new(&config).into_schema());
    let original_docs = examples();

    let fields = SchemaFields::new(schema.clone(), &config)?;

    let index = IndexBuilder::new(schema.clone())
        .index_and_commit(
            config.index_writer_memory_budget_in_bytes,
            &fields,
            original_docs,
        )?
        .build();

    let (header, data_block) = DraftManifest::try_new(&index)?.try_into(&index)?;

    // +----------------+
    // | Query Baseline |
    // +----------------+
    // These identical queries should work on our read-only archive directory implementation.

    // let query_session = QuerySession::new(&index)?;
    // let doc_mapper = DocMapper::new(query_session.searcher(), &config, original_docs);
    //
    // trace!(">>> Querying RamDirectory");
    // run_search_queries(&query_session, &doc_mapper)?;
    // trace!("---");

    // These are the stages for preparing the index as a sequence of bytes.
    //      1. Draft the FileMetadata.
    //      2. Build the data block one file at a time.
    //          a. Back-fill pending fields in FileMetadata (offset, footer, crc)
    //      4. Build the Header.

    // +--------------------+
    // | Draft FileMetadata |
    // +--------------------+
    //      - Path length
    //      - Path
    //      - Logical file size (footer is removed by Tantivy)
    //
    // The other fields are known only while building the data block. So it will be back-filled
    // when the data blocks are being written.

    // +------------+
    // | Data Block |
    // +------------+
    // The Tantivy directory strips the footer and serves only the logical file contents. So when
    // assembling the data block implement a workaround - manually reconstruct and append the footer
    // for all index data binary files (except the meta.json).
    //
    // Now we can back-fill the `data_footer_len` field in `FileMetadata`. We can also now compute
    // and back-fill the `data_offset` of the next `FileMetadata` entry.

    // +--------------+
    // | Header Block |
    // +--------------+

    // let (header, data_block) = DraftManifest::try_new(&index)?.try_into(&index)?;
    //
    // let header_bytes: Vec<u8> = header.clone().into();
    // let roundtrip_header = Header::try_from(header_bytes.clone())?;
    // trace!(
    //     "[Header] data block size:{}, file metadata block size: {} file metadata block crc32:{}",
    //     roundtrip_header.total_data_block_size,
    //     roundtrip_header.file_metadata_size,
    //     roundtrip_header.file_metadata_crc32
    // );
    // trace!("[Header] Round trip header:{roundtrip_header:#?}");

    // +----------------------+
    // | Blob Index Directory |
    // +----------------------+
    // let archive_dir = ReadOnlyArchiveDirectory::new(roundtrip_header, data_block.clone());
    // trace!("Read-only Archive Directory:{archive_dir:?}");

    // let read_only_index = Index::open_or_create(archive_dir, schema.as_ref().clone())?;
    // let index_wrapper = ImmutableIndex::new(read_only_index);

    // +---------------------------+
    // | Query Baseline Comparison |
    // +---------------------------+
    // The read-only directory of this index was constructed from an index. We verified earlier that
    // querying it works without problems. The same queries should return identical results when
    // applied against the read-only archive directory implementation.

    // let query_session = QuerySession::new(&index_wrapper)?;
    // let doc_mapper = DocMapper::new(query_session.searcher(), &config, original_docs);
    //
    // trace!(">>> Querying ReadOnlyArchiveDirectory");
    // run_search_queries(&query_session, &doc_mapper)?;

    // +---------------------------------------+
    // | Embed Full-Text Index in Parquet File |
    // +---------------------------------------+
    // The `RecordBatch`es are written first. This is followed by the byte serialized full-text
    // index. The offset of the index is added to `FileMetadata.key_value_metadata`. The Parquet
    // file size is now larger because of the embedded full text index. This is backwards compatible
    // with readers who will skip the index embedded within the file.

    let id_field = Field::new("id", DataType::UInt64, false);
    let title_field = Field::new("title", DataType::Utf8, true);
    let body_field = Field::new("body", DataType::Utf8, true);

    let arrow_schema_ref = Arc::new(Schema::new(vec![id_field, title_field, body_field]));

    let id_array: ArrayRef = Arc::new(
        examples()
            .iter()
            .map(|doc| doc.id())
            .collect::<UInt64Array>(),
    );
    let title_array: ArrayRef = Arc::new(
        examples()
            .iter()
            .map(|doc| Some(doc.title()))
            .collect::<StringArray>(),
    );
    let body_array: ArrayRef = Arc::new(
        examples()
            .iter()
            .map(|doc| doc.body())
            .collect::<StringArray>(),
    );
    let batch = RecordBatch::try_new(
        arrow_schema_ref.clone(),
        vec![id_array, title_array, body_array],
    )?;

    let saved_path = PathBuf::from("fat.parquet");
    let file = File::create(&saved_path)?;

    let mut writer = ArrowWriter::try_new(file, arrow_schema_ref.clone(), None)?;

    writer.write(&batch)?;
    writer.flush()?;

    let offset = writer.bytes_written();

    let header_bytes: Vec<u8> = header.clone().into();
    writer.write_all(header_bytes.as_bytes())?;
    writer.write_all(data_block.deref())?;

    info!("Index will be written to offset: {offset}");
    info!(
        "Index size: {} bytes",
        header_bytes.len() + data_block.len()
    );

    // Store the full-text index offset in `FileMetadata.key_value_metadata` for reading it back
    // later.
    writer.append_key_value_metadata(KeyValue::new(
        FULL_TEXT_INDEX_KEY.to_string(),
        offset.to_string(),
    ));

    writer.close()?;
    info!("Wrote Parquet file to: {}", saved_path.display());

    // +-------------------------------------+
    // | Read Offset from key_value_metadata |
    // +-------------------------------------+

    let mut file = File::open(&saved_path)?;

    let reader = SerializedFileReader::new(file.try_clone()?)?;
    let meta = reader.metadata().file_metadata();

    let kvs = meta.key_value_metadata().ok_or_else(|| {
        ParquetMetadata("Could not find key_value_metadata in FileMetadata".to_string())
    })?;
    let kv = kvs
        .iter()
        .find(|kv| kv.key == FULL_TEXT_INDEX_KEY)
        .ok_or(ParquetMetadata(format!(
            "Could not find key:{FULL_TEXT_INDEX_KEY} in key_value_metadata",
        )))?;
    let full_text_index_offset = kv
        .value
        .as_deref()
        .ok_or_else(|| ParquetError::General("Missing index offset".into()))?
        .parse::<u64>()
        .map_err(|e| ParquetError::General(e.to_string()))?;
    info!("Index {FULL_TEXT_INDEX_KEY} offset: {full_text_index_offset}");

    // +------------------------------------------+
    // | Read Full-Text Index Embedded In Parquet |
    // +------------------------------------------+

    file.seek(SeekFrom::Start(full_text_index_offset))?;
    let index_header = Header::from_reader(&file)?;

    let mut data_block_buffer = vec![0u8; index_header.total_data_block_size as usize];
    file.read_exact(&mut data_block_buffer)?;
    let index_data = DataBlock::new(data_block_buffer);

    let index_dir = ReadOnlyArchiveDirectory::new(index_header, index_data);

    let read_only_index = Index::open_or_create(index_dir, schema.as_ref().clone())?;
    let index_wrapper = ImmutableIndex::new(read_only_index);

    let query_session = QuerySession::new(&index_wrapper)?;
    let doc_mapper = DocMapper::new(query_session.searcher(), &config, original_docs);

    info!(">>> Querying full-text index embedded within Parquet");
    run_search_queries(&query_session, &doc_mapper)?;

    let provider = Arc::new(FullTextIndex::try_open(
        &saved_path,
        schema.clone(),
        arrow_schema_ref.clone(),
    )?);
    let ctx = SessionContext::new();
    ctx.register_table("t", provider)?;

    let sql = "SELECT * FROM t where title LIKE '%dairy cow%'";
    info!("Executing Query: {sql}");
    let df = ctx.sql(sql).await?;
    let result = df.to_string().await?;

    info!("\n{result}");

    let explain_query = format!("EXPLAIN FORMAT TREE {sql}");
    let df = ctx.sql(&explain_query).await?;
    let result = df.to_string().await?;

    info!("\n{result}");

    Ok(())
}

/// Initializes the logger.
fn setup_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
}
