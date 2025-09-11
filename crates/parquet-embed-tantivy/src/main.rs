use datafusion::arrow::array::UInt64Array;
use datafusion::prelude::SessionContext;
use datafusion_common::arrow::array::{ArrayRef, StringArray};
use datafusion_common::arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::arrow::record_batch::RecordBatch;
use log::info;
use parquet_embed_tantivy::common::{Config, SchemaFields};
use parquet_embed_tantivy::custom_index::manifest::DraftManifest;
use parquet_embed_tantivy::doc::{examples, DocSchema};
use parquet_embed_tantivy::error::Result;
use parquet_embed_tantivy::index::{FullTextIndex, IndexBuilder};
use parquet_embed_tantivy::writer::ParquetWriter;
use std::path::PathBuf;
use std::sync::Arc;

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

    // These are the stages for preparing the index as a sequence of bytes.
    //      1. Draft the FileMetadata.
    //      2. Build the data block one file at a time.
    //          a. Back-fill pending fields in FileMetadata (offset, footer, crc)
    //      4. Build the Header.
    //
    // +--------------------+
    // | Draft FileMetadata |
    // +--------------------+
    //      - Path length
    //      - Path
    //      - Logical file size (footer is removed by Tantivy)
    //
    // The other fields are known only while building the data block. So it will be back-filled
    // when the data blocks are being written.
    //
    // +------------+
    // | Data Block |
    // +------------+
    // The Tantivy directory strips the footer and serves only the logical file contents. So when
    // assembling the data block implement a workaround - manually reconstruct and append the footer
    // for all index data binary files (except the meta.json).
    //
    // Now we can back-fill the `data_footer_len` field in `FileMetadata`. We can also now compute
    // and back-fill the `data_offset` of the next `FileMetadata` entry.
    //
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

    let mut writer = ParquetWriter::try_new(saved_path.clone(), arrow_schema_ref.clone(), None)?;
    writer.write_record_batch(&batch)?;
    writer.write_index_and_close(header, data_block)?;

    // +-------------------------------------+
    // | Read Offset from key_value_metadata |
    // +-------------------------------------+

    // let mut file = File::open(&saved_path)?;
    //
    // let reader = SerializedFileReader::new(file.try_clone()?)?;
    // let meta = reader.metadata().file_metadata();
    //
    // let kvs = meta.key_value_metadata().ok_or_else(|| {
    //     ParquetMetadata("Could not find key_value_metadata in FileMetadata".to_string())
    // })?;
    // let kv = kvs
    //     .iter()
    //     .find(|kv| kv.key == FULL_TEXT_INDEX_KEY)
    //     .ok_or(ParquetMetadata(format!(
    //         "Could not find key:{FULL_TEXT_INDEX_KEY} in key_value_metadata",
    //     )))?;
    // let full_text_index_offset = kv
    //     .value
    //     .as_deref()
    //     .ok_or_else(|| ParquetError::General("Missing index offset".into()))?
    //     .parse::<u64>()
    //     .map_err(|e| ParquetError::General(e.to_string()))?;
    // info!("Index {FULL_TEXT_INDEX_KEY} offset: {full_text_index_offset}");

    // +------------------------------------------+
    // | Read Full-Text Index Embedded In Parquet |
    // +------------------------------------------+

    // file.seek(SeekFrom::Start(full_text_index_offset))?;
    // let index_header = Header::from_reader(&file)?;
    //
    // let mut data_block_buffer = vec![0u8; index_header.total_data_block_size as usize];
    // file.read_exact(&mut data_block_buffer)?;
    // let index_data = DataBlock::new(data_block_buffer);

    // let index_dir = ReadOnlyArchiveDirectory::new(index_header, index_data);
    //
    // let read_only_index = Index::open_or_create(index_dir, schema.as_ref().clone())?;
    // let index_wrapper = ImmutableIndex::new(read_only_index);
    //
    // let query_session = QuerySession::new(&index_wrapper)?;
    // let doc_mapper = DocMapper::new(query_session.searcher(), &config, original_docs);
    //
    // info!(">>> Querying full-text index embedded within Parquet");
    // run_search_queries(&query_session, &doc_mapper)?;

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
