use datafusion::prelude::SessionContext;
use log::{info, trace};
use parquet_embed_tantivy::custom_index::manifest::DraftManifest;
use parquet_embed_tantivy::data_generator::title::TitleGenerator;
use parquet_embed_tantivy::doc::{
    generate_record_batch_for_docs, tiny_docs, ArrowDocSchema, DocTantivySchema,
};
use parquet_embed_tantivy::error::Result;
use parquet_embed_tantivy::index::{FullTextIndex, TantivyDocIndexBuilder};
use parquet_embed_tantivy::writer::ParquetWriter;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();

    let schema = Arc::new(DocTantivySchema::new().into_schema());

    let index = TantivyDocIndexBuilder::new(schema.clone())
        .write_docs(tiny_docs())?
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
    let arrow_docs_schema = ArrowDocSchema::default();
    let data_source = tiny_docs().collect::<Vec<_>>();
    let batch = generate_record_batch_for_docs(arrow_docs_schema.clone(), &data_source)?;

    let parquet_file_path = PathBuf::from("doc_with_full_text_index.parquet");

    let mut writer =
        ParquetWriter::try_new(parquet_file_path.clone(), arrow_docs_schema.clone(), None)?;
    writer.write_record_batch(&batch)?;
    writer.write_index_and_close(header, data_block)?;
    trace!(
        "Wrote file: {} with embedded full text index.",
        parquet_file_path.display()
    );

    let provider = Arc::new(FullTextIndex::try_open(
        &parquet_file_path,
        schema.clone(),
        arrow_docs_schema.clone(),
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

    trace!("Deleting file: {}", parquet_file_path.display());
    std::fs::remove_file(&parquet_file_path)?;

    const RNG_SEED: u64 = 123;
    for (x, y) in TitleGenerator::new(RNG_SEED)
        .take(10)
        .zip(TitleGenerator::new(RNG_SEED).take(10))
    {
        trace!("{x}, {y}")
    }

    Ok(())
}

/// Initializes the logger.
fn setup_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
}
