use clap::Parser;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use log::{info, trace};
use parquet_embed_tantivy::doc::{ArrowDocSchema, DocTantivySchema};
use parquet_embed_tantivy::error::Result;
use parquet_embed_tantivy::index::FullTextIndex;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(name = "main")]
#[command(about = "Run SQL LIKE queries with a supporting full-text index")]
#[command(
    long_about = "Optimize LIKE queries with a full-text index embedded in the parquet file."
)]
#[command(version)]
struct Args {
    /// Input directory for generated parquet files
    #[arg(short, long)]
    parquet_with_index: PathBuf,
}

/// These are the stages for preparing the index as a sequence of bytes.
///      1. Draft the FileMetadata.
///      2. Build the data block one file at a time.
///          a. Back-fill pending fields in FileMetadata (offset, footer, crc)
///      4. Build the Header.
///
/// +--------------------+
/// | Draft FileMetadata |
/// +--------------------+
///      - Path length
///      - Path
///      - Logical file size (footer is removed by Tantivy)
///
/// The other fields are known only while building the data block. So it will be back-filled
/// when the data blocks are being written.
///
/// +------------+
/// | Data Block |
/// +------------+
/// The Tantivy directory strips the footer and serves only the logical file contents. So when
/// assembling the data block implement a workaround - manually reconstruct and append the footer
/// for all index data binary files (except the meta.json).
///
/// Now we can back-fill the `data_footer_len` field in `FileMetadata`. We can also now compute
/// and back-fill the `data_offset` of the next `FileMetadata` entry.
///
/// +---------------------------------------+
/// | Embed Full-Text Index in Parquet File |
/// +---------------------------------------+
/// The `RecordBatch`es are written first. This is followed by the byte serialized full-text
/// index. The offset of the index is added to `FileMetadata.key_value_metadata`. The Parquet
/// file size is now larger because of the embedded full text index. This is backwards compatible
/// with readers who will skip the index embedded within the file.
#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();

    let args = Args::parse();

    let tantivy_docs_schema = Arc::new(DocTantivySchema::new().into_schema());
    let arrow_docs_schema = ArrowDocSchema::default();

    let provider = Arc::new(FullTextIndex::try_open(
        &args.parquet_with_index,
        tantivy_docs_schema.clone(),
        arrow_docs_schema.clone(),
    )?);
    let ctx = SessionContext::new();
    ctx.register_table("t", provider)?;

    let sql = "SELECT * FROM t where title LIKE '%idempotency normalization%'";
    let explain_query = format!("EXPLAIN FORMAT TREE {sql}");
    let df = ctx.sql(&explain_query).await?;
    let result = df.to_string().await?;

    info!("\n{result}");

    let start = Instant::now();
    let _df = ctx.sql(sql).await?;
    let duration = start.elapsed();
    // let result = df.to_string().await?;
    // trace!("\n{result}");
    trace!("Executed in {duration:?}. Query with full-text index: {sql}");

    let sql = "SELECT * FROM t where title LIKE '%idempotency normalization%'";
    let ctx2 = SessionContext::new();
    ctx2.register_parquet(
        "t",
        args.parquet_with_index.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let start = Instant::now();
    let _df = ctx2.sql(sql).await?;
    let duration = start.elapsed();
    // let result = df.to_string().await?;
    // trace!("\n{result}");
    trace!("Executed in {duration:?}. Query: {sql}");

    Ok(())
}

/// Initializes the logger.
fn setup_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
}
