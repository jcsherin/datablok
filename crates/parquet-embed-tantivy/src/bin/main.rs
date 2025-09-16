use clap::Parser;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_execution::config::SessionConfig;
use datafusion_expr::col;
use itertools::Itertools;
use parquet_embed_tantivy::data_generator::words::SELECTIVITY_PHRASES;
use parquet_embed_tantivy::doc::{ArrowDocSchema, DocTantivySchema};
use parquet_embed_tantivy::error::Result;
use parquet_embed_tantivy::index::FullTextIndex;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, instrument, trace, Instrument};
use tracing_subscriber::fmt::format::FmtSpan;

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
    input_dir: PathBuf,

    /// Optional list of query identifiers to execute (e.g. --queries 1,3,9)
    #[arg(short, long, value_delimiter = ',')]
    queries: Vec<u32>,
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
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .compact()
        .init();

    let args = Args::parse();

    let tantivy_docs_schema = Arc::new(DocTantivySchema::new().into_schema());
    let arrow_docs_schema = ArrowDocSchema::default();

    let full_text_index = Arc::new(FullTextIndex::try_open(
        &args.input_dir,
        tantivy_docs_schema.clone(),
        arrow_docs_schema.clone(),
    )?);

    let ctx_optimized = SessionContext::new();
    ctx_optimized.register_table("t", full_text_index)?;

    let session_config = SessionConfig::new()
        .with_parquet_pruning(true)
        .with_parquet_page_index_pruning(true);
    let ctx_baseline = SessionContext::new_with_config(session_config);
    ctx_baseline
        .register_parquet(
            "t",
            args.input_dir.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;

    let generate_sql = |(first, second): (&str, &str)| {
        format!("SELECT * FROM t WHERE title LIKE '%{first} {second}%'")
    };
    let control_words = SELECTIVITY_PHRASES.iter().map(|(fst, _)| *fst);
    let search_phrases = control_words
        .clone()
        .cartesian_product(control_words.clone())
        .map(generate_sql)
        .collect_vec();

    let filtered_queries = search_phrases
        .iter()
        .enumerate()
        .filter(|(id, _)| args.queries.is_empty() || args.queries.contains(&(*id as u32)))
        .collect_vec();

    // Warmup the OS page caches, and negate disk I/O latency
    info!("Warming up OS page cache...");
    for (_, sql) in filtered_queries.iter() {
        execute_sql(&ctx_baseline, sql)
            .instrument(tracing::info_span!("warmup_os_cache"))
            .await?;
    }

    for (id, sql) in filtered_queries.iter() {
        run_comparison(*id, sql, &ctx_baseline, &ctx_optimized).await?;
    }

    Ok(())
}

async fn execute_sql(ctx: &SessionContext, sql: &str) -> Result<Duration> {
    let df = ctx
        .sql(sql)
        .instrument(tracing::info_span!("create_dataframe_from_sql", sql=%sql))
        .await?;

    let start = Instant::now();
    df.collect()
        .instrument(tracing::info_span!("execute_sql"))
        .await?;
    let duration = start.elapsed();

    info!("Completed in {duration:?}");

    Ok(duration)
}

#[instrument(name = "query_comparison", skip_all, fields(query_id = %_query_id))]
async fn run_comparison(
    _query_id: usize,
    sql: &str,
    ctx_baseline: &SessionContext,
    ctx_optimized: &SessionContext,
) -> Result<()> {
    let baseline = execute_sql(ctx_baseline, sql)
        .instrument(tracing::info_span!("run", run_type = "baseline"))
        .await?;

    trace!("{sql}");
    let optimized = execute_sql(ctx_optimized, sql)
        .instrument(tracing::info_span!("run", run_type = "optimized"))
        .await?;

    let row_count = ctx_baseline.sql(sql).await?.count().await?;
    info!("Row count: {row_count}");

    if row_count > 0 {
        let df = ctx_optimized.sql(sql).await?;
        let df = df.sort(vec![col("id").sort(true, true)])?;
        let df = df.limit(0, Some(5))?;
        let output = df.to_string().await?;
        trace!("Showing first 5 rows:\n{output}\n");
    }

    let delta = optimized.abs_diff(baseline);
    let baseline_s = baseline.as_secs_f32();
    let optimized_s = optimized.as_secs_f32();

    if optimized < baseline {
        let speedup = baseline_s / optimized_s;

        info!("speedup: {speedup:.3}x");
        info!("-{delta:?} optimized: {optimized:?}, baseline: {baseline:?}");
    } else {
        let slowdown = optimized_s / baseline_s;

        info!("slowdown: {slowdown:.3}x");
        info!("+{delta:?} optimized: {optimized:?}, baseline: {baseline:?}");
    }

    Ok(())
}
