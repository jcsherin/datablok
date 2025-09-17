use clap::Parser;
use comfy_table::{presets, CellAlignment, Table, TableComponent};
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

    let mut results: Vec<(QueryComparisonMetrics, &str)> =
        Vec::with_capacity(filtered_queries.len());
    for (id, sql) in filtered_queries.iter() {
        let metrics = run_comparison(*id, sql, &ctx_baseline, &ctx_optimized).await?;
        trace!("{metrics:?}");

        results.push((metrics, sql));
    }

    let total_row_count =
        count_parquet_rows(&ctx_optimized, args.input_dir.to_str().unwrap()).await?;

    print_summary_table(results, total_row_count);
    println!(
        "Parquet: {} row count: {total_row_count}",
        args.input_dir.to_str().unwrap()
    );

    Ok(())
}

fn print_summary_table(results: Vec<(QueryComparisonMetrics, &str)>, parquet_row_count: usize) {
    let mut table = Table::new();
    table.load_preset(presets::NOTHING);
    table.set_header(vec![
        "Query ID",
        "Rows",
        "Selectivity",
        "Baseline",
        "With FTS",
        "Diff",
        "Perf Change",
        // "SQL",
    ]);
    table.set_style(TableComponent::VerticalLines, '|');
    table.set_style(TableComponent::LeftBorder, '|');
    table.set_style(TableComponent::RightBorder, '|');

    // Add header separator
    table.set_style(TableComponent::HeaderLines, '-');
    table.set_style(TableComponent::LeftHeaderIntersection, '+');
    table.set_style(TableComponent::MiddleHeaderIntersections, '+');
    table.set_style(TableComponent::RightHeaderIntersection, '+');

    // Add top and bottom borders
    table.set_style(TableComponent::TopBorder, '-');
    table.set_style(TableComponent::BottomBorder, '-');
    table.set_style(TableComponent::TopLeftCorner, '+');
    table.set_style(TableComponent::TopRightCorner, '+');
    table.set_style(TableComponent::BottomLeftCorner, '+');
    table.set_style(TableComponent::BottomRightCorner, '+');
    table.set_style(TableComponent::TopBorderIntersections, '+');
    table.set_style(TableComponent::BottomBorderIntersections, '+');

    for i in 0..=5 {
        if let Some(column) = table.column_mut(i) {
            column.set_cell_alignment(CellAlignment::Right);
        }
    }

    let query_count = results.len();
    let mut slow_query_count = 0;
    for (m, _sql) in results {
        let delta = m.abs_diff();
        let formatted_delta = if m.is_speedup() {
            format!("-{delta:.2?}")
        } else {
            format!("+{delta:.2?}")
        };

        let formatted_perf_change = match m.get_change_in_performance() {
            PerfChange::Speedup(v) => {
                format!("{v:.2}X")
            }
            PerfChange::Slowdown(v) => {
                slow_query_count += 1;
                format!("{v:.2}X (slowdown)")
            }
        };

        let selectivity_percentage =
            (m.total_row_count() as f32 / parquet_row_count as f32) * 100.0;

        table.add_row(vec![
            m.query_id().to_string(),
            m.total_row_count().to_string(),
            format!("{:.4}%", selectivity_percentage),
            format!("{:.2?}", m.baseline()),
            format!("{:.2?}", m.optimized()),
            formatted_delta,
            formatted_perf_change,
            // sql.to_string(),
        ]);
    }

    println!("{table}");
    println!("Slow Queries: {slow_query_count} of {query_count}");
}

async fn count_parquet_rows(ctx: &SessionContext, path: &str) -> Result<usize> {
    let df = ctx
        .read_parquet(path, ParquetReadOptions::default())
        .await?;
    let total_rows = df.count().await?;

    Ok(total_rows)
}

async fn execute_sql(ctx: &SessionContext, sql: &str) -> Result<Duration> {
    let df = ctx
        .sql(sql)
        .instrument(tracing::trace_span!("create_dataframe_from_sql", sql=%sql))
        .await?;

    let start = Instant::now();
    df.collect()
        .instrument(tracing::trace_span!("execute_sql"))
        .await?;
    let duration = start.elapsed();

    info!("Completed in {duration:?}");

    Ok(duration)
}

#[derive(Debug)]
struct QueryComparisonMetrics {
    query_id: usize,
    baseline_query: Duration,
    optimized_query: Duration,
    total_row_count: usize,
}

enum PerfChange {
    Speedup(f32),
    Slowdown(f32),
}

impl QueryComparisonMetrics {
    fn new(
        query_id: usize,
        baseline_query: Duration,
        optimized_query: Duration,
        total_row_count: usize,
    ) -> Self {
        Self {
            query_id,
            baseline_query,
            optimized_query,
            total_row_count,
        }
    }

    fn baseline(&self) -> Duration {
        self.baseline_query
    }

    fn optimized(&self) -> Duration {
        self.optimized_query
    }

    fn total_row_count(&self) -> usize {
        self.total_row_count
    }

    fn query_id(&self) -> usize {
        self.query_id
    }

    fn is_speedup(&self) -> bool {
        self.baseline_query > self.optimized_query
    }

    fn abs_diff(&self) -> Duration {
        self.optimized_query.abs_diff(self.baseline_query)
    }

    fn get_change_in_performance(&self) -> PerfChange {
        let baseline = self.baseline_query.as_secs_f32();
        let optimized = self.optimized_query.as_secs_f32();

        if self.is_speedup() {
            PerfChange::Speedup(baseline / optimized)
        } else {
            PerfChange::Slowdown(optimized / baseline)
        }
    }
}

#[instrument(name = "query_comparison", skip_all, fields(query_id = %query_id, sql = %sql, row_count, speedup, slowdown, delta, optimized_duration, baseline_duration
))]
async fn run_comparison(
    query_id: usize,
    sql: &str,
    ctx_baseline: &SessionContext,
    ctx_optimized: &SessionContext,
) -> Result<QueryComparisonMetrics> {
    let baseline = execute_sql(ctx_baseline, sql)
        .instrument(tracing::trace_span!("run", run_type = "baseline"))
        .await?;

    let optimized = execute_sql(ctx_optimized, sql)
        .instrument(tracing::trace_span!("run", run_type = "optimized"))
        .await?;

    let row_count = ctx_baseline.sql(sql).await?.count().await?;
    tracing::Span::current().record("row_count", row_count);

    if row_count > 0 {
        let df = ctx_optimized.sql(sql).await?;
        let df = df.sort(vec![col("id").sort(true, true)])?;
        let df = df.limit(0, Some(5))?;
        let output = df.to_string().await?;
        trace!("Showing first 5 rows:\n{output}\n");
    }

    let metrics = QueryComparisonMetrics::new(query_id, baseline, optimized, row_count);

    tracing::Span::current().record("baseline_duration", format!("{:?}", metrics.baseline()));
    tracing::Span::current().record("optimized_duration", format!("{:?}", metrics.optimized()));

    match metrics.get_change_in_performance() {
        PerfChange::Speedup(change) => {
            tracing::Span::current().record("delta", format!("-{:?}", metrics.abs_diff()));
            tracing::Span::current().record("speedup", format!("{change:.3}X"));
        }
        PerfChange::Slowdown(change) => {
            tracing::Span::current().record("delta", format!("+{:?}", metrics.abs_diff()));
            tracing::Span::current().record("slowdown", format!("{change:.3}X"));
        }
    }

    Ok(metrics)
}
