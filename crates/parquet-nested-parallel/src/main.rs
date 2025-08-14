use clap::Parser;
use human_format::Formatter;
use log::{info, LevelFilter};
use parquet_nested_common::prelude::get_contact_schema;
use parquet_nested_parallel::datagen::ContactGeneratorFactory;
use parquet_nested_parallel::pipeline::{run_pipeline, PipelineConfigBuilder};
use std::error::Error;
use std::fs;
use std::path::PathBuf;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

/// A tool for generating and writing nested Parquet data in parallel.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// The target number of records to generate.
    #[arg(long, default_value_t = 10_000_000)]
    target_records: usize,

    /// The size of each record batch.
    #[arg(long, default_value_t = 4096)]
    record_batch_size: usize,

    /// The number of parallel writers.
    #[arg(long, default_value_t = 4)]
    num_writers: usize,

    /// The output directory for the Parquet files.
    #[arg(long, default_value = "output_parquet")]
    output_dir: String,

    /// The base filename for the output Parquet files.
    #[arg(long, default_value = "contacts")]
    output_filename: String,

    /// If true, the pipeline will not run, but the effective configuration will be printed.
    #[arg(long)]
    dry_run: bool,
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    env_logger::builder()
        .format_timestamp_micros()
        .filter_level(LevelFilter::Info)
        .init();

    let cli = Cli::parse();

    let command_string = format!(
        "--target-records {} --record-batch-size {} --num-writers {} --output-dir {} --output-filename {}",
        cli.target_records,
        cli.record_batch_size,
        cli.num_writers,
        cli.output_dir,
        cli.output_filename
    );
    info!("Running with command: {command_string}");

    let output_dir = PathBuf::from(cli.output_dir);

    fs::create_dir_all(&output_dir)?;

    let config = PipelineConfigBuilder::new()
        .with_target_records(cli.target_records)
        .with_num_writers(cli.num_writers)
        .with_record_batch_size(cli.record_batch_size)
        .with_output_dir(output_dir)
        .with_output_filename(cli.output_filename)
        .with_arrow_schema(get_contact_schema())
        .try_build()?;

    if cli.dry_run {
        info!("Dry run enabled. Exiting without running the pipeline.");
        return Ok(());
    }

    info!("Running with configuration: {config:?}");

    let factory = ContactGeneratorFactory::from_config(&config);
    let metrics = run_pipeline(&config, &factory)?;

    info!(
        "Total generation and write time: {:?}.",
        metrics.elapsed_time
    );

    // Throughput
    let elapsed_secs = metrics.elapsed_time.as_secs_f64();
    let gb_per_sec = (metrics.total_in_memory_bytes as f64 / 1_000_000_000.0) / elapsed_secs;

    let mut human_formatter = Formatter::new();
    human_formatter.with_decimals(0).with_separator("");

    info!(
        "Record Throughput: {} records/sec",
        human_formatter.format(metrics.records_per_sec)
    );
    info!("In-Memory Throughput: {gb_per_sec:.2} GB/s");

    info!("Exiting main");
    Ok(())
}
