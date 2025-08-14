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

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    env_logger::builder()
        .format_timestamp_micros()
        .filter_level(LevelFilter::Info)
        .init();

    const TARGET_RECORDS: usize = 10_000_000;
    const RECORD_BATCH_SIZE: usize = 4096;
    const NUM_WRITERS: usize = 4; // This is still coupled to the 4 hardcoded channels in pipeline.rs
    let output_dir = PathBuf::from("output_parquet");

    fs::create_dir_all(&output_dir)?;

    let config = PipelineConfigBuilder::new()
        .with_target_records(TARGET_RECORDS)
        .with_num_writers(NUM_WRITERS)
        .with_record_batch_size(RECORD_BATCH_SIZE)
        .with_output_dir(output_dir)
        .with_output_filename("contacts".to_string())
        .with_arrow_schema(get_contact_schema())
        .try_build()?;

    info!("Config: {config:?}");

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
