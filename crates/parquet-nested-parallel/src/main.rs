use human_format::Formatter;
use log::{LevelFilter, info};
use parquet_nested_parallel::pipeline::{PipelineConfig, run_pipeline};
use std::error::Error;
use std::fs;
use std::path::PathBuf;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    env_logger::builder().filter_level(LevelFilter::Info).init();

    const TARGET_CONTACTS: usize = 10_000_000;
    const RECORD_BATCH_SIZE: usize = 4096;
    const BASE_CHUNK_SIZE: usize = 256;
    const NUM_WRITERS: usize = 4; // This is still coupled to the 4 hardcoded channels in pipeline.rs
    let num_producers = rayon::current_num_threads().saturating_sub(NUM_WRITERS);
    let output_dir = PathBuf::from("output_parquet");

    fs::create_dir_all(&output_dir)?;

    let config = PipelineConfig {
        target_contacts: TARGET_CONTACTS,
        num_writers: NUM_WRITERS,
        num_producers,
        record_batch_size: RECORD_BATCH_SIZE,
        output_dir,
        output_filename: "contacts".to_string(),
    };

    let mut human_formatter = Formatter::new();
    human_formatter.with_decimals(0).with_separator("");

    info!(
        "Generating {} contacts across {} cores. Chunk size: {}.",
        human_formatter.format(config.target_contacts as f64),
        rayon::current_num_threads(),
        human_formatter.format(BASE_CHUNK_SIZE as f64),
    );

    let metrics = run_pipeline(&config)?;

    info!(
        "Total generation and write time: {:?}.",
        metrics.elapsed_time
    );

    // Throughput
    let elapsed_secs = metrics.elapsed_time.as_secs_f64();
    let records_per_sec = (config.target_contacts as f64) / elapsed_secs;
    let gb_per_sec = (metrics.total_in_memory_bytes as f64 / 1_000_000_000.0) / elapsed_secs;

    info!(
        "Record Throughput: {} records/sec",
        human_formatter.format(records_per_sec)
    );
    info!("In-Memory Throughput: {gb_per_sec:.2} GB/s");

    Ok(())
}
