use crate::datagen::ContactRecordBatchGenerator;
use arrow::record_batch::RecordBatch;
use human_format::Formatter;
use log::info;
use parquet::arrow::ArrowWriter;
use parquet_nested_common::prelude::*;
use rayon::prelude::*;
use std::error::Error;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

/// Configuration for the parallel data generation and writing pipeline.
///
/// This struct centralizes all the tunable parameters for a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// The total number of contacts to generate.
    pub target_contacts: usize,
    /// The number of concurrent writer threads.
    pub num_writers: usize,
    /// The number of producer threads in the rayon thread pool.
    pub num_producers: usize,
    /// The number of rows per RecordBatch.
    pub record_batch_size: usize,
    /// The directory where the output Parquet files will be written.
    pub output_dir: PathBuf,
}

/// Contains performance metrics from a completed pipeline run.
#[derive(Debug, PartialEq)]
pub struct PipelineMetrics {
    /// The total in-memory size of all RecordBatches that were written.
    pub total_in_memory_bytes: usize,
    /// The total time taken for the entire pipeline to complete.
    pub elapsed_time: Duration,
}

fn create_writer_thread(
    path: PathBuf,
    rx: mpsc::Receiver<RecordBatch>,
) -> thread::JoinHandle<Result<usize, Box<dyn Error + Send + Sync>>> {
    thread::spawn(move || {
        let parquet_schema = get_contact_schema();
        let parquet_file = File::create(path)?;
        let mut parquet_writer = ArrowWriter::try_new(parquet_file, parquet_schema.clone(), None)?;

        let mut count = 0;
        let mut total_bytes = 0;

        for record_batch in rx {
            // Track the in-memory size of the batch
            total_bytes += record_batch.get_array_memory_size();

            parquet_writer.write(&record_batch)?;
            count += record_batch.num_rows();
        }

        parquet_writer.close()?;

        let mut human_formatter = Formatter::new();
        human_formatter.with_decimals(0).with_separator("");
        info!(
            "Finished writing parquet file. Wrote {} contacts.",
            human_formatter.format(count as f64)
        );

        Ok(total_bytes)
    })
}

pub fn run_pipeline(
    config: &PipelineConfig,
) -> Result<PipelineMetrics, Box<dyn Error + Send + Sync>> {
    let start_time = Instant::now();
    // Constraint: phone numbers are unique globally
    let phone_id_counter = Arc::new(AtomicUsize::new(0));

    // Hardcoded to 4 writers for now as per the original main.rs
    // This will be made dynamic in a future refactoring.
    let (tx1, rx1) = mpsc::sync_channel(config.num_producers);
    let (tx2, rx2) = mpsc::sync_channel(config.num_producers);
    let (tx3, rx3) = mpsc::sync_channel(config.num_producers);
    let (tx4, rx4) = mpsc::sync_channel(config.num_producers);

    let writer_handle_1 = create_writer_thread(config.output_dir.join("contacts_1.parquet"), rx1);
    let writer_handle_2 = create_writer_thread(config.output_dir.join("contacts_2.parquet"), rx2);
    let writer_handle_3 = create_writer_thread(config.output_dir.join("contacts_3.parquet"), rx3);
    let writer_handle_4 = create_writer_thread(config.output_dir.join("contacts_4.parquet"), rx4);

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(config.num_producers)
        .build()
        .unwrap();

    pool.install(|| {
        let num_batches = config.target_contacts.div_ceil(config.record_batch_size);
        let parquet_schema = get_contact_schema();

        // Create clones of the resources that will be moved into each thread.
        let senders = (Arc::new(tx1), Arc::new(tx2), Arc::new(tx3), Arc::new(tx4));
        let phone_id_counter = phone_id_counter.clone();

        // We will parallelize the work by giving each producer thread a range of chunks to process.
        (0..num_batches).into_par_iter().for_each_init(
            || {
                (
                    ContactRecordBatchGenerator::new(
                        parquet_schema.clone(),
                        phone_id_counter.clone(),
                    ),
                    senders.clone(),
                )
            },
            |(generator_state, senders), batch_index| {
                let start_row = batch_index * config.record_batch_size;
                let current_batch_size =
                    std::cmp::min(config.record_batch_size, config.target_contacts - start_row);

                if current_batch_size == 0 {
                    return;
                }

                let rb = generator_state
                    .generate(batch_index as u64, current_batch_size)
                    .expect("Failed to generate fused record batch");

                match batch_index % 4 {
                    0 => senders.0.send(rb).expect("Failed to send to rx1"),
                    1 => senders.1.send(rb).expect("Failed to send to rx2"),
                    2 => senders.2.send(rb).expect("Failed to send to rx3"),
                    _ => senders.3.send(rb).expect("Failed to send to rx4"),
                }
            },
        );

        // The original senders (tx1, tx2, etc.) are still owned by the main thread.
        // We drop them here, which closes the channels. The writer threads will
        // then finish their work and terminate gracefully.
        drop(senders);
    });

    // Teardown
    let bytes1 = writer_handle_1.join().unwrap()?;
    let bytes2 = writer_handle_2.join().unwrap()?;
    let bytes3 = writer_handle_3.join().unwrap()?;
    let bytes4 = writer_handle_4.join().unwrap()?;
    let total_in_memory_bytes = bytes1 + bytes2 + bytes3 + bytes4;

    let elapsed_time = start_time.elapsed();

    Ok(PipelineMetrics {
        total_in_memory_bytes,
        elapsed_time,
    })
}
