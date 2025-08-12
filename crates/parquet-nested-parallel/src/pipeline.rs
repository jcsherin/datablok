use crate::datagen::ContactRecordBatchGenerator;
use arrow::record_batch::RecordBatch;
use human_format::Formatter;
use log::info;
use parquet::arrow::ArrowWriter;
use parquet_nested_common::prelude::*;
use rayon::prelude::*;
use std::error::Error;
use std::fmt::Debug;
use std::fs::File;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use std::{fmt, thread};

/// Configuration for the parallel data generation and writing pipeline.
///
/// This struct centralizes all the tunable parameters for a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// The total number of contacts to generate.
    target_contacts: usize,
    /// The number of concurrent writer threads.
    num_writers: usize,
    /// The number of producer threads in the rayon thread pool.
    num_producers: usize,
    /// The number of rows per RecordBatch.
    record_batch_size: usize,
    /// The directory where the output Parquet files will be written.
    output_dir: PathBuf,
    /// The filename to use when output Parquet files are written.
    output_filename: String,
}

impl PipelineConfig {
    pub fn target_contacts(&self) -> usize {
        self.target_contacts
    }

    pub fn num_writers(&self) -> usize {
        self.num_writers
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            target_contacts: 10_000,
            num_writers: 2,
            num_producers: 8,
            record_batch_size: 1024,
            output_dir: PathBuf::from("output"),
            output_filename: "out".to_string(),
        }
    }
}

#[derive(Debug)]
pub enum PipelineConfigError {
    ZeroProducers {
        total_threads: usize,
        num_writers: usize,
    },
}

impl fmt::Display for PipelineConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineConfigError::ZeroProducers {
                total_threads,
                num_writers,
            } => {
                write!(
                    f,
                    "No. of producer threads must be greater than zero. Total threads: {total_threads}. Writer threads: {num_writers}.",
                )
            }
        }
    }
}

impl Error for PipelineConfigError {}

#[derive(Debug, Default)]
pub struct PipelineConfigBuilder {
    inner: PipelineConfig,
}

impl PipelineConfigBuilder {
    pub fn new() -> Self {
        Self {
            inner: PipelineConfig::default(),
        }
    }

    pub fn with_target_contacts(mut self, target_contacts: usize) -> Self {
        self.inner.target_contacts = target_contacts;
        self
    }

    pub fn with_num_writers(mut self, num_writers: usize) -> Self {
        self.inner.num_writers = num_writers;
        self
    }

    pub fn with_record_batch_size(mut self, record_batch_size: usize) -> Self {
        self.inner.record_batch_size = record_batch_size;
        self
    }

    pub fn with_output_dir(mut self, output_dir: PathBuf) -> Self {
        self.inner.output_dir = output_dir;
        self
    }

    pub fn with_output_filename(mut self, output_filename: String) -> Self {
        self.inner.output_filename = output_filename;
        self
    }

    pub fn try_build(mut self) -> Result<PipelineConfig, PipelineConfigError> {
        let total_threads = rayon::current_num_threads();
        let num_producers = total_threads.saturating_sub(self.inner.num_writers);

        self.inner.num_producers = num_producers;
        if self.inner.num_producers == 0 {
            return Err(PipelineConfigError::ZeroProducers {
                total_threads,
                num_writers: self.inner.num_writers,
            });
        }

        Ok(self.inner)
    }
}

/// Contains performance metrics from a completed pipeline run.
#[derive(Debug, PartialEq)]
pub struct PipelineMetrics {
    /// The total in-memory size of all RecordBatches that were written.
    pub total_in_memory_bytes: usize,
    /// The total time taken for the entire pipeline to complete.
    pub elapsed_time: Duration,
    /// The total throughput of the pipeline relative to records processed.
    pub records_per_sec: f64,
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

    let mut writers: Vec<_> = Vec::new();
    let mut senders = Vec::new();

    for i in 0..config.num_writers {
        let (tx, rx) = mpsc::sync_channel(config.num_producers * 2); // double buffer depth

        let output_filename = format!("{filename}_{i}.parquet", filename = config.output_filename);
        let output_path = config.output_dir.join(output_filename);

        let writer = create_writer_thread(output_path, rx);

        writers.push(writer);
        senders.push(tx);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(config.num_producers)
        .build()
        .unwrap();

    let processing_result_from_pool = pool.install(|| {
        let num_batches = config.target_contacts.div_ceil(config.record_batch_size);
        let parquet_schema = get_contact_schema();
        let phone_number_batch_size =
            ContactRecordBatchGenerator::PHONE_NUMBER_UPPER_BOUND.div_ceil(num_batches);

        // We will parallelize the work by giving each producer thread a range of chunks to process.
        let processing_result = (0..num_batches).into_par_iter().try_for_each(
            |batch_index| -> Result<(), Box<dyn Error + Send + Sync>> {
                let start_row = batch_index * config.record_batch_size;
                let current_batch_size =
                    std::cmp::min(config.record_batch_size, config.target_contacts - start_row);

                if current_batch_size == 0 {
                    return Ok(());
                }

                let phone_id_offset = batch_index * phone_number_batch_size;
                let rb = ContactRecordBatchGenerator::new(parquet_schema.clone()).generate(
                    batch_index as u64,
                    current_batch_size,
                    phone_id_offset,
                )?;

                let sender_id = batch_index % config.num_writers;

                senders[sender_id]
                    .send(rb)
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

                Ok(())
            },
        );

        // The original senders (tx1, tx2, etc.) are still owned by the main thread.
        // We drop them here, which closes the channels. The writer threads will
        // then finish their work and terminate gracefully.
        drop(senders);
        processing_result
    });

    processing_result_from_pool?;

    let mut total_in_memory_bytes = 0;
    let mut writer_errors: Vec<String> = Vec::new();

    // Graceful Shutdown
    for (i, writer_handle) in writers.into_iter().enumerate() {
        match writer_handle.join() {
            Ok(Ok(bytes)) => {
                total_in_memory_bytes += bytes;
            }
            Ok(Err(e)) => {
                let error_msg = format!("Writer thread {i} failed with error: {e:?}");
                log::error!("{}", &error_msg);
                writer_errors.push(error_msg);
            }
            Err(e) => {
                let owned_error_msg;
                let msg = if let Some(s) = e.downcast_ref::<&'static str>() {
                    *s
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.as_str()
                } else if let Some(error) = e.downcast_ref::<Box<dyn Error + Send + 'static>>() {
                    owned_error_msg = error.to_string();
                    owned_error_msg.as_str()
                } else {
                    "unknown error"
                };

                let error_msg = format!("Writer thread {i} panicked: {msg}");
                log::error!("{}", &error_msg);
                writer_errors.push(error_msg);
            }
        }
    }

    if !writer_errors.is_empty() {
        return Err(writer_errors.join("\n").into());
    }

    let elapsed_time = start_time.elapsed();
    let records_per_sec = (config.target_contacts as f64) / elapsed_time.as_secs_f64();

    Ok(PipelineMetrics {
        total_in_memory_bytes,
        elapsed_time,
        records_per_sec,
    })
}
