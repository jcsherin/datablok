use crate::datagen::{
    ContactRecordBatchGenerator, RecordBatchGenerator, RecordBatchGeneratorFactory,
};
use crate::skew::MAX_PHONES_PER_CONTACT;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use human_format::Formatter;
use log::info;
use parquet::arrow::ArrowWriter;

use rayon::prelude::*;
use std::error::Error;
use std::fmt::Debug;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use std::{fmt, thread};

/// Configuration for the parallel data generation and writing pipeline.
///
/// This struct centralizes all the tunable parameters for a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// The total number of record to generate.
    target_records: usize,
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
    /// The Arrow Schema
    arrow_schema: SchemaRef,
}

impl PipelineConfig {
    pub fn target_records(&self) -> usize {
        self.target_records
    }

    pub fn total_batches(&self) -> usize {
        self.target_records.div_ceil(self.record_batch_size)
    }

    pub fn record_batch_size(&self) -> usize {
        self.record_batch_size
    }

    pub fn num_writers(&self) -> usize {
        self.num_writers
    }

    pub fn num_producers(&self) -> usize {
        self.num_producers
    }

    pub fn output_filename(&self) -> &str {
        &self.output_filename
    }

    pub fn output_dir(&self) -> &Path {
        self.output_dir.as_path()
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            target_records: 10_000,
            num_writers: 2,
            num_producers: 8,
            record_batch_size: 1024,
            output_dir: PathBuf::from("output"),
            output_filename: "out".to_string(),
            arrow_schema: std::sync::Arc::new(Schema::empty()),
        }
    }
}

#[derive(Debug)]
pub enum PipelineConfigError {
    ZeroProducers {
        total_threads: usize,
        num_writers: usize,
    },
    ZeroWriters,
    MissingSchema,
    TargetRecordsTooLarge {
        target_records: usize,
        max_records: u64,
        total_phone_numbers: u64,
        max_phones_per_record: usize,
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
            PipelineConfigError::ZeroWriters => {
                write!(f, "Number of writers must be greater than zero.")
            }
            PipelineConfigError::MissingSchema => {
                write!(
                    f,
                    "The pipeline config is missing an Arrow schema definition.",
                )
            }
            PipelineConfigError::TargetRecordsTooLarge {
                target_records,
                max_records,
                total_phone_numbers,
                max_phones_per_record,
            } => {
                write!(
                    f,
                    concat!(
                        "Configuration error: The requested number of target_records ({}) ",
                        "is too high. Based on the system's capacity of {} unique ",
                        "phone numbers and a maximum of {} phones per record, the ",
                        "limit for target_records is {}. Please adjust the configuration."
                    ),
                    target_records, total_phone_numbers, max_phones_per_record, max_records
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

    pub fn with_target_records(mut self, target_records: usize) -> Self {
        self.inner.target_records = target_records;
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

    pub fn with_arrow_schema(mut self, arrow_schema: SchemaRef) -> Self {
        self.inner.arrow_schema = arrow_schema;
        self
    }

    pub fn try_build(mut self) -> Result<PipelineConfig, PipelineConfigError> {
        if self.inner.num_writers == 0 {
            return Err(PipelineConfigError::ZeroWriters);
        }

        let total_threads = rayon::current_num_threads();
        let num_producers = total_threads.saturating_sub(self.inner.num_writers);

        self.inner.num_producers = num_producers;
        if self.inner.num_producers == 0 {
            return Err(PipelineConfigError::ZeroProducers {
                total_threads,
                num_writers: self.inner.num_writers,
            });
        }

        if self.inner.arrow_schema.fields().is_empty() {
            return Err(PipelineConfigError::MissingSchema);
        }

        let max_possible_phones = self.inner.target_records as u64 * MAX_PHONES_PER_CONTACT as u64;
        if max_possible_phones >= ContactRecordBatchGenerator::PHONE_NUMBER_UPPER_BOUND {
            return Err(PipelineConfigError::TargetRecordsTooLarge {
                target_records: self.inner.target_records,
                max_records: ContactRecordBatchGenerator::PHONE_NUMBER_UPPER_BOUND
                    / MAX_PHONES_PER_CONTACT as u64,
                total_phone_numbers: ContactRecordBatchGenerator::PHONE_NUMBER_UPPER_BOUND,
                max_phones_per_record: MAX_PHONES_PER_CONTACT,
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
    parquet_schema: SchemaRef,
) -> thread::JoinHandle<Result<usize, Box<dyn Error + Send + Sync>>> {
    thread::spawn(move || {
        let parquet_file = File::create(path.as_path())?;
        let mut parquet_writer = ArrowWriter::try_new(parquet_file, parquet_schema, None)?;

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
            "Finished writing parquet file: {path}. Wrote {count} records.",
            path = path.display(),
            count = human_formatter.format(count as f64)
        );

        Ok(total_bytes)
    })
}

pub fn run_pipeline(
    config: &PipelineConfig,
    factory: &impl RecordBatchGeneratorFactory,
) -> Result<PipelineMetrics, Box<dyn Error + Send + Sync>> {
    let start_time = Instant::now();

    let mut writers: Vec<_> = Vec::new();
    let mut senders = Vec::new();

    for i in 0..config.num_writers() {
        let (tx, rx) = mpsc::sync_channel(config.num_producers() * 2); // double buffer depth

        let output_filename = format!(
            "{filename}_{i}.parquet",
            filename = config.output_filename()
        );
        let output_path = config.output_dir().join(output_filename);

        let writer = create_writer_thread(output_path, rx, config.arrow_schema());

        writers.push(writer);
        senders.push(tx);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(config.num_producers)
        .build()
        .unwrap();

    let processing_result_from_pool = pool.install(|| {
        let num_batches = config.target_records.div_ceil(config.record_batch_size);

        // We will parallelize the work by giving each producer thread a range of chunks to process.
        let processing_result = (0..num_batches).into_par_iter().try_for_each(
            |batch_index| -> Result<(), Box<dyn Error + Send + Sync>> {
                let start_row = batch_index * config.record_batch_size;
                let current_batch_size =
                    std::cmp::min(config.record_batch_size, config.target_records - start_row);

                if current_batch_size == 0 {
                    return Ok(());
                }

                let record_batch = factory
                    .create_generator(batch_index)
                    .generate(current_batch_size)?;

                let sender_id = batch_index % config.num_writers;

                senders[sender_id]
                    .send(record_batch)
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
    let records_per_sec = (config.target_records as f64) / elapsed_time.as_secs_f64();

    Ok(PipelineMetrics {
        total_in_memory_bytes,
        elapsed_time,
        records_per_sec,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datagen::ContactRecordBatchGenerator;
    use crate::skew::MAX_PHONES_PER_CONTACT;

    #[test]
    fn test_config_builder_fails_on_too_many_records() {
        let schema = parquet_nested_common::prelude::get_contact_schema(); // Use a real schema

        // Calculate a number of records that is just over the limit.
        let max_records =
            ContactRecordBatchGenerator::PHONE_NUMBER_UPPER_BOUND / MAX_PHONES_PER_CONTACT as u64;
        let invalid_target_records = (max_records + 1) as usize;

        let builder = PipelineConfigBuilder::new()
            .with_arrow_schema(schema)
            .with_target_records(invalid_target_records);

        let result = builder.try_build();
        let error = result.unwrap_err();

        let expected_message = format!(
            concat!(
                "Configuration error: The requested number of target_records ({}) ",
                "is too high. Based on the system's capacity of {} unique ",
                "phone numbers and a maximum of {} phones per record, the ",
                "limit for target_records is {}. Please adjust the configuration."
            ),
            invalid_target_records,
            ContactRecordBatchGenerator::PHONE_NUMBER_UPPER_BOUND,
            MAX_PHONES_PER_CONTACT,
            max_records
        );

        assert_eq!(error.to_string(), expected_message);
    }

    #[test]
    fn test_config_builder_fails_on_zero_writers() {
        let builder = PipelineConfigBuilder::new().with_num_writers(0);
        let result = builder.try_build();
        let error = result.unwrap_err();

        let expected_message = "Number of writers must be greater than zero.".to_string();

        assert_eq!(error.to_string(), expected_message);

        if let PipelineConfigError::ZeroWriters = error {
            // Correct error variant caught
        } else {
            panic!(
                "Expected ZeroWriters error, but got a different error: {:?}",
                error
            );
        }
    }
}
