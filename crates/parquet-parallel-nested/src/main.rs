use arrow::array::{ListBuilder, StringBuilder, StringDictionaryBuilder, StructBuilder};
use arrow::datatypes::{SchemaRef, UInt8Type};
use arrow::record_batch::RecordBatch;
use human_format::Formatter;
use log::{LevelFilter, info};
use parquet::arrow::ArrowWriter;
use parquet_common::prelude::*;
use parquet_parallel_nested::datagen::{generate_name, get_num_phones, get_phone_template};
use rand::SeedableRng;
use rand::prelude::StdRng;
use rayon::prelude::*;
use std::error::Error;
use std::fmt::{Debug, Write};
use std::fs::File;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Instant;

#[derive(Debug)]
struct RecordBatchGenerator {
    schema: SchemaRef,
    counter: Arc<AtomicUsize>,
}

impl RecordBatchGenerator {
    const PHONE_NUMBER_LENGTH: usize = 16;

    fn new(schema: SchemaRef, counter: Arc<AtomicUsize>) -> Self {
        RecordBatchGenerator { schema, counter }
    }

    fn get(&mut self, seed: u64, count: usize) -> Result<RecordBatch, Box<dyn Error>> {
        let mut name = StringBuilder::new();
        let phone_type = StringDictionaryBuilder::<UInt8Type>::new();
        let phone_number = StringBuilder::new();
        let mut phone_number_buf = String::with_capacity(Self::PHONE_NUMBER_LENGTH);
        let phone = StructBuilder::new(
            get_contact_phone_fields(),
            vec![Box::new(phone_number), Box::new(phone_type)],
        );
        let mut phones = ListBuilder::new(phone);

        let rng = &mut StdRng::seed_from_u64(seed);
        let mut name_buf = String::with_capacity(32);

        for _ in 0..count {
            name.append_option(generate_name(rng, &mut name_buf));

            let phones_count = get_num_phones(rng);
            if phones_count == 0 {
                phones.append_null();
            } else {
                let builder = phones.values();

                for _ in 0..phones_count {
                    builder.append(true);

                    let (has_number, phone_type) = get_phone_template(rng);

                    let phone_number_builder = builder
                        .field_builder::<StringBuilder>(PHONE_NUMBER_FIELD_INDEX)
                        .ok_or_else(|| {
                            Box::<dyn Error>::from(
                                "Expected `number` field at idx: 0 of `Phone` struct builder.",
                            )
                        })?;

                    if has_number {
                        let uniq_suffix = self.counter.fetch_add(1, Ordering::Relaxed);

                        write!(phone_number_buf, "+91-99-{uniq_suffix:08}").unwrap();
                        phone_number_builder.append_value(&phone_number_buf);

                        phone_number_buf.clear();
                    } else {
                        phone_number_builder.append_option(None::<String>);
                    }

                    builder
                        .field_builder::<StringDictionaryBuilder<UInt8Type>>(PHONE_TYPE_FIELD_INDEX)
                        .ok_or_else(|| Box::<dyn Error>::from("Expected `phone_type` field at idx: {PHONE_TYPE_FIELD_INDEX} of `Phone` struct builder."))?
                        .append_option(phone_type);
                }

                phones.append(true);
            }
        }

        let rb = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(name.finish()), Arc::new(phones.finish())],
        )?;

        Ok(rb)
    }
}

fn create_writer_thread(
    path: &'static str,
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

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    env_logger::builder().filter_level(LevelFilter::Info).init();

    let target_contacts: usize = 10_000_000;

    const RECORD_BATCH_SIZE: usize = 4096;
    const BASE_CHUNK_SIZE: usize = 256;
    let num_cores = rayon::current_num_threads();
    const NUM_WRITERS: usize = 2;
    let num_producers = num_cores.saturating_sub(NUM_WRITERS);

    let mut human_formatter = Formatter::new();
    human_formatter.with_decimals(0).with_separator("");

    info!(
        "Generating {} contacts across {num_cores} cores. Chunk size: {}.",
        human_formatter.format(target_contacts as f64),
        human_formatter.format(BASE_CHUNK_SIZE as f64),
    );

    // Constraint: phone numbers are unique globally
    let phone_id_counter = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    let (tx1, rx1) = mpsc::sync_channel::<RecordBatch>(num_producers);
    let (tx2, rx2) = mpsc::sync_channel::<RecordBatch>(num_producers);
    let (tx3, rx3) = mpsc::sync_channel::<RecordBatch>(num_producers);
    let (tx4, rx4) = mpsc::sync_channel::<RecordBatch>(num_producers);

    let writer_handle_1 = create_writer_thread("contacts_1.parquet", rx1);
    let writer_handle_2 = create_writer_thread("contacts_2.parquet", rx2);
    let writer_handle_3 = create_writer_thread("contacts_3.parquet", rx3);
    let writer_handle_4 = create_writer_thread("contacts_4.parquet", rx4);

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_producers)
        .build()
        .unwrap();
    pool.install(|| {
        let num_batches = target_contacts.div_ceil(RECORD_BATCH_SIZE);
        let parquet_schema = get_contact_schema();

        // Create clones of the resources that will be moved into each thread.
        let senders = (Arc::new(tx1), Arc::new(tx2), Arc::new(tx3), Arc::new(tx4));
        let phone_id_counter = phone_id_counter.clone();

        // We will parallelize the work by giving each producer thread a range of chunks to process.
        (0..num_batches).into_par_iter().for_each_init(
            || {
                (
                    RecordBatchGenerator::new(parquet_schema.clone(), phone_id_counter.clone()),
                    senders.clone(),
                )
            },
            |(generator_state, senders), batch_index| {
                let start_row = batch_index * RECORD_BATCH_SIZE;
                let current_batch_size =
                    std::cmp::min(RECORD_BATCH_SIZE, target_contacts - start_row);

                if current_batch_size == 0 {
                    return;
                }

                let rb = generator_state
                    .get(batch_index as u64, current_batch_size)
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

    let elapsed = start_time.elapsed();
    info!("Total generation and write time: {elapsed:?}.");

    // Throughput
    let elapsed_secs = elapsed.as_secs_f64();
    let records_per_sec = (target_contacts as f64) / elapsed_secs;
    let gb_per_sec = (total_in_memory_bytes as f64 / 1_000_000_000.0) / elapsed_secs;

    info!(
        "Record Throughput: {} records/sec",
        human_formatter.format(records_per_sec)
    );
    info!("In-Memory Throughput: {gb_per_sec:.2} GB/s");

    Ok(())
}
