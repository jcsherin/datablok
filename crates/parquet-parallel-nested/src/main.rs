use arrow::array::{ListBuilder, StringBuilder, StringDictionaryBuilder, StructBuilder};
use arrow::datatypes::{SchemaRef, UInt8Type};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use fake::Fake;
use fake::faker::name::en::{FirstName, LastName};
use human_format::Formatter;
use log::{LevelFilter, info};
use parquet::arrow::ArrowWriter;
use parquet_common::prelude::*;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rayon::prelude::*;
use std::error::Error;
use std::fmt::{Debug, Display, Write};
use std::fs::File;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Instant;

fn generate_phone_type(rng: &mut impl Rng) -> PhoneType {
    match rng.random_range(0..100) {
        0..=54 => PhoneType::Mobile, // 0.55
        55..=89 => PhoneType::Work,  // 0.35
        _ => PhoneType::Home,        // 0.10
    }
}

fn generate_partial_phone(rng: &mut impl Rng) -> PartialPhone {
    match rng.random_range(0..100) {
        0..=89 => PartialPhone(Some(generate_phone_type(rng)), true), // 0.90
        90..=94 => PartialPhone(None, true),                          // 0.05
        95..=98 => PartialPhone(Some(generate_phone_type(rng)), false), // 0.04
        _ => PartialPhone(None, false),                               // 0.01
    }
}

fn generate_phones(rng: &mut impl Rng) -> Vec<PartialPhone> {
    let num_phones = match rng.random_range(0..100) {
        0..=39 => 0,                  // 0.40
        40..=84 => 1,                 // 0.45
        85..=94 => 2,                 // 0.10
        _ => rng.random_range(3..=5), // 0.05
    };

    (0..num_phones)
        .map(|_| generate_partial_phone(rng))
        .collect()
}

fn generate_name(rng: &mut impl Rng, name_buf: &mut String) -> Option<String> {
    if rng.random_bool(0.8) {
        write!(
            name_buf,
            "{} {}",
            FirstName().fake::<&str>(),
            LastName().fake::<&str>(),
        )
        .unwrap();

        let name = Some(name_buf.clone());
        name_buf.clear();

        name
    } else {
        None
    }
}

fn generate_partial_contact(rng: &mut impl Rng, name_buf: &mut String) -> PartialContact {
    let name = generate_name(rng, name_buf);
    let phones = generate_phones(rng);

    PartialContact(name, phones)
}

fn generate_contacts_chunk(size: usize, seed: u64) -> Vec<PartialContact> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut contacts = Vec::with_capacity(size);
    let mut name_buf = String::with_capacity(32);

    for _ in 0..size {
        contacts.push(generate_partial_contact(&mut rng, &mut name_buf));
    }

    contacts
}

/// An intermediate value Phone struct value.
///
/// The first argument is the optional `phone_type`.
/// The second argument indicates if `phone_number` is present.
///
/// The `phone_number` is unique globally across the generated dataset. During dataset generation
/// if the second argument is `true` then a unique `phone_number` is slotted in, otherwise if it
/// is `false`, then the `phone_number` is not present in the `Phone` struct.
#[derive(Debug, Clone)]
struct PartialPhone(Option<PhoneType>, bool);

#[derive(Debug, Clone)]
struct PartialContact(Option<String>, Vec<PartialPhone>);

#[allow(dead_code)]
#[derive(Debug)]
struct GeneratorState {
    schema: SchemaRef,
    name: StringBuilder,
    phone_number_buf: String,
    counter: Arc<AtomicUsize>,
    phones: ListBuilder<StructBuilder>,
    current_chunks: usize,
}

#[allow(dead_code)]
enum GeneratorStateError {
    NotEnoughChunks { current: usize, required: usize },
    TryFlushZeroChunks,
}

impl Debug for GeneratorStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeneratorStateError::NotEnoughChunks { current, required } => f
                .debug_struct("NotEnoughChunks")
                .field("current", current)
                .field("required", required)
                .finish(),
            GeneratorStateError::TryFlushZeroChunks => {
                f.debug_struct("TryFlushZeroChunks").finish()
            }
        }
    }
}

impl Display for GeneratorStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeneratorStateError::NotEnoughChunks { current, required } => {
                write!(
                    f,
                    "Not enough chunks to build a RecordBatch. Current:{current}. Required: {required}.",
                )
            }
            GeneratorStateError::TryFlushZeroChunks => {
                write!(f, "Flush attempted but no chunks were present.")
            }
        }
    }
}

impl Error for GeneratorStateError {}

impl GeneratorState {
    #[allow(dead_code)]
    const PHONE_NUMBER_LENGTH: usize = 16;
    #[allow(dead_code)]
    const CHUNK_MULTIPLIER: usize = 16;
    #[allow(dead_code)]
    fn new(schema: SchemaRef, counter: Arc<AtomicUsize>) -> Self {
        let name = StringBuilder::new();
        let phone_type = StringDictionaryBuilder::<UInt8Type>::new();
        let phone_number = StringBuilder::new();
        let phone_number_buf = String::with_capacity(Self::PHONE_NUMBER_LENGTH);
        let phone = StructBuilder::new(
            get_contact_phone_fields(),
            vec![Box::new(phone_number), Box::new(phone_type)],
        );
        let phones = ListBuilder::new(phone);

        GeneratorState {
            schema,
            name,
            phone_number_buf,
            counter,
            phones,
            current_chunks: 0,
        }
    }

    #[allow(dead_code)]
    fn append(&mut self, chunk: Vec<PartialContact>) -> Result<(), Box<dyn Error>> {
        for PartialContact(name, phones) in chunk {
            self.name.append_option(name);

            if phones.is_empty() {
                // Handles zero phones in contact
                self.phones.append_null();
            } else {
                // Handles at least one ore more phones
                let builder = self.phones.values();

                for PartialPhone(phone_type, has_number) in phones {
                    builder.append(true);

                    // Field: `Phone.number`
                    let phone_number_builder = builder
                        .field_builder::<StringBuilder>(PHONE_NUMBER_FIELD_INDEX)
                        .ok_or_else(|| {
                            Box::<dyn Error>::from(
                                "Expected `number` field at idx: 0 of `Phone` struct builder.",
                            )
                        })?;
                    if has_number {
                        let uniq_suffix = self.counter.fetch_add(1, Ordering::Relaxed);

                        write!(self.phone_number_buf, "+91-99-{uniq_suffix:08}").unwrap();
                        phone_number_builder.append_value(&self.phone_number_buf);

                        self.phone_number_buf.clear();
                    } else {
                        phone_number_builder.append_option(None::<String>);
                    }

                    // Field: `Phone.phone_type`
                    builder
                        .field_builder::<StringDictionaryBuilder<UInt8Type>>(PHONE_TYPE_FIELD_INDEX)
                        .ok_or_else(|| Box::<dyn Error>::from("Expected `phone_type` field at idx: {PHONE_TYPE_FIELD_INDEX} of `Phone` struct builder."))?
                        .append_option(phone_type);
                }

                self.phones.append(true);
            }
        }

        self.current_chunks += 1;

        Ok(())
    }

    #[allow(dead_code)]
    fn try_inner(&mut self) -> Result<RecordBatch, ArrowError> {
        let rb = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(self.name.finish()), Arc::new(self.phones.finish())],
        )?;

        self.name = StringBuilder::new();

        let phone_type = StringDictionaryBuilder::<UInt8Type>::new();
        let phone_number = StringBuilder::new();
        let phone = StructBuilder::new(
            get_contact_phone_fields(),
            vec![Box::new(phone_number), Box::new(phone_type)],
        );
        self.phones = ListBuilder::new(phone);

        self.current_chunks = 0;

        Ok(rb)
    }

    #[allow(dead_code)]
    fn try_get_record_batch(&mut self) -> Result<RecordBatch, Box<dyn Error>> {
        if self.current_chunks < Self::CHUNK_MULTIPLIER {
            return Err(Box::new(GeneratorStateError::NotEnoughChunks {
                current: self.current_chunks,
                required: Self::CHUNK_MULTIPLIER,
            }));
        }

        let rb = self.try_inner()?;
        Ok(rb)
    }

    #[allow(dead_code)]
    fn try_flush_record_batch(&mut self) -> Result<RecordBatch, Box<dyn Error>> {
        if self.current_chunks == 0 {
            return Err(Box::new(GeneratorStateError::TryFlushZeroChunks));
        }

        // Todo: for sanity mark this GeneratorState as invalid!
        let rb = self.try_inner()?;
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
        let chunk_count = target_contacts.div_ceil(BASE_CHUNK_SIZE);
        let parquet_schema = get_contact_schema();

        // Create clones of the resources that will be moved into each thread.
        let senders = (Arc::new(tx1), Arc::new(tx2), Arc::new(tx3), Arc::new(tx4));
        let phone_id_counter = phone_id_counter.clone();

        // We will parallelize the work by giving each producer thread a range of chunks to process.
        (0..num_producers).into_par_iter().for_each(|producer_id| {
            // Each thread gets its own state and a clone of the senders.
            let mut generator_state =
                GeneratorState::new(parquet_schema.clone(), phone_id_counter.clone());

            // Determine the range of chunks this specific thread is responsible for.
            let chunks_per_producer = chunk_count.div_ceil(num_producers);
            let start_chunk = producer_id * chunks_per_producer;
            let end_chunk = ((producer_id + 1) * chunks_per_producer).min(chunk_count);

            // Process the assigned range of chunks.
            for chunk_index in start_chunk..end_chunk {
                let start_index = chunk_index * BASE_CHUNK_SIZE;
                let current_chunk_size =
                    std::cmp::min(BASE_CHUNK_SIZE, target_contacts - start_index);

                if current_chunk_size == 0 {
                    continue;
                }

                let seed = chunk_index as u64;
                let partial_contacts = generate_contacts_chunk(current_chunk_size, seed);

                // Append the generated data to the thread's state.
                generator_state
                    .append(partial_contacts)
                    .expect("Failed to append Vec<PartialContact> to GeneratorState.");

                // If enough chunks have been appended, a RecordBatch is ready. Send it.
                if let Ok(rb) = generator_state.try_get_record_batch() {
                    match chunk_index % 4 {
                        0 => senders
                            .0
                            .send(rb)
                            .expect("Failed to send RecordBatch to rx1"),
                        1 => senders
                            .1
                            .send(rb)
                            .expect("Failed to send RecordBatch to rx2"),
                        2 => senders
                            .2
                            .send(rb)
                            .expect("Failed to send RecordBatch to rx3"),
                        _ => senders
                            .3
                            .send(rb)
                            .expect("Failed to send RecordBatch to rx4"),
                    }
                }
            }

            // After the thread has processed all its chunks, there might be leftover
            // data in its state. We must flush this as a final RecordBatch.
            if let Ok(rb) = generator_state.try_flush_record_batch() {
                // We use the producer_id for round-robin distribution of the final batch.
                match producer_id % 4 {
                    0 => senders
                        .0
                        .send(rb)
                        .expect("Failed to send final RecordBatch to rx1"),
                    1 => senders
                        .1
                        .send(rb)
                        .expect("Failed to send final RecordBatch to rx2"),
                    2 => senders
                        .2
                        .send(rb)
                        .expect("Failed to send final RecordBatch to rx3"),
                    _ => senders
                        .3
                        .send(rb)
                        .expect("Failed to send final RecordBatch to rx4"),
                }
            }
        });

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
