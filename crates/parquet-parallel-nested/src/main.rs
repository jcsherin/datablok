use arrow::array::{ListBuilder, StringBuilder, StringDictionaryBuilder, StructBuilder};
use arrow::datatypes::{SchemaRef, UInt8Type};
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
use std::fmt::Write;
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

fn to_record_batch(
    schema: SchemaRef,
    phone_id_counter: &AtomicUsize,
    chunk: Vec<PartialContact>,
) -> Result<RecordBatch, Box<dyn Error>> {
    let mut name_builder = StringBuilder::new();

    let phone_number_builder = StringBuilder::new();
    let phone_type_builder = StringDictionaryBuilder::<UInt8Type>::new();
    let phone_struct_builder = StructBuilder::new(
        get_contact_phone_fields(),
        vec![Box::new(phone_number_builder), Box::new(phone_type_builder)],
    );

    let mut phones_list_builder = ListBuilder::new(phone_struct_builder);

    let mut phone_number_buf = String::with_capacity(16);

    for PartialContact(name, phones) in chunk {
        name_builder.append_option(name);

        if phones.is_empty() {
            phones_list_builder.append_null();
        } else {
            let struct_builder = phones_list_builder.values();

            for PartialPhone(phone_type, has_phone_number) in phones {
                struct_builder.append(true);

                if has_phone_number {
                    let id = phone_id_counter.fetch_add(1, Ordering::Relaxed);
                    write!(phone_number_buf, "+91-99-{id:08}")?;
                    struct_builder
                        .field_builder::<StringBuilder>(PHONE_NUMBER_FIELD_INDEX)
                        .unwrap()
                        .append_value(&phone_number_buf);

                    phone_number_buf.clear();
                } else {
                    struct_builder
                        .field_builder::<StringBuilder>(PHONE_NUMBER_FIELD_INDEX)
                        .unwrap()
                        .append_option(None::<String>);
                }

                struct_builder
                    .field_builder::<StringDictionaryBuilder<UInt8Type>>(PHONE_TYPE_FIELD_INDEX)
                    .unwrap()
                    .append_option(phone_type);
            }

            phones_list_builder.append(true);
        }
    }

    let name_array = Arc::new(name_builder.finish());
    let phones_array = Arc::new(phones_list_builder.finish());

    RecordBatch::try_new(schema, vec![name_array, phones_array]).map_err(Into::into)
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
    let phone_id_counter = AtomicUsize::new(0);
    let start_time = Instant::now();

    let (tx1, rx1) = mpsc::sync_channel::<RecordBatch>(num_producers);
    let (tx2, rx2) = mpsc::sync_channel::<RecordBatch>(num_producers);
    // let (tx3, rx3) = mpsc::sync_channel::<RecordBatch>(num_producers);
    // let (tx4, rx4) = mpsc::sync_channel::<RecordBatch>(num_producers);

    let writer_handle_1 = create_writer_thread("contacts_1.parquet", rx1);
    let writer_handle_2 = create_writer_thread("contacts_2.parquet", rx2);
    // let writer_handle_3 = create_writer_thread("contacts_3.parquet", rx3);
    // let writer_handle_4 = create_writer_thread("contacts_4.parquet", rx4);

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_producers)
        .build()
        .unwrap();

    pool.install(|| {
        let chunk_count = target_contacts.div_ceil(BASE_CHUNK_SIZE);
        let parquet_schema = get_contact_schema();
        // let senders = (tx1, tx2, tx3, tx4);
        let senders = (tx1, tx2);

        (0..chunk_count)
            .into_par_iter()
            // .for_each_with(senders, |(s1, s2, s3, s4), chunk_index| {
            .for_each_with(senders, |(s1, s2), chunk_index| {
                let start_index = chunk_index * BASE_CHUNK_SIZE;
                let current_chunk_size =
                    std::cmp::min(BASE_CHUNK_SIZE, target_contacts - start_index);

                if current_chunk_size == 0 {
                    return;
                }

                let seed = chunk_index as u64;

                let partial_contacts = generate_contacts_chunk(current_chunk_size, seed);

                let record_batch =
                    to_record_batch(parquet_schema.clone(), &phone_id_counter, partial_contacts)
                        .expect("Failed to create RecordBatch");

                match chunk_index % 2 {
                    0 => s1.send(record_batch).expect("Failed to send to rx1"),
                    _ => s2.send(record_batch).expect("Failed to send to rx2"),
                    // 2 => s3.send(record_batch).expect("Failed to send to rx3"),
                    // _ => s4.send(record_batch).expect("Failed to send to rx4"),
                }
            });
    });

    // Teardown
    let bytes1 = writer_handle_1.join().unwrap()?;
    let bytes2 = writer_handle_2.join().unwrap()?;
    // let bytes3 = writer_handle_3.join().unwrap()?;
    // let bytes4 = writer_handle_4.join().unwrap()?;
    // let total_in_memory_bytes = bytes1 + bytes2 + bytes3 + bytes4;
    let total_in_memory_bytes = bytes1 + bytes2;

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
