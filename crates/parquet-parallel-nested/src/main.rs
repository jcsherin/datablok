use arrow::record_batch::RecordBatch;
use fake::Fake;
use fake::faker::name::en::{FirstName, LastName};
use human_format::Formatter;
use log::{LevelFilter, info};
use parquet::arrow::ArrowWriter;
use parquet_common::prelude::{
    Contact, ContactBuilder, PhoneBuilder, PhoneType, create_record_batch, get_contact_schema,
};
use proptest::prelude::{BoxedStrategy, Just, Strategy};
use proptest::prop_oneof;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngSeed, TestRunner};
use rayon::prelude::*;
use std::error::Error;
use std::fs::File;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

// A Zipfian-like categorical distribution for `phone_type`
//
// | Phone Type | Probability |
// |------------|-------------|
// | Mobile     | 0.55        |
// | Work       | 0.35        |
// | Home       | 0.10        |
//
fn phone_type_strategy() -> BoxedStrategy<PhoneType> {
    prop_oneof![
        55 => Just(PhoneType::Mobile),
        35 => Just(PhoneType::Work),
        10 => Just(PhoneType::Home),
    ]
    .boxed()
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

// Categorical Distribution for properties of Phone struct
//
//
// | `phone_number` | `phone_type` | Probability |
// |----------------|--------------|-------------|
// | Some(_)        | Some(_)      | 90%         |
// | Some(_)        | None         | 5%          |
// | None           | Some(_)      | 4%          |
// | None           | None         | 1%          |
//
fn phone_struct_strategy() -> BoxedStrategy<PartialPhone> {
    let strategy = prop_oneof![
        90 => (phone_type_strategy().prop_map(Some), Just(true)),
        5 => (Just(None), Just(true)),
        4 => (phone_type_strategy().prop_map(Some), Just(false)),
        1 => (Just(None), Just(false)),
    ]
    .boxed();

    strategy
        .prop_map(|(phone_type, has_phone_number)| PartialPhone(phone_type, has_phone_number))
        .boxed()
}

// Cardinality of phones per contact
//
// | Cardinality | Probability |
// |-------------|-------------|
// |          0  |         40% |
// |          1  |         45% |
// |          2  |         10% |
// |          3+ |          5% |
//
fn phones_strategy() -> BoxedStrategy<Vec<PartialPhone>> {
    let strategy = phone_struct_strategy();
    prop_oneof![
        40 => Just(Vec::new()),
        45 => proptest::collection::vec(strategy.clone(), 1),
        10 => proptest::collection::vec(strategy.clone(), 2),
        5 => proptest::collection::vec(strategy.clone(), 3..=5),
    ]
    .boxed()
}

// Uses fake to generate realistic names
// | Name    | Probability |
// |---------|-------------|
// | Some(_) |         80% |
// | None    |         20% |
//
fn name_strategy() -> BoxedStrategy<Option<String>> {
    prop_oneof![
        80 => Just(()).prop_map(|_| Some(format!("{} {}", FirstName().fake::<String>(), LastName().fake::<String>()))),
        20 => Just(None)
    ].boxed()
}

fn contact_strategy() -> BoxedStrategy<PartialContact> {
    (name_strategy(), phones_strategy())
        .prop_map(|(name, phones)| PartialContact(name, phones))
        .boxed()
}

fn generate_contacts_chunk(size: usize, seed: u64) -> Vec<PartialContact> {
    let mut runner = TestRunner::new(Config {
        rng_seed: RngSeed::Fixed(seed),
        ..Config::default()
    });

    let strategy = proptest::collection::vec(contact_strategy(), size);
    strategy
        .new_tree(&mut runner)
        .expect("Failed to generate chunk of partial contacts")
        .current()
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
    let num_threads = rayon::current_num_threads();

    let mut human_formatter = Formatter::new();
    human_formatter.with_decimals(0).with_separator("");

    info!(
        "Generating {} contacts across {num_threads} threads. Chunk size: {}.",
        human_formatter.format(target_contacts as f64),
        human_formatter.format(BASE_CHUNK_SIZE as f64),
    );

    // Constraint: phone numbers are unique globally
    let phone_id_counter = AtomicUsize::new(0);
    let start_time = Instant::now();

    let (tx, rx) = mpsc::sync_channel::<RecordBatch>(num_threads * 2);

    let writer_handle = thread::spawn(move || -> Result<(), Box<dyn Error + Send + Sync>> {
        let parquet_schema = get_contact_schema();
        let parquet_file = File::create("contacts.parquet")?;
        let mut parquet_writer = ArrowWriter::try_new(parquet_file, parquet_schema.clone(), None)?;

        let mut count = 0;

        for record_batch in rx {
            parquet_writer.write(&record_batch)?;
            count += record_batch.num_rows();
        }

        parquet_writer.close()?;
        info!(
            "Finished writing parquet file. Wrote {} contacts.",
            human_formatter.format(count as f64)
        );

        Ok(())
    });

    let chunk_count = target_contacts.div_ceil(BASE_CHUNK_SIZE);
    let parquet_schema = get_contact_schema();

    (0..chunk_count)
        .into_par_iter()
        .for_each_with(tx, |tx, chunk_index| {
            let start_index = chunk_index * BASE_CHUNK_SIZE;
            let current_chunk_size = std::cmp::min(BASE_CHUNK_SIZE, target_contacts - start_index);

            if current_chunk_size == 0 {
                return;
            }

            let seed = chunk_index as u64;
            let partial_contacts = generate_contacts_chunk(current_chunk_size, seed);

            // Assemble the Vec<Contact> for this small chunk
            let contacts_chunk: Vec<Contact> = partial_contacts
                .into_iter()
                .map(|partial_contact| {
                    let PartialContact(name, partial_phones) = partial_contact;
                    let phones_iter = partial_phones.into_iter().map(|partial_phone| {
                        let PartialPhone(phone_type, has_number) = partial_phone;
                        let mut builder = PhoneBuilder::default();
                        if has_number {
                            let id = phone_id_counter.fetch_add(1, Ordering::Relaxed);
                            builder = builder.with_number(format!("+91-99-{id:08}"));
                        }
                        if let Some(pt) = phone_type {
                            builder = builder.with_phone_type(pt);
                        }
                        builder.build()
                    });
                    let mut builder = ContactBuilder::default();
                    if let Some(name) = name {
                        builder = builder.with_name(name);
                    }
                    builder.with_phones(phones_iter).build()
                })
                .collect();

            // Convert the chunk to a RecordBatch and send it to the writer
            let record_batch = create_record_batch(parquet_schema.clone(), &contacts_chunk)
                .expect("Failed to create RecordBatch");
            tx.send(record_batch).unwrap();
        });

    // Teardown
    writer_handle.join().unwrap()?;
    info!(
        "Total generation and write time: {:?}.",
        start_time.elapsed()
    );

    Ok(())
}
