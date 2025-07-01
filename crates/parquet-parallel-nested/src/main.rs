use arrow::array::{ListBuilder, StringBuilder, StringDictionaryBuilder, StructBuilder};
use arrow::datatypes::{SchemaRef, UInt8Type};
use arrow::record_batch::RecordBatch;
use fake::Fake;
use fake::faker::name::en::{FirstName, LastName};
use human_format::Formatter;
use log::{LevelFilter, info};
use parquet::arrow::ArrowWriter;
use parquet_common::prelude::*;
use proptest::prelude::{BoxedStrategy, Just, Strategy};
use proptest::prop_oneof;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngSeed, TestRunner};
use rayon::prelude::*;
use std::error::Error;
use std::fmt::Write;
use std::fs::File;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
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

            let record_batch =
                to_record_batch(parquet_schema.clone(), &phone_id_counter, partial_contacts)
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
