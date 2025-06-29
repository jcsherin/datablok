use fake::Fake;
use fake::faker::name::en::{FirstName, LastName};
use human_format::Formatter;
use log::{LevelFilter, info};
use parquet_common::prelude::{Contact, ContactBuilder, PhoneBuilder, PhoneType};
use proptest::prelude::{BoxedStrategy, Just, Strategy};
use proptest::prop_oneof;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngSeed, TestRunner};
use rayon::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
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

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    let target_contacts: usize = 10_000_000;
    let num_threads = rayon::current_num_threads();
    let base_chunk_size = target_contacts.div_ceil(num_threads);

    let mut human_formatter = Formatter::new();
    human_formatter.with_decimals(0).with_separator("");

    info!(
        "Generating {} contacts across {num_threads} threads. Chunk size: ~{}.",
        human_formatter.format(target_contacts as f64),
        human_formatter.format(base_chunk_size as f64),
    );

    // Constraint: phone numbers are unique globally
    let phone_id_counter = AtomicUsize::new(0);

    // Begin parallel data generation
    let start_generation_time = Instant::now();

    let contacts: Vec<Contact> = (0..num_threads)
        .into_par_iter()
        .flat_map(|i| {
            let start_index = i * base_chunk_size;
            let current_chunk_size =
                target_contacts.min(start_index + base_chunk_size) - start_index;

            if current_chunk_size == 0 {
                return Vec::new();
            }

            generate_contacts_chunk(current_chunk_size, i as u64)
                .into_iter()
                .map(|partial_contact| {
                    let PartialContact(name, partial_phones) = partial_contact;

                    let phones = partial_phones.into_iter().map(|partial_phone| {
                        let PartialPhone(phone_type, has_phone_number) = partial_phone;

                        let mut builder = PhoneBuilder::default();
                        if has_phone_number {
                            let unique_id = phone_id_counter.fetch_add(1, Ordering::Relaxed);
                            builder = builder.with_number(format!("+91-99-{unique_id:08}"));
                        }
                        if let Some(value) = phone_type {
                            builder = builder.with_phone_type(value);
                        }

                        builder.build()
                    });

                    let mut builder = ContactBuilder::default();

                    if let Some(value) = name {
                        builder = builder.with_name(value);
                    }
                    builder.with_phones(phones).build()
                })
                .collect::<Vec<Contact>>()
        })
        .collect();

    let elapsed_generation_time = start_generation_time.elapsed();

    info!("Generation time (parallel) is: {elapsed_generation_time:?}");
    info!(
        "Generated a total of {} contacts",
        human_formatter.format(contacts.len() as f64)
    );

    info!("--- First 10 Generated Contacts ---");
    for (i, contact) in contacts.iter().take(10).enumerate() {
        info!("Contact {}: {:?}", i + 1, contact);
    }
}
