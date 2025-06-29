use human_format::Formatter;
use log::{LevelFilter, info};
use parquet_common::contact::PhoneType;
use proptest::prelude::{BoxedStrategy, Just, Strategy};
use proptest::prop_oneof;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngSeed, TestRunner};
use rayon::prelude::*;
use std::time::Instant;

// A Zipfian-like categorical distribution for `phone_type`
//
// | Phone Type | Probabiltiy |
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

#[allow(dead_code)]
fn phone_type_vec_strategy(size: usize) -> BoxedStrategy<Vec<PhoneType>> {
    proptest::collection::vec(phone_type_strategy(), size).boxed()
}

fn generate_phone_type_chunk(size: usize, seed: u64) -> Vec<PhoneType> {
    let mut runner = TestRunner::new(Config {
        rng_seed: RngSeed::Fixed(seed),
        ..Config::default()
    });

    let strategy = proptest::collection::vec(phone_type_strategy(), size).boxed();
    strategy
        .new_tree(&mut runner)
        .expect("Failed to generate chunk")
        .current()
}

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    let target_size: usize = 10_000_000;
    let num_threads = rayon::current_num_threads();
    let base_chunk_size = target_size.div_ceil(num_threads);

    let mut human_formatter = Formatter::new();

    human_formatter.with_decimals(0).with_separator("");

    info!(
        "Generating {} values across {num_threads} threads with chunk size {}.",
        human_formatter.format(target_size as f64),
        human_formatter.format(base_chunk_size as f64),
    );

    // Begin parallel data generation
    let start_generation_time = Instant::now();

    let (mobile_count, work_count, home_count) = (0..num_threads)
        .into_par_iter()
        .flat_map_iter(|i| {
            let start_index = i * base_chunk_size;
            let end_index = (i + 1) * base_chunk_size;

            let current_chunk_size = target_size.min(end_index) - start_index;

            if current_chunk_size > 0 {
                // The choice of unique seed is the chunk index
                generate_phone_type_chunk(current_chunk_size, i as u64)
            } else {
                Vec::new()
            }
        })
        .fold(
            || (0usize, 0usize, 0usize),
            |(mut mobile, mut work, mut home), phone_type: PhoneType| {
                match phone_type {
                    PhoneType::Mobile => mobile += 1,
                    PhoneType::Home => home += 1,
                    PhoneType::Work => work += 1,
                }
                (mobile, work, home)
            },
        )
        .reduce(
            || (0usize, 0usize, 0usize),
            |(m1, w1, h1), (m2, w2, h2)| (m1 + m2, w1 + w2, h1 + h2),
        );

    let elapsed_generation_time = start_generation_time.elapsed();

    info!("Generation time (parallel) is: {elapsed_generation_time:?}");

    info!("Mobile count: {mobile_count}");
    info!("Work count: {work_count}");
    info!("Home count: {home_count}");
}
