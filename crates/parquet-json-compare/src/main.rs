use parquet_common::contact::PhoneType;
use proptest::prelude::{BoxedStrategy, Just, Strategy};
use proptest::prop_oneof;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;

fn phone_type_strategy() -> BoxedStrategy<PhoneType> {
    prop_oneof![
        55 => Just(PhoneType::Mobile),
        35 => Just(PhoneType::Work),
        10 => Just(PhoneType::Home),
    ]
    .boxed()
}

fn phone_type_vec_strategy(size: usize) -> BoxedStrategy<Vec<PhoneType>> {
    proptest::collection::vec(phone_type_strategy(), size).boxed()
}

fn main() {
    let mut runner = TestRunner::default();

    let batch_10_k = phone_type_vec_strategy(10_000)
        .new_tree(&mut runner)
        .expect("Failed to generate 10K batch")
        .current();

    println!("Generated 100K batch. First 5: {:?}", &batch_10_k[0..5]);
    println!("100K batch size: {}", batch_10_k.len());

    let (mobile_count, work_count, home_count) = batch_10_k.iter().fold(
        (0usize, 0usize, 0usize), // Initial tuple (all counts zero)
        |(mut mobile, mut work, mut home), phone_type| {
            match phone_type {
                PhoneType::Mobile => mobile += 1,
                PhoneType::Work => work += 1,
                PhoneType::Home => home += 1,
            }
            (mobile, work, home) // Return the updated tuple
        },
    );

    println!(
        "Mobile: {} {}",
        mobile_count,
        mobile_count as f32 / batch_10_k.len() as f32
    );
    println!(
        "Work: {} {}",
        work_count,
        work_count as f32 / batch_10_k.len() as f32
    );
    println!(
        "Home: {} {}",
        home_count,
        home_count as f32 / batch_10_k.len() as f32
    );
}
