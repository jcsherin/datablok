use criterion::{criterion_group, criterion_main, Criterion};
use parquet_nested_common::prelude::get_contact_schema;
use parquet_nested_parallel::datagen::ContactRecordBatchGenerator;
use parquet_nested_parallel::skew::{generate_name, generate_phone_template};
use rand::prelude::StdRng;
use rand::SeedableRng;
use std::hint::black_box;
use std::time::Duration;

/// Benchmarks for primitive data generation functions in `skew.rs`.
fn skew_primitives_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("skew primitives generator");
    group.measurement_time(Duration::from_secs(20));

    const SEED: u64 = 0;
    const NAME_SIZE: usize = 32;
    let rng = &mut StdRng::seed_from_u64(SEED);
    let mut name_buf = String::with_capacity(NAME_SIZE);

    group.bench_function("generate names", |b| {
        b.iter(|| black_box(generate_name(rng, &mut name_buf)))
    });

    group.bench_function("generate phones struct template", |b| {
        b.iter(|| black_box(generate_phone_template(rng)))
    });
}

/// Benchmarks for generating full `RecordBatch` of contacts.
fn contact_record_batch_generator_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("ContactRecordBatchGenerator");
    group.measurement_time(Duration::from_secs(20));

    let schema = get_contact_schema();
    const SEED: u64 = 0;
    const RECORD_BATCH_SIZES: &[usize] = &[1024, 2048, 4096, 8192];

    for &size in RECORD_BATCH_SIZES {
        group.bench_function(format!("generate {} records", size), |b| {
            let generator = ContactRecordBatchGenerator::new(schema.clone());
            b.iter(|| black_box(generator.generate(SEED, size, 0).unwrap()))
        });
    }
}

criterion_group!(
    benches,
    skew_primitives_benchmark,
    contact_record_batch_generator_benchmark
);
criterion_main!(benches);
