use criterion::{criterion_group, criterion_main, Criterion};
use parquet_nested_common::prelude::get_contact_schema;
use parquet_nested_parallel::datagen::RecordBatchGenerator;
use parquet_nested_parallel::datagen::{ContactGeneratorFactory, RecordBatchGeneratorFactory};
use parquet_nested_parallel::skew::{generate_name, generate_phone_template};
use rand::prelude::StdRng;
use rand::SeedableRng;
use std::hint::black_box;
use std::sync::mpsc;
use std::time::Duration;
use tempfile::NamedTempFile;

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

    const BATCH_INDEX: usize = 123;
    const RECORD_BATCH_SIZES: &[usize] = &[1024, 2048, 4096, 8192];

    for &size in RECORD_BATCH_SIZES {
        group.bench_function(format!("generate {} records", size), |b| {
            let record_batch_size = 4096;

            let factory = ContactGeneratorFactory::new(get_contact_schema(), record_batch_size);

            b.iter(|| {
                black_box(
                    factory
                        .create_generator(BATCH_INDEX)
                        .generate(size)
                        .unwrap(),
                )
            })
        });
    }
}

/// Benchmark for Parquet writer.
fn contact_record_batch_writer_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("ContactRecordBatchWriter");
    group.measurement_time(Duration::from_secs(20));
    // group.sample_size(20); // Reduced sample size for slower, I/O-bound benchmark

    const RECORD_BATCH_SIZES: &[usize] = &[1024, 2048, 4096, 8192];
    const NUM_BATCHES_TO_WRITE: usize = 1000;

    for &size in RECORD_BATCH_SIZES {
        let factory = ContactGeneratorFactory::new(get_contact_schema(), size);
        let batches_to_write: Vec<_> = (0..NUM_BATCHES_TO_WRITE)
            .map(|i| factory.create_generator(i).generate(size).unwrap())
            .collect();
        let schema = batches_to_write[0].schema();

        let bench_name = format!("write {} records (x{} batches)", size, NUM_BATCHES_TO_WRITE);
        group.bench_function(bench_name, |b| {
            b.iter_batched(
                || {
                    let (tx, rx) = mpsc::channel();
                    for batch in &batches_to_write {
                        tx.send(batch.clone()).unwrap();
                    }
                    drop(tx); // Close the sender to signal end of stream.
                    rx
                },
                // The routine to be measured.
                |rx| {
                    let temp_file = NamedTempFile::new().unwrap();
                    black_box(
                        parquet_nested_parallel::pipeline::writer_thread_inner(
                            temp_file.path().to_path_buf(),
                            rx,
                            schema.clone(),
                        )
                        .unwrap(),
                    );
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
}

criterion_group!(
    benches,
    skew_primitives_benchmark,
    contact_record_batch_generator_benchmark,
    contact_record_batch_writer_benchmark
);
criterion_main!(benches);
