use criterion::{Criterion, criterion_group, criterion_main};
use parquet_parallel_nested::datagen::generate_name;
use parquet_parallel_nested::datagen::get_phone_template;
use rand::SeedableRng;
use rand::prelude::StdRng;
use std::hint::black_box;
use std::time::Duration;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("nested data generator");
    group.measurement_time(Duration::from_secs(20));

    const SEED: u64 = 123456;
    const NAME_SIZE: usize = 32;
    let rng = &mut StdRng::seed_from_u64(SEED);
    let mut name_buf = String::with_capacity(NAME_SIZE);

    group.bench_function("generate names", |b| {
        b.iter(|| black_box(generate_name(rng, &mut name_buf)))
    });

    group.bench_function("generate phones struct template", |b| {
        b.iter(|| black_box(get_phone_template(rng)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
