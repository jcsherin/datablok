use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use parquet_nested_parallel::pipeline::{PipelineConfig, run_pipeline};
use std::time::Duration;
use tempfile::tempdir;

fn pipeline_throughput_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_throughput_benchmark");
    group.measurement_time(Duration::from_secs(60));

    let target_contacts_sizes = &[100_000, 1_000_000, 10_000_000];
    let num_writers = 4;
    let num_producers = rayon::current_num_threads().saturating_sub(num_writers);

    for &target_contacts in target_contacts_sizes {
        group.bench_function(
            format!(
                "{} contacts, {} parquet writer threads, {} data generator threads",
                target_contacts, num_writers, num_producers
            ),
            |b| {
                b.iter_batched(
                    || {
                        let temp_dir = tempdir().unwrap();
                        let config = PipelineConfig {
                            target_contacts,
                            num_writers,
                            num_producers,
                            record_batch_size: 4096,
                            output_dir: temp_dir.path().to_path_buf(),
                            output_filename: "contacts".to_string(),
                        };

                        (config, temp_dir)
                    },
                    |(config, _temp_dir)| {
                        // The code to be benchmarked
                        black_box(run_pipeline(&config).unwrap());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, pipeline_throughput_benchmark);
criterion_main!(benches);
