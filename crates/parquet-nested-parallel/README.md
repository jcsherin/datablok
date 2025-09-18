A fast, parallel Parquet nested data generator built using [Arrow and Parquet].

I wrote a [blog post] documenting the sequence of performance improvements in
reducing the total runtime, and improving the CPU efficiency fixing one
bottleneck after another.

Skip to [Implementation Notes](#implementation-notes).

### Generating a Synthetic Dataset

First, build in `release` mode:

```text
cargo build -p parquet-nested-parallel --release
```

To generate a dataset with 10 million rows:

```text
$ RUST_LOG=info ./target/release/parquet-nested-parallel --target-records 10000000

[2025-09-18T08:26:30.233440Z INFO  parquet_nested_parallel] Running with command: --target-records 10000000 --record-batch-size 4096 --num-writers 4 --output-dir output_parquet --output-filename contacts
[2025-09-18T08:26:30.234610Z INFO  parquet_nested_parallel] Running with configuration: PipelineConfig { target_records: 10000000, num_writers: 4, num_producers: 8, record_batch_size: 4096, output_dir: "output_parquet", output_filename: "contacts", arrow_schema: Schema { fields: [Field { name: "name", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "phones", data_type: List(Field { name: "item", data_type: Struct([Field { name: "number", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "phone_type", data_type: Dictionary(UInt8, Utf8), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }], metadata: {} } }
[2025-09-18T08:26:30.654594Z INFO  parquet_nested_parallel::pipeline] Finished writing parquet file: output_parquet/contacts_2.parquet. Wrote 2M records.
[2025-09-18T08:26:30.656607Z INFO  parquet_nested_parallel::pipeline] Finished writing parquet file: output_parquet/contacts_3.parquet. Wrote 2M records.
[2025-09-18T08:26:30.659806Z INFO  parquet_nested_parallel::pipeline] Finished writing parquet file: output_parquet/contacts_0.parquet. Wrote 3M records.
[2025-09-18T08:26:30.661166Z INFO  parquet_nested_parallel::pipeline] Finished writing parquet file: output_parquet/contacts_1.parquet. Wrote 3M records.
[2025-09-18T08:26:30.661234Z INFO  parquet_nested_parallel] Total generation and write time: 426.414666ms.
[2025-09-18T08:26:30.661243Z INFO  parquet_nested_parallel] Record Throughput: 23M records/sec
[2025-09-18T08:26:30.661245Z INFO  parquet_nested_parallel] In-Memory Throughput: 1.20 GB/s
[2025-09-18T08:26:30.661248Z INFO  parquet_nested_parallel] Exiting main
```

The parquet files are written to `output_parquet/` directory:

```text
$ du -ah output_parquet                                                                                                                          02:09:33 PM
 73M    output_parquet/contacts_3.parquet
 73M    output_parquet/contacts_2.parquet
 73M    output_parquet/contacts_0.parquet
 73M    output_parquet/contacts_1.parquet
291M    output_parquet
```

### Command-line Options

```text
A tool for generating and writing nested Parquet data in parallel

Usage: parquet-nested-parallel [OPTIONS]

Options:
      --target-records <TARGET_RECORDS>
          The target number of records to generate [default: 10000000]
      --record-batch-size <RECORD_BATCH_SIZE>
          The size of each record batch [default: 4096]
      --num-writers <NUM_WRITERS>
          The number of parallel writers [default: 4]
      --output-dir <OUTPUT_DIR>
          The output directory for the Parquet files [default: output_parquet]
      --output-filename <OUTPUT_FILENAME>
          The base filename for the output Parquet files [default: contacts]
      --dry-run
          If true, the pipeline will not run, but the effective configuration will be printed
  -h, --help
          Print help
  -V, --version
          Print version
```

### Local Dev CI Script

To run the workflow in GitHub CI during local development:

```text
./scripts/verify.sh parquet-nested-parallel --verbose
```

This will execute cargo commands: `fmt`, `check`, `clippy`, and `test`.

### Implementation Notes

A [Zipfian-like] data skew is configured for each field in the nested data
structure, to generate a dataset which better reflects real-world data
distribution, which is not uniformly random. This is a set of light-weight
functions defined in [skew.rs].

The data generator is fused together with Arrow `RecordBatch` creation to
increase throughput. This is a direct transformation from a native nested
`struct` to an Arrow `RecordBatch` without other intermediate transformations.
The low-level shredding API is imperative and difficult to use relative to flat
array builders. This complexity is high even for a single level of nesting and
a low number of fields. The fused shredding implementation is in [datagen.rs].

#### Parallel Execution

The total work is split into batches the size of a `RecordBatch`, and the data
generator threads work in parallel without any coordination, or shared data
structures to stream completed `RecordBatch`es to a writer thread. Each writer
thread writes a separate Parquet file to disk.

The entry point for parallel execution pipeline is in [pipeline.rs].

```rust
let config = PipelineConfigBuilder::new()
    .with_target_records(cli.target_records)
    .with_num_writers(cli.num_writers)
    .with_record_batch_size(cli.record_batch_size)
    .with_output_dir(output_dir)
    .with_output_filename(cli.output_filename)
    .with_arrow_schema(get_contact_schema())
    .try_build()?;

let factory = ContactGeneratorFactory::from_config(&config);
let metrics = run_pipeline(&config, &factory)?;
```
The number of Parquet Shredding writer threads is configurable from the CLI. The
remaining available cores are then used for data generation. For the `Contact`
struct, a 1:1 distribution of data generator and writer threads produces the
best performance.

[Arrow and Parquet]: https://docs.rs/parquet/55.0.0/parquet/

[Zipfian-like]: https://en.wikipedia.org/wiki/Zipf%27s_law

[skew.rs]: https://github.com/jcsherin/datablok/blob/4c790d0f672aea00870b68b5a25d25b592e60879/crates/parquet-nested-parallel/src/skew.rs

[datagen.rs]: https://github.com/jcsherin/datablok/blob/4c790d0f672aea00870b68b5a25d25b592e60879/crates/parquet-nested-parallel/src/datagen.rs#L146-L217

[pipeline.rs]: https://github.com/jcsherin/datablok/blob/4c790d0f672aea00870b68b5a25d25b592e60879/crates/parquet-nested-parallel/src/pipeline.rs#L280-L392

[blog post]: https://jacobsherin.com/posts/2025-09-01-arrow-shredding-pipeline-perf/

