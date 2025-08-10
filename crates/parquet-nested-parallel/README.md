This program demonstrates a parallel, high-throughput pipeline for shredding
nested data structures and writing them to multiple Parquet files concurrently.
It implements multiple partitions of multi-producer, single consumer pattern.
Each partition contains multiple producer threads that generate millions of
nested data and then streams them to a dedicated writer thread which writes to
a Parquet file.

The data is generated with a [Zipfian-like] skew, rather than a uniform
distribution. This better reflects real-world data, where distributions are
rarely uniform and typically feature a long-tail of less frequent values.

[Zipfian-like]: https://en.wikipedia.org/wiki/Zipf%27s_law

- [ ] Add a section on how performance improved 10x through a series of
  data-driven performance improvements.

### How to Run

Run this from the repo root:

```zsh
RUST_LOG=info cargo run -p parquet-nested-parallel --release
```

### Development CI

For running the same workflow as GitHub CI in local development environment:

```zsh
./scripts/verify.sh parquet-nested-parallel --verbose
```

### Expected Output

```text
[2025-08-08T10:08:15Z INFO  parquet_parallel_nested] Generating 10M contacts across 16 cores. Chunk size: 256.
[2025-08-08T10:08:15Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 3M contacts.
[2025-08-08T10:08:15Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 2M contacts.
[2025-08-08T10:08:15Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 3M contacts.
[2025-08-08T10:08:15Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 2M contacts.
[2025-08-08T10:08:15Z INFO  parquet_parallel_nested] Total generation and write time: 448.616305ms.
[2025-08-08T10:08:15Z INFO  parquet_parallel_nested] Record Throughput: 22M records/sec
[2025-08-08T10:08:15Z INFO  parquet_parallel_nested] In-Memory Throughput: 1.14 GB/s
```

### Performance

#### Perf Stats

```text
$ perf stat -e cycles,instructions,cache-references,cache-misses,branch-instructions,branch-misses ./target/release/parquet-parallel-nested
...
 Performance counter stats for './target/release/parquet-parallel-nested':

    14,585,115,667      cycles                                                                  (83.43%)
    30,262,087,965      instructions                     #    2.07  insn per cycle              (83.47%)
       347,781,116      cache-references                                                        (83.43%)
        41,686,650      cache-misses                     #   11.99% of all cache refs           (83.17%)
     5,409,693,348      branch-instructions                                                     (83.15%)
       131,463,682      branch-misses                    #    2.43% of all branches             (83.44%)

       0.572045514 seconds time elapsed

       2.960127000 seconds user
       0.456030000 seconds sys 
```

#### Hyperfine Stats

```text
$ ~/.cargo/bin/hyperfine --warmup 1 --runs 10 \                                                                                                              
                         --prepare 'sync; echo 3 | sudo tee /proc/sys/vm/drop_caches' \
                         './target/release/parquet-parallel-nested'

Benchmark 1: ./target/release/parquet-parallel-nested
  Time (mean ± σ):     536.5 ms ±   4.6 ms    [User: 2919.1 ms, System: 379.1 ms]
  Range (min … max):   526.4 ms … 542.3 ms    10 runs
```

[//]: # (### Design Notes)

[//]: # ()

[//]: # (* The data generation for)

[//]: # (  `Contact` struct uses a preset distribution defined for each of its fields.)

[//]: # (    * Use)

[//]: # (      `proptest` as the abstract shaper which knows the probability distribution of each)

[//]: # (      `Contact` field defined)

[//]: # (      as a `BoxedStrategy`.)

[//]: # (    * The abstract &#40;template-like&#41; shape makes it possible to configure)

[//]: # (      `Contact.name` to have realistic looking)

[//]: # (      values by using the)

[//]: # (      `fake` package, instead of generating random string values.)

[//]: # (    * Each)

[//]: # (      `Contact.phone_number` value is globally unique. This is implemented using a global)

[//]: # (      `std::sync::atomic::AtomicUsize` counter which is sequentially incremented. This was chosen to balance between)

[//]: # (      realistic enough numbers and not having to coordinate using an external data structure like a HashMap or bloom)

[//]: # (      filters.)

[//]: # (* Single-threaded execution on a single core is easy to write and requires less code. But it immediately runs into)

[//]: # (  bottleneck if we need to generate >1M nested data structure values.)

[//]: # (* Embarrassingly parallel data generation using `rayon` parallel iterator.)

[//]: # (    * A single dedicated parquet writer &#40;consumer&#41; thread which reads)

[//]: # (      `RecordBatch` from channel and write to file)

[//]: # (      storage.)

[//]: # (    * Many producers with a pipeline like: `PartialContact` _chunk_ -> `Contact`)

[//]: # (      _chunk_ -> `RecordBatch`.)

[//]: # (* Performance:)

[//]: # (    * Changing)

[//]: # (      `BASE_CHUNK_SIZE` from 8192 -> 256 makes the execution ~2x faster.)

[//]: # (* Decisions:)

[//]: # (    * Single-threaded vec materialization works upto 100K values. The next step is to generate small chunks and)

[//]: # (      immediately write to disk to avoid OOM crashes.)

[//]: # (    * There is a seam which separates the abstract shaping from the concrete &#40;assembly&#41; materialization of the value.)

[//]: # (        * The)

[//]: # (          `Contacts.phones.phone_number` is globally unique. The abstract shape contains the cardinality)

[//]: # (          distribution of)

[//]: # (          `Contact.phones` field, but the actual number which is used is only known at runtime tracked)

[//]: # (          by a global)

[//]: # (          `AtomicUsize` counter which is incremented by 1. Requires no coordination or locking.)

[//]: # (    * Switching to `tikv-jemallocator` had a negative impact of performance.)

[//]: # ()

[//]: # (### 1 Billion Nested Data Structures)

[//]: # ()

[//]: # (Linear scaling in performance from 10 million through 1 billion is the effect of fixed costs being amortized over longer)

[//]: # (runs. The current version fuses data generation with)

[//]: # (`RecordBatch` building. The producer thread control flow is)

[//]: # (simple. There is no extra code required for handling trailing rows which are less than the optimal)

[//]: # (`RecordBatch`)

[//]: # (size.)

[//]: # ()

[//]: # (```text)

[//]: # (➜  rusty-doodles git:&#40;main&#41; ✗ perf stat -e cycles,instructions,cache-references,cache-misses,branch-instructions,branch-misses ./target/release/parquet-parallel-nested                  dbu6)

[//]: # ([2025-07-03T18:22:25Z INFO  parquet_parallel_nested] Generating 1G contacts across 16 cores. Chunk size: 256.)

[//]: # ([2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 250M contacts.)

[//]: # ([2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 250M contacts.)

[//]: # ([2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 250M contacts.)

[//]: # ([2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 250M contacts.)

[//]: # ([2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Total generation and write time: 45.0708714s.)

[//]: # ([2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Record Throughput: 22M records/sec)

[//]: # ([2025-07-03T18:23:11Z INFO  parquet_parallel_nested] In-Memory Throughput: 1.13 GB/s)

[//]: # ()

[//]: # ( Performance counter stats for './target/release/parquet-parallel-nested':)

[//]: # ()

[//]: # ( 1,380,116,176,317      cycles                                                                  &#40;83.34%&#41;)

[//]: # ( 2,854,183,038,000      instructions                     #    2.07  insn per cycle              &#40;83.32%&#41;)

[//]: # (    36,862,140,938      cache-references                                                        &#40;83.36%&#41;)

[//]: # (     3,787,813,422      cache-misses                     #   10.28% of all cache refs           &#40;83.32%&#41;)

[//]: # (   527,304,886,022      branch-instructions                                                     &#40;83.30%&#41;)

[//]: # (    11,521,613,424      branch-misses                    #    2.19% of all branches             &#40;83.36%&#41;)

[//]: # ()

[//]: # (      45.079519917 seconds time elapsed)

[//]: # ()

[//]: # (     300.597543000 seconds user)

[//]: # (      28.455504000 seconds sys)

[//]: # (```)

[//]: # ()

[//]: # (The size of the parquet files which contains 750 million contacts each created by 4 writer threads.)

[//]: # ()

[//]: # (```text)

[//]: # (➜  rusty-doodles git:&#40;main&#41; ✗ ls contacts_*                                                                                                                                              dbu6)

[//]: # (contacts_1.parquet  contacts_2.parquet  contacts_3.parquet  contacts_4.parquet)

[//]: # (➜  rusty-doodles git:&#40;main&#41; ✗ ls contacts_* -lth                                                                                                                                         dbu6)

[//]: # (-rw-rw-r-- 1 jcsherin jcsherin 7.5G Jul  3 18:23 contacts_2.parquet)

[//]: # (-rw-rw-r-- 1 jcsherin jcsherin 7.5G Jul  3 18:23 contacts_3.parquet)

[//]: # (-rw-rw-r-- 1 jcsherin jcsherin 7.5G Jul  3 18:23 contacts_1.parquet)

[//]: # (-rw-rw-r-- 1 jcsherin jcsherin 7.5G Jul  3 18:23 contacts_4.parquet)

[//]: # (```)

[//]: # ()

[//]: # (### Performance Analysis: String Generation Strategies)

[//]: # ()

[//]: # (The)

[//]: # (`generate_name` method generates names with a high degree of duplication, where only ~25% of name are unique.)

[//]: # (The goal is to reduce the millions of small, short-lived heap allocations by comparing the baseline &#40;no interning&#41;,)

[//]: # (against two caching strategies: a global shared &#40;)

[//]: # (`DashMap`&#41; string interner and a thread-local string interner.)

[//]: # ()

[//]: # (Counter-intuitively, both interning strategies led to significant performance regression compared to the baseline)

[//]: # (version.)

[//]: # ()

[//]: # (The)

[//]: # (`DashMap` version has cache-coherency issues due to multiple thread writers on shared data structure. The)

[//]: # (thread-local version overcomes this problem, but the overhead it introduces in hashing,)

[//]: # (`RefCell` borrow, branching)

[//]: # (etc. proved to be more expensive than simply making more string allocations.)

[//]: # ()

[//]: # (The result is a lesson in what appeared to be an optimization target on paper &#40;a hot loop with many small)

[//]: # (allocations&#41;, in practice is already optimal.)

[//]: # ()

[//]: # (#### Raw Data: Performance Comparison &#40;10M Run Size&#41;)

[//]: # ()

[//]: # (| Metric                           | Baseline        | Shared `DashMap` Interner | Thread-Local Interner |)

[//]: # (|:---------------------------------|:----------------|:--------------------------|:----------------------|)

[//]: # (| **Wall Time &#40;Elapsed&#41;**          | **`0.440 s`**   | `0.955 s` &#40;+117%&#41;         | `0.793 s` &#40;+80%&#41;      |)

[//]: # (| **Record Throughput**            | **`23 M/s`**    | `11 M/s` &#40;-52%&#41;           | `14 M/s` &#40;-39%&#41;       |)

[//]: # (| **In-Memory Throughput**         | **`1.18 GB/s`** | `0.55 GB/s` &#40;-53%&#41;        | `0.72 GB/s` &#40;-39%&#41;    |)

[//]: # (|                                  |                 |                           |                       |)

[//]: # (| **Instructions Per Cycle &#40;IPC&#41;** | **`2.13`**      | `1.10` &#40;-48%&#41;             | `1.11` &#40;-48%&#41;         |)

[//]: # (| **Cache Miss Rate**              | **`11.39%`**    | `22.49%` &#40;+97%&#41;           | `15.91%` &#40;+40%&#41;       |)

[//]: # (| **Branch Miss Rate**             | **`2.16%`**     | `2.75%` &#40;+27%&#41;            | `3.03%` &#40;+40%&#41;        |)

[//]: # (|                                  |                 |                           |                       |)

[//]: # (| **Total CPU Time &#40;User + Sys&#41;**  | `3.155 s`       | `7.188 s` &#40;+128%&#41;         | `9.068 s` &#40;+187%&#41;     |)

[//]: # (| **User Time**                    | `2.834 s`       | `6.373 s`                 | `7.480 s`             |)

[//]: # (| **System Time**                  | `0.320 s`       | `0.814 s`                 | `1.588 s`             |)

[//]: # (|                                  |                 |                           |                       |)

[//]: # (| **Cycles**                       | `13.4 B`        | `30.9 B` &#40;+130%&#41;          | `38.4 B` &#40;+186%&#41;      |)

[//]: # (| **Instructions**                 | `28.6 B`        | `33.8 B` &#40;+18%&#41;           | `42.7 B` &#40;+49%&#41;       |)

[//]: # (| **Cache References**             | `336 M`         | `649 M` &#40;+93%&#41;            | `772 M` &#40;+130%&#41;       |)

[//]: # (| **Cache Misses**                 | `38.3 M`        | `146.0 M` &#40;+281%&#41;         | `122.9 M` &#40;+221%&#41;     |)

[//]: # (| **Branch Instructions**          | `5.31 B`        | `6.09 B` &#40;+15%&#41;           | `7.50 B` &#40;+41%&#41;       |)

[//]: # (| **Branch Misses**                | `115 M`         | `167 M` &#40;+45%&#41;            | `227 M` &#40;+97%&#41;        |)

[//]: # ()
