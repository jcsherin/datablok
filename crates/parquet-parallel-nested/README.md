High-performance parallel data generation with custom data skew from a concrete Rust nested data structures and
finally written to a Parquet file. The generated `contacts.parquet` file is ~300MB.

### How to Run

From the root of the monorepo, you can run this experiment using the package name:

```zsh
RUST_LOG=info cargo run -p parquet-parallel-nested
```

To check the code formatting, linting, and other issues, you can use the verification script:

```zsh
./scripts/verify.sh parquet-parallel-nested
```

### Expected Output

_TODO_

### Schema

```rust
#[derive(Debug, Clone, PartialEq)]
enum PhoneType {
    Mobile,
    Home,
    Work,
}

struct Phone {
    number: Option<String>,
    phone_type: Option<PhoneType>,
}

struct Contact {
    name: Option<String>,
    phones: Option<Vec<Phone>>,
}
```

### Data Distribution

The data skew of `phone_type` and `phone_number` fields are [Zipfian-like](https://en.wikipedia.org/wiki/Zipf%27s_law).

The value distribution in real-world datasets are not uniform. Rather it has a long-tail and few items account for
most of the occurrences.

| Field                 | Distribution                                                           | Description                                               | 
|-----------------------|------------------------------------------------------------------------|-----------------------------------------------------------|
| `name`                | 20% `NULL` <br/> 80% unique names                                      | Significant number of contacts are missing a name.        |
| `phones.phone_type`   | 55% Mobile <br/> 35% Work <br/> 10% Home                               | Skewed distribution is more realistic than a uniform one. |
| `phones.phone_number` | 40% zero phones <br/> 45% 1 phone <br/> 10% 2 phone <br/> 5% 3+ phones | Skewed towards most records having only 1 phone or none.  | 

| `phone_number` | `phone_type` | Probability |
|----------------|--------------|-------------|
| Some(_)        | Some(_)      | 90%         |
| Some(_)        | None         | 5%          |
| None           | Some(_)      | 4%          |
| None           | None         | 1%          |

### Cardinality

| Field                 | Cardinality | Description                                                     |
|-----------------------|-------------|-----------------------------------------------------------------|
| `name`                | High        | Names are mostly unique.                                        | 
| `phones.phone_type`   | Low         | It has only three possible values: `["Home", "Work", "Mobile"]` | 
| `phones.phone_number` | High        | Phone numbers are unique.                                       |

### Nullability

| Field                 | Nullablle |
|-----------------------|-----------|
| `name`                | true      |
| `phones.phone_type`   | true      |
| `phones.phone_number` | true      |

### Representing Optional Fields in JSON

In Rust, the absence of a value is represented by `Option::None`. When serialized to JSON, this maps to an explicit
`null` value, ensuring the key is always present.

_Q. Why not omit the `null` in the JSON representation?_

The reason to omit `null` value keys is that it will reduce the storage size. But we will follow existing practice
of being explicit. It removes ambiguity regarding the which keys are present in a structure and the semantics of the
value. For readers can identify a valid property has not value at present. For writers can identify if the key is to
be zeroed out if it has an existing value. But if it were absent then the writer has to guess the intent.

__Rust Struct__:

```rust
Contact { name: None, phones: None, }
```

__Resulting JSON__:

```json
{
  "name": null,
  "phones": null
}
```

### Design Notes

* The data generation for `Contact` struct uses a preset distribution defined for each of its fields.
    * Use `proptest` as the abstract shaper which knows the probability distribution of each `Contact` field defined
      as a `BoxedStrategy`.
    * The abstract (template-like) shape makes it possible to configure `Contact.name` to have realistic looking
      values by using the `fake` package, instead of generating random string values.
    * Each `Contact.phone_number` value is globally unique. This is implemented using a global
      `std::sync::atomic::AtomicUsize` counter which is sequentially incremented. This was chosen to balance between
      realistic enough numbers and not having to coordinate using an external data structure like a HashMap or bloom
      filters.
* Single-threaded execution on a single core is easy to write and requires less code. But it immediately runs into
  bottleneck if we need to generate >1M nested data structure values.
* Embarrassingly parallel data generation using `rayon` parallel iterator.
    * A single dedicated parquet writer (consumer) thread which reads `RecordBatch` from channel and write to file
      storage.
    * Many producers with a pipeline like: `PartialContact` _chunk_ -> `Contact` _chunk_ -> `RecordBatch`.
* Performance:
    * Changing `BASE_CHUNK_SIZE` from 8192 -> 256 makes the execution ~2x faster.
* Decisions:
    * Single-threaded vec materialization works upto 100K values. The next step is to generate small chunks and
      immediately write to disk to avoid OOM crashes.
    * There is a seam which separates the abstract shaping from the concrete (assembly) materialization of the value.
        * The `Contacts.phones.phone_number` is globally unique. The abstract shape contains the cardinality
          distribution of `Contact.phones` field, but the actual number which is used is only known at runtime tracked
          by a global `AtomicUsize` counter which is incremented by 1. Requires no coordination or locking.
    * Switching to `tikv-jemallocator` had a negative impact of performance.

### 1 Billion Nested Data Structures

Linear scaling in performance from 10 million through 1 billion is the effect of fixed costs being amortized over longer
runs. The current version fuses data generation with `RecordBatch` building. The producer thread control flow is
simple. There is no extra code required for handling trailing rows which are less than the optimal `RecordBatch`
size.

```text
➜  rusty-doodles git:(main) ✗ perf stat -e cycles,instructions,cache-references,cache-misses,branch-instructions,branch-misses ./target/release/parquet-parallel-nested                  dbu6
[2025-07-03T18:22:25Z INFO  parquet_parallel_nested] Generating 1G contacts across 16 cores. Chunk size: 256.
[2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 250M contacts.
[2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 250M contacts.
[2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 250M contacts.
[2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Finished writing parquet file. Wrote 250M contacts.
[2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Total generation and write time: 45.0708714s.
[2025-07-03T18:23:11Z INFO  parquet_parallel_nested] Record Throughput: 22M records/sec
[2025-07-03T18:23:11Z INFO  parquet_parallel_nested] In-Memory Throughput: 1.13 GB/s

 Performance counter stats for './target/release/parquet-parallel-nested':

 1,380,116,176,317      cycles                                                                  (83.34%)
 2,854,183,038,000      instructions                     #    2.07  insn per cycle              (83.32%)
    36,862,140,938      cache-references                                                        (83.36%)
     3,787,813,422      cache-misses                     #   10.28% of all cache refs           (83.32%)
   527,304,886,022      branch-instructions                                                     (83.30%)
    11,521,613,424      branch-misses                    #    2.19% of all branches             (83.36%)

      45.079519917 seconds time elapsed

     300.597543000 seconds user
      28.455504000 seconds sys
```

The size of the parquet files which contains 750 million contacts each created by 4 writer threads.

```text
➜  rusty-doodles git:(main) ✗ ls contacts_*                                                                                                                                              dbu6
contacts_1.parquet  contacts_2.parquet  contacts_3.parquet  contacts_4.parquet
➜  rusty-doodles git:(main) ✗ ls contacts_* -lth                                                                                                                                         dbu6
-rw-rw-r-- 1 jcsherin jcsherin 7.5G Jul  3 18:23 contacts_2.parquet
-rw-rw-r-- 1 jcsherin jcsherin 7.5G Jul  3 18:23 contacts_3.parquet
-rw-rw-r-- 1 jcsherin jcsherin 7.5G Jul  3 18:23 contacts_1.parquet
-rw-rw-r-- 1 jcsherin jcsherin 7.5G Jul  3 18:23 contacts_4.parquet
```

### Performance Analysis: String Generation Strategies

The `generate_name` method generates names with a high degree of duplication, where only ~25% of name are unique.
The goal is to reduce the millions of small, short-lived heap allocations by comparing the baseline (no interning),
against two caching strategies: a global shared (`DashMap`) string interner and a thread-local string interner.

Counter-intuitively, both interning strategies led to significant performance regression compared to the baseline
version.

The `DashMap` version has cache-coherency issues due to multiple thread writers on shared data structure. The
thread-local version overcomes this problem, but the overhead it introduces in hashing, `RefCell` borrow, branching
etc. proved to be more expensive than simply making more string allocations.

The result is a lesson in what appeared to be an optimization target on paper (a hot loop with many small
allocations), in practice is already optimal.

#### Raw Data: Performance Comparison (10M Run Size)

| Metric                           | Baseline        | Shared `DashMap` Interner | Thread-Local Interner |
|:---------------------------------|:----------------|:--------------------------|:----------------------|
| **Wall Time (Elapsed)**          | **`0.440 s`**   | `0.955 s` (+117%)         | `0.793 s` (+80%)      |
| **Record Throughput**            | **`23 M/s`**    | `11 M/s` (-52%)           | `14 M/s` (-39%)       |
| **In-Memory Throughput**         | **`1.18 GB/s`** | `0.55 GB/s` (-53%)        | `0.72 GB/s` (-39%)    |
|                                  |                 |                           |                       |
| **Instructions Per Cycle (IPC)** | **`2.13`**      | `1.10` (-48%)             | `1.11` (-48%)         |
| **Cache Miss Rate**              | **`11.39%`**    | `22.49%` (+97%)           | `15.91%` (+40%)       |
| **Branch Miss Rate**             | **`2.16%`**     | `2.75%` (+27%)            | `3.03%` (+40%)        |
|                                  |                 |                           |                       |
| **Total CPU Time (User + Sys)**  | `3.155 s`       | `7.188 s` (+128%)         | `9.068 s` (+187%)     |
| **User Time**                    | `2.834 s`       | `6.373 s`                 | `7.480 s`             |
| **System Time**                  | `0.320 s`       | `0.814 s`                 | `1.588 s`             |
|                                  |                 |                           |                       |
| **Cycles**                       | `13.4 B`        | `30.9 B` (+130%)          | `38.4 B` (+186%)      |
| **Instructions**                 | `28.6 B`        | `33.8 B` (+18%)           | `42.7 B` (+49%)       |
| **Cache References**             | `336 M`         | `649 M` (+93%)            | `772 M` (+130%)       |
| **Cache Misses**                 | `38.3 M`        | `146.0 M` (+281%)         | `122.9 M` (+221%)     |
| **Branch Instructions**          | `5.31 B`        | `6.09 B` (+15%)           | `7.50 B` (+41%)       |
| **Branch Misses**                | `115 M`         | `167 M` (+45%)            | `227 M` (+97%)        |

