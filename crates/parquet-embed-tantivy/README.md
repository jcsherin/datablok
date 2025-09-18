# Embedding a Tantivy Index In Parquet

> Parquet tolerates unknown bytes within the file body and permits arbitrary key/value pairs in its footer metadata. These two features enable embedding user-defined indexes directly in the file—no extra files, no format forks, and no compatibility breakage.
>
> From DataFusion blog post: [Embedding User-Defined Indexes in Apache Parquet Files]

This demo extends Parquet with a [Tantivy full-text search index]. A custom
[TableProvider] implementation uses the embedded full-text index to optimize
wildcard `LIKE` predicates in a query. For example:

```sql
SELECT *
FROM t
WHERE title LIKE '%dairy cow%'
```

In the above query, the predicate: `title LIKE '%dairy cow%'` does not help
predicate pushdown to skip any rows. The leading and trailing wildcards means
a substring match can exist anywhere inside a string value. Therefore, a full
table scan is required to filter matching rows.

In this case the baseline physical plan looks like this:

```text
+---------------+-------------------------------+
| plan_type     | plan                          |
+---------------+-------------------------------+
| physical_plan | ┌───────────────────────────┐ |
|               | │    CoalesceBatchesExec    │ |
|               | │    --------------------   │ |
|               | │     target_batch_size:    │ |
|               | │            8192           │ |
|               | └─────────────┬─────────────┘ |
|               | ┌─────────────┴─────────────┐ |
|               | │         FilterExec        │ |
|               | │    --------------------   │ |
|               | │         predicate:        │ |
|               | │   title LIKE %dairy cow%  │ |
|               | └─────────────┬─────────────┘ |
|               | ┌─────────────┴─────────────┐ |
|               | │       DataSourceExec      │ |
|               | │    --------------------   │ |
|               | │         files: 12         │ |
|               | │      format: parquet      │ |
|               | │                           │ |
|               | │         predicate:        │ |
|               | │   title LIKE %dairy cow%  │ |
|               | └───────────────────────────┘ |
|               |                               |
+---------------+-------------------------------+
```

## Use the Index Results to Rewrite the Physical Plan

During query execution we use the embedded full-text index to find matches. This
is done by extracting the pattern string from the wildcard `LIKE` predicate and
then rewriting it into a [Tantivy Query].

For example,
`title LIKE '%dairy cow%'` is transformed into the Tantivy search query:

```rust
PhraseQuery::new(vec![
    Term::from_field_text(title_field, "dairy"),
    Term::from_field_text(title_field, "cow"),
])
```

The results from Tantivy are resolved into a set of integer `id` column values.
The original predicate: `title LIKE '%dairy cow%'` can now be transformed into
a predicate pushdown friendly predicate: `id IN (...)` which will filter the
same rows in the Parquet file.

This predicate: `id IN (...)` can be now be used while scanning the Parquet file
to skip data pages and row groups significantly reducing both decoding compute
and I/O.

### Building the Full-Text Index


We extend DataFusion with a custom [TableProvider] which rewrites the search
pattern in the LIKE `predicate` into a [Tantivy Query]. The index hits resolve
to a set of `id` column values, which are used to create a new physical
predicate: `id IN (...)` which is pushdown to the Parquet source.

The optimized physical plan looks like this:

```text
+---------------+-------------------------------+
| plan_type     | plan                          |
+---------------+-------------------------------+
| physical_plan | ┌───────────────────────────┐ |
|               | │       DataSourceExec      │ |
|               | │    --------------------   │ |
|               | │         files: 12         │ |
|               | │      format: parquet      │ |
|               | │                           │ |
|               | │         predicate:        │ |
|               | │  id IN (1979290, 4565514, │ |
|               | │          9794628)         │ |
|               | └───────────────────────────┘ |
|               |                               |
+---------------+-------------------------------+
```

## Results

The geometric mean of speedup across 36 queries used for testing is 1.68X.

```text
┌──────────┬───────┬─────────────┬──────────┬──────────┬───────────┬──────────────────┐
│ Query ID │  Rows │ Selectivity │ Baseline │ With FTS │      Diff │ Perf Change      │
├──────────┼───────┼─────────────┼──────────┼──────────┼───────────┼──────────────────┤
│       35 │     0 │     0.0000% │  61.23ms │ 803.46µs │  -60.43ms │ 76.21X           │
│       28 │     0 │     0.0000% │  63.40ms │   2.10ms │  -61.30ms │ 30.15X           │
│       34 │     3 │     0.0000% │  63.57ms │   3.44ms │  -60.13ms │ 18.48X           │
│       21 │     0 │     0.0000% │  58.60ms │   3.71ms │  -54.90ms │ 15.81X           │
│       29 │     9 │     0.0001% │  63.13ms │   4.31ms │  -58.82ms │ 14.65X           │
│       33 │    12 │     0.0001% │  68.19ms │   5.93ms │  -62.26ms │ 11.51X           │
│       23 │    13 │     0.0001% │  64.73ms │   5.70ms │  -59.03ms │ 11.36X           │
│       14 │     0 │     0.0000% │  64.18ms │  16.71ms │  -47.47ms │ 3.84X            │
│        7 │     0 │     0.0000% │  66.47ms │  32.18ms │  -34.28ms │ 2.07X            │
│       32 │    63 │     0.0006% │  66.34ms │  39.74ms │  -26.59ms │ 1.67X            │
│       11 │   123 │     0.0012% │  67.32ms │  41.93ms │  -25.39ms │ 1.61X            │
│       27 │    75 │     0.0008% │  65.97ms │  42.02ms │  -23.96ms │ 1.57X            │
│       17 │    65 │     0.0007% │  64.28ms │  41.10ms │  -23.18ms │ 1.56X            │
│       22 │    58 │     0.0006% │  59.15ms │  40.12ms │  -19.03ms │ 1.47X            │
│       31 │   158 │     0.0016% │  62.04ms │  47.94ms │  -14.10ms │ 1.29X            │
│       26 │   341 │     0.0034% │  63.55ms │  51.32ms │  -12.23ms │ 1.24X            │
│       16 │   357 │     0.0036% │  63.36ms │  51.18ms │  -12.17ms │ 1.24X            │
│       10 │   750 │     0.0075% │  67.28ms │  54.39ms │  -12.88ms │ 1.24X            │
│       25 │   682 │     0.0068% │  65.33ms │  53.27ms │  -12.06ms │ 1.23X            │
│        9 │  1331 │     0.0133% │  68.28ms │  58.66ms │   -9.62ms │ 1.16X            │
│        4 │  1987 │     0.0199% │  67.83ms │  58.42ms │   -9.41ms │ 1.16X            │
│       15 │   681 │     0.0068% │  64.17ms │  56.17ms │   -8.00ms │ 1.14X            │
│       24 │  1921 │     0.0192% │  65.13ms │  57.15ms │   -7.99ms │ 1.14X            │
│       30 │   376 │     0.0038% │  59.38ms │  52.87ms │   -6.51ms │ 1.12X            │
│        5 │   394 │     0.0039% │  65.34ms │  59.37ms │   -5.97ms │ 1.10X            │
│       19 │  1380 │     0.0138% │  64.51ms │  60.83ms │   -3.68ms │ 1.06X            │
│       18 │  3918 │     0.0392% │  65.47ms │  63.73ms │   -1.74ms │ 1.03X            │
│       20 │   712 │     0.0071% │  63.98ms │  63.10ms │ -888.04µs │ 1.01X            │
│        3 │  3920 │     0.0392% │  64.67ms │  64.49ms │ -182.29µs │ 1.00X            │
│       13 │  6768 │     0.0677% │  65.22ms │  94.61ms │  +29.39ms │ 1.45X (slowdown) │
│        8 │  6908 │     0.0691% │  63.37ms │  98.75ms │  +35.38ms │ 1.56X (slowdown) │
│       12 │ 19564 │     0.1956% │  67.76ms │ 106.02ms │  +38.26ms │ 1.56X (slowdown) │
│        2 │ 19385 │     0.1938% │  66.37ms │ 111.01ms │  +44.65ms │ 1.67X (slowdown) │
│        1 │ 38758 │     0.3876% │  65.21ms │ 156.84ms │  +91.63ms │ 2.41X (slowdown) │
│        6 │ 38772 │     0.3877% │  64.44ms │ 155.73ms │  +91.29ms │ 2.42X (slowdown) │
│        0 │ 52060 │     0.5206% │  63.26ms │ 245.70ms │ +182.44ms │ 3.88X (slowdown) │
└──────────┴───────┴─────────────┴──────────┴──────────┴───────────┴──────────────────┘
```

[Embedding User-Defined Indexes in Apache Parquet Files]: https://datafusion.apache.org/blog/2025/07/14/user-defined-parquet-indexes/

[Tantivy full-text search index]: https://github.com/quickwit-oss/tantivy

[TableProvider]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html#table-provider-and-scan

[Tantivy Query]: https://docs.rs/tantivy/latest/tantivy/query/trait.Query.html

### Byte Array Index

In Parquet format it is possible to extend it by writing arbitrary bytes between
the data pages and the metadata. This is an advanced implementation where we
embed a full-text search index inside Parquet, and use Apache DataFusion to
accelerate `LIKE` queries on the indexed text columns.

### Sketch

### Phase 1

While the Parquet file is being created, in parallel create the full-text index
using the [Tantivy] library. The challenge here is to convert the index data
into a byte array which can be embedded in Parquet.

[Tantivy]: https://github.com/quickwit-oss/tantivy

After the index is committed, we will first write the directory manifest to
the beginning of the byte array. Then copy the contents from each file and
append it to this byte array. With a single-pass we will have a read-only
full-text index represented as a byte array which is now ready to be written
to the Parquet file. The

The offset of this byte array can be stored in the file
metadata, [key_value_metadata] field. This is backwards compatible as existing
Parquet readers will skip the byte array which is not a data page. It will not
affect existing query performance.

[key_value_metadata]: https://github.com/apache/parquet-format/blob/819adce0ec6aa848e56c56f20b9347f4ab50857f/src/main/thrift/parquet.thrift#L1266-L1267

We will then implement the [Directory trait] which knows how to read the bytes
embedded in the Parquet file. We can check that the byte array index works
by using this trait implementation to run Tantivy search queries.

[Directory trait]: https://docs.rs/tantivy/0.24.1/tantivy/directory/trait.Directory.html

#### Phase 2

_TODO_

### How to Run

From the root of the monorepo, you can run this experiment using its package
name. The `RUST_LOG` variable is used to
control the log output.

```zsh
RUST_LOG=info cargo run -p parquet-embed-tantivy
```

To check the code for formatting, linting, and other issues, you can use the
verification script:

```zsh
./scripts/verify.sh parquet-embed-tantivy
```

### Expected Output

```text
// @formatter:off
[<timestamp> INFO  tantivy::indexer::segment_updater] save metas
[<timestamp> INFO  tantivy::indexer::index_writer] Preparing commit
[<timestamp> INFO  tantivy::indexer::index_writer] Prepared commit 8
[<timestamp> INFO  tantivy::indexer::prepared_commit] committing 8
[<timestamp> INFO  tantivy::indexer::segment_updater] save metas
[<timestamp> INFO  tantivy::indexer::segment_updater] Running garbage collection
[<timestamp> INFO  tantivy::directory::managed_directory] Garbage collect
[<timestamp> INFO  tantivy_byte_array_index] Matches count: 1
[<timestamp> INFO  tantivy_byte_array_index] Matched Original Doc [ID=1]: Doc { id: 1, title: "The Diary of Muadib", body: None }
// @formatter:on
```
