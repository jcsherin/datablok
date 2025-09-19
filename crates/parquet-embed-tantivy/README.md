# Embedding a Tantivy Index In Parquet

> Parquet tolerates unknown bytes within the file body and permits arbitrary key/value pairs in its footer metadata. These two features enable embedding user-defined indexes directly in the file—no extra files, no format forks, and no compatibility breakage.
>
> From DataFusion blog post: [Embedding User-Defined Indexes in Apache Parquet Files]

This demo extends a Parquet file by embedding a [Tantivy full-text search index]
inside it. A custom [DataFusion TableProvider] implementation uses the
embedded full-text index to optimize wildcard `LIKE` predicates.

For example:

```sql
SELECT id,
       title
FROM t
WHERE title LIKE '%dairy cow%'
```

## Leading Wildcard Forces a Full-Table Scan

The predicate
`title LIKE '%dairy cow%'` is not [Sargable (Search ARGument ABLE)],
because it contains a leading wildcard (%). A leading wildcard means the match
can start anywhere. Therefore, it cannot use the column min/max statistics to
prune row groups. This prevents predicate pushdown, forcing a full table scan
where a substring match has to be performed for every row.

While leading wildcard forces a full table scan, the `%dairy cow%` pattern with
both leading and trailing wildcard represents the worst-case scenario. It
requires a more computationally expensive check to see if a match exists anywhere
within the string, for every single row.

Here is the physical plan for this query:

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

We can see in the above physical plan that even though the predicate is pushdown
to the Parquet data source, a `FilterExec` with the exact same predicate sits
above it to guarantee non-matching rows are filtered out.

## Query Optimization using Full-Text Index

The core of the optimization is using the full-text index search results to
rewrite the original predicate into a highly efficient sargable one.

### 1. Transforming LIKE Predicate into a Tantivy Query

First, the pattern is extracted from the original predicate. For example:
`title LIKE '%dairy cow%'`, is converted into a list of search terms:
`[dairy ,cow]`. This list is then use to create a [Tantivy Query].

```rust
PhraseQuery::new(vec![
    Term::from_field_text(title_field, "dairy"),
    Term::from_field_text(title_field, "cow"),
])
```

### 2. Creating a Sargable Predicate

The search results from Tantivy are a collection of matching documents. We
retrieve the stored `id` values from these documents and use them to rewrite the
original predicate `title LIKE '%dairy cow%'` into a sargable
`id IN (...)` predicate.

This new predicate can now be pushed down into the Parquet data source, which
uses the min/max statistics on the `id` column to prune row groups and data
pages, avoiding a full table scan.

The optimized plan now looks like this:

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

### Special Case: Short-Circuiting Zero Matches

When no matches are found in the full-text index, and if the query contains no
other filters, the final result will be empty. This case is optimized as a no-op,
and returns early without ever scanning the Parquet data source.

```text
> SELECT id, title
    FROM t
   WHERE title LIKE '%cow cow%';

+----+-------+
| id | title |
+----+-------+
+----+-------+
0 row(s) fetched.
```

In this case the optimized plan looks like this:

```text
+---------------+-------------------------------+
| plan_type     | plan                          |
+---------------+-------------------------------+
| physical_plan | ┌───────────────────────────┐ |
|               | │         EmptyExec         │ |
|               | └───────────────────────────┘ |
|               |                               |
+---------------+-------------------------------+
```

In the test cases which return zero rows, this results in a maximum speedup of
upto ~70X. The variability in speedup corresponds to the differences in the time
it takes for full-text search to complete for different search terms.

```text
┌──────────┬──────────┬──────────┬──────────┬──────┬─────────────┬─────────────┐
│ Query ID │ Baseline │ With FTS │     Diff │ Rows │ Selectivity │ Perf Change │
├──────────┼──────────┼──────────┼──────────┼──────┼─────────────┼─────────────┤
│       35 │  55.81ms │ 769.46µs │ -55.04ms │    0 │     0.0000% │ 72.53X      │
│       28 │  65.03ms │   2.06ms │ -62.98ms │    0 │     0.0000% │ 31.60X      │
│       21 │  56.71ms │   3.63ms │ -53.08ms │    0 │     0.0000% │ 15.61X      │
│       14 │  61.57ms │  16.56ms │ -45.01ms │    0 │     0.0000% │ 3.72X       │
│        7 │  63.89ms │  32.86ms │ -31.02ms │    0 │     0.0000% │ 1.94X       │
└──────────┴──────────┴──────────┴──────────┴──────┴─────────────┴─────────────┘
Slow Queries: 0 of 5
Path: output/docs_with_fts_index_10000000.parquet
Parquet Row count: 10000000
```

## Parquet File on Disk

The table schema used for generating the Parquet file is:

```text
+-------------+-----------+-------------+
| column_name | data_type | is_nullable |
+-------------+-----------+-------------+
| id          | UInt64    | NO          |
| title       | Utf8View  | YES         |
+-------------+-----------+-------------+
```

The documents indexed in Tantivy have an identical schema:

```rust
let mut schema_builder = SchemaBuilder::new();

schema_builder.add_u64_field("id", INDEXED | STORED);
schema_builder.add_text_field("title", TEXT);

schema_builder.build();
```

The Parquet file with the embedded Tantivy full-text index is consistently ~80%
larger than the parquet file without the index.

```text
// 10 million rows
515M    output/docs_10000000.parquet
926M    output/docs_with_fts_index_10000000.parquet

// 1 million rows
 52M    output/docs_1000000.parquet
 95M    output/docs_with_fts_index_1000000.parquet
```

## Performance

### 1. Full-Text Index Setup Cost

There is a one-time cost for reading the embedded full-text from the Parquet file
and initializing it for querying. It is ~130ms (~411 MB) for a tantivy index
containing, 10 million documents.

```text
TRACE open:read_directory:index_offset: parquet_embed_tantivy::index: close time.busy=15.7µs time.idle=750ns
TRACE open:read_directory:deserialize_header: parquet_embed_tantivy::index: close time.busy=116µs time.idle=417ns
TRACE open:read_directory:deserialize_data_block: parquet_embed_tantivy::index: close time.busy=127ms time.idle=624ns
TRACE open:read_directory: parquet_embed_tantivy::index: close time.busy=128ms time.idle=416ns
```

### 2. Low Selectivity Queries are a Bottleneck

If the full-text index matches a lot of rows, the performance bottleneck becomes
resolving the matching documents to a list of `id` values. A full-table scan has
a better, stable performance in this case.

```text
┌──────────┬──────────┬──────────┬───────────┬───────┬─────────────┬──────────────────┐
│ Query ID │ Baseline │ With FTS │      Diff │  Rows │ Selectivity │ Perf Change      │
├──────────┼──────────┼──────────┼───────────┼───────┼─────────────┼──────────────────┤
│       19 │  59.03ms │  55.96ms │   -3.07ms │  1380 │     0.0138% │ 1.05X            │
│        8 │  64.94ms │  96.76ms │  +31.82ms │  6908 │     0.0691% │ 1.49X (slowdown) │
│       13 │  62.94ms │  96.77ms │  +33.83ms │  6768 │     0.0677% │ 1.54X (slowdown) │
│        2 │  63.75ms │ 103.61ms │  +39.85ms │ 19385 │     0.1938% │ 1.63X (slowdown) │
│       12 │  64.41ms │ 106.60ms │  +42.19ms │ 19564 │     0.1956% │ 1.66X (slowdown) │
│        1 │  67.10ms │ 156.67ms │  +89.57ms │ 38758 │     0.3876% │ 2.34X (slowdown) │
│        6 │  65.36ms │ 156.17ms │  +90.81ms │ 38772 │     0.3877% │ 2.39X (slowdown) │
│        0 │  63.34ms │ 238.08ms │ +174.75ms │ 52060 │     0.5206% │ 3.76X (slowdown) │
└──────────┴──────────┴──────────┴───────────┴───────┴─────────────┴──────────────────┘
Slow Queries: 7 of 8
Path: output/docs_with_fts_index_10000000.parquet
Parquet Row Count: 10000000
```

In this trace for query 0 which returns 52060 rows, search the full-text completed
in ~84ms and resolving the search results into a list of
`id` values takes another
~60ms. The total time spend in full-text search is ~145ms, whereas a full-table
scan completes in ~60ms.

```text
TRACE query_comparison:run:execute_sql:scan:extract_title_like_pattern: parquet_embed_tantivy::index: close time.busy=166ns time.idle=417ns query_id=0 sql=SELECT * FROM t WHERE title LIKE '%concurrency concurrency%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:create_index_query: parquet_embed_tantivy::index: close time.busy=18.2µs time.idle=126ns query_id=0 sql=SELECT * FROM t WHERE title LIKE '%concurrency concurrency%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:search_index:search_execute_hits_and_count: parquet_embed_tantivy::index: close time.busy=83.6ms time.idle=83.0ns query_id=0 sql=SELECT * FROM t WHERE title LIKE '%concurrency concurrency%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:search_index:resolve_hits_to_id_values: parquet_embed_tantivy::index: close time.busy=59.5ms time.idle=208ns query_id=0 sql=SELECT * FROM t WHERE title LIKE '%concurrency concurrency%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:search_index: parquet_embed_tantivy::index: close time.busy=145ms time.idle=166ns query_id=0 sql=SELECT * FROM t WHERE title LIKE '%concurrency concurrency%' run_type="optimized"
```
### 3. Creating a Large IN Predicate is Fast

In DataFusion the time taken to create an `id IN (...)` predicate for 52K rows
and creating the physical plan completes in ~6ms.

```text
TRACE query_comparison:run:execute_sql:scan:ids_to_predicate: parquet_embed_tantivy::index: close time.busy=6.27ms time.idle=1.54µs query_id=0 sql=SELECT * FROM t WHERE title LIKE '%concurrency concurrency%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:create_optimized_physical_plan: parquet_embed_tantivy::index: close time.busy=114µs time.idle=459ns query_id=0 sql=SELECT * FROM t WHERE title LIKE '%concurrency concurrency%' run_type="optimized"
```

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

[DataFusion TableProvider]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html#table-provider-and-scan

[Tantivy Query]: https://docs.rs/tantivy/latest/tantivy/query/trait.Query.html

[Sargable (Search ARGument ABLE)]: https://en.wikipedia.org/wiki/Sargable