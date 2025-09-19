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

### 4. Zero Results are Extremely Fast

The `EmptyExec` optimization speeds up queries that find no matches in the
full-text index (when other filtering predicates are not present) by 2X to 70X,
depending on the search terms.

```text
┌──────────┬──────────┬──────────┬──────────┬──────┬─────────────┬─────────────┐
│ Query ID │ Baseline │ With FTS │     Diff │ Rows │ Selectivity │ Perf Change │
├──────────┼──────────┼──────────┼──────────┼──────┼─────────────┼─────────────┤
│       35 │  56.89ms │ 816.00µs │ -56.08ms │    0 │     0.0000% │ 69.72X      │
│       28 │  62.21ms │   2.04ms │ -60.17ms │    0 │     0.0000% │ 30.45X      │
│       21 │  56.02ms │   3.50ms │ -52.52ms │    0 │     0.0000% │ 15.99X      │
│       14 │  62.48ms │  16.53ms │ -45.95ms │    0 │     0.0000% │ 3.78X       │
│        7 │  61.40ms │  30.77ms │ -30.63ms │    0 │     0.0000% │ 2.00X       │
└──────────┴──────────┴──────────┴──────────┴──────┴─────────────┴─────────────┘
Slow Queries: 0 of 5
Path: output/docs_with_fts_index_10000000.parquet
Parquet Row Count: 10000000
```

The variability comes from the time it takes Tantivy the determine that search
term has no matches.

When examining trace for slowest in this group which is query 7, we can see that
querying the Tantivy full-text index takes ~30ms to complete. This is ~100% of
the time for executing query 7.

For example, Query 35 (~70X speedup) is fast because the index search completes
in 669µs.

```text
TRACE query_comparison:run:execute_sql:scan:extract_title_like_pattern: parquet_embed_tantivy::index: close time.busy=167ns time.idle=125ns query_id=35 sql=SELECT * FROM t WHERE title LIKE '%idempotency idempotency%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:create_index_query: parquet_embed_tantivy::index: close time.busy=1.42µs time.idle=166ns query_id=35 sql=SELECT * FROM t WHERE title LIKE '%idempotency idempotency%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:search_index:search_execute_hits_and_count: parquet_embed_tantivy::index: close time.busy=478µs time.idle=167ns query_id=35 sql=SELECT * FROM t WHERE title LIKE '%idempotency idempotency%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:search_index:resolve_hits_to_id_values: parquet_embed_tantivy::index: close time.busy=333ns time.idle=126ns query_id=35 sql=SELECT * FROM t WHERE title LIKE '%idempotency idempotency%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:search_index: parquet_embed_tantivy::index: close time.busy=669µs time.idle=124ns query_id=35 sql=SELECT * FROM t WHERE title LIKE '%idempotency idempotency%' run_type="optimized"
```
On the other hand, Query 7 (2X speedup) takes ~30ms, which accounts for nearly
100% of the total query time.

```text
TRACE query_comparison:run:execute_sql:scan:extract_title_like_pattern: parquet_embed_tantivy::index: close time.busy=166ns time.idle=334ns query_id=7 sql=SELECT * FROM t WHERE title LIKE '%runtime runtime%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:create_index_query: parquet_embed_tantivy::index: close time.busy=18.8µs time.idle=167ns query_id=7 sql=SELECT * FROM t WHERE title LIKE '%runtime runtime%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:search_index:search_execute_hits_and_count: parquet_embed_tantivy::index: close time.busy=30.5ms time.idle=250ns query_id=7 sql=SELECT * FROM t WHERE title LIKE '%runtime runtime%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:search_index:resolve_hits_to_id_values: parquet_embed_tantivy::index: close time.busy=208ns time.idle=251ns query_id=7 sql=SELECT * FROM t WHERE title LIKE '%runtime runtime%' run_type="optimized"
TRACE query_comparison:run:execute_sql:scan:query_result_ids:search_index: parquet_embed_tantivy::index: close time.busy=30.8ms time.idle=167ns query_id=7 sql=SELECT * FROM t WHERE title LIKE '%runtime runtime%' run_type="optimized"
```

## Summary

```text
┌──────────┬──────────┬──────────┬───────────┬───────┬─────────────┬──────────────────┐
│ Query ID │ Baseline │ With FTS │      Diff │  Rows │ Selectivity │ Perf Change      │
├──────────┼──────────┼──────────┼───────────┼───────┼─────────────┼──────────────────┤
│       35 │  58.34ms │ 722.75µs │  -57.62ms │     0 │     0.0000% │ 80.72X           │
│       28 │  61.37ms │   2.02ms │  -59.35ms │     0 │     0.0000% │ 30.36X           │
│       34 │  59.82ms │   2.98ms │  -56.84ms │     3 │     0.0000% │ 20.10X           │
│       29 │  61.41ms │   3.85ms │  -57.56ms │     9 │     0.0001% │ 15.97X           │
│       21 │  54.10ms │   3.60ms │  -50.50ms │     0 │     0.0000% │ 15.03X           │
│       33 │  63.92ms │   5.18ms │  -58.74ms │    12 │     0.0001% │ 12.34X           │
│       23 │  63.74ms │   5.65ms │  -58.09ms │    13 │     0.0001% │ 11.29X           │
│       14 │  62.00ms │  16.84ms │  -45.16ms │     0 │     0.0000% │ 3.68X            │
│        7 │  62.92ms │  32.59ms │  -30.33ms │     0 │     0.0000% │ 1.93X            │
│       32 │  65.38ms │  38.23ms │  -27.15ms │    63 │     0.0006% │ 1.71X            │
│       17 │  65.05ms │  39.85ms │  -25.21ms │    65 │     0.0007% │ 1.63X            │
│       11 │  61.52ms │  39.25ms │  -22.27ms │   123 │     0.0012% │ 1.57X            │
│       27 │  62.14ms │  40.76ms │  -21.38ms │    75 │     0.0008% │ 1.52X            │
│       31 │  63.06ms │  41.42ms │  -21.64ms │   158 │     0.0016% │ 1.52X            │
│       22 │  57.60ms │  37.94ms │  -19.66ms │    58 │     0.0006% │ 1.52X            │
│       16 │  63.09ms │  45.57ms │  -17.52ms │   357 │     0.0036% │ 1.38X            │
│       26 │  64.67ms │  49.99ms │  -14.69ms │   341 │     0.0034% │ 1.29X            │
│       25 │  64.89ms │  52.44ms │  -12.45ms │   682 │     0.0068% │ 1.24X            │
│       15 │  62.73ms │  51.79ms │  -10.94ms │   681 │     0.0068% │ 1.21X            │
│        5 │  64.35ms │  53.86ms │  -10.49ms │   394 │     0.0039% │ 1.19X            │
│        4 │  62.83ms │  53.77ms │   -9.06ms │  1987 │     0.0199% │ 1.17X            │
│       20 │  65.56ms │  56.36ms │   -9.20ms │   712 │     0.0071% │ 1.16X            │
│        9 │  65.68ms │  56.87ms │   -8.82ms │  1331 │     0.0133% │ 1.16X            │
│       24 │  62.58ms │  55.07ms │   -7.52ms │  1921 │     0.0192% │ 1.14X            │
│       10 │  62.54ms │  56.31ms │   -6.24ms │   750 │     0.0075% │ 1.11X            │
│        3 │  61.86ms │  59.44ms │   -2.42ms │  3920 │     0.0392% │ 1.04X            │
│       18 │  63.50ms │  62.10ms │   -1.40ms │  3918 │     0.0392% │ 1.02X            │
│       30 │  56.31ms │  56.06ms │ -243.50µs │   376 │     0.0038% │ 1.00X            │
│       19 │  56.68ms │  58.00ms │   +1.32ms │  1380 │     0.0138% │ 1.02X (slowdown) │
│       13 │  61.27ms │  92.50ms │  +31.23ms │  6768 │     0.0677% │ 1.51X (slowdown) │
│        8 │  61.96ms │  97.34ms │  +35.38ms │  6908 │     0.0691% │ 1.57X (slowdown) │
│       12 │  63.85ms │ 101.63ms │  +37.79ms │ 19564 │     0.1956% │ 1.59X (slowdown) │
│        2 │  62.16ms │ 101.88ms │  +39.72ms │ 19385 │     0.1938% │ 1.64X (slowdown) │
│        1 │  63.10ms │ 149.69ms │  +86.59ms │ 38758 │     0.3876% │ 2.37X (slowdown) │
│        6 │  62.86ms │ 149.31ms │  +86.45ms │ 38772 │     0.3877% │ 2.38X (slowdown) │
│        0 │  63.36ms │ 230.10ms │ +166.74ms │ 52060 │     0.5206% │ 3.63X (slowdown) │
└──────────┴──────────┴──────────┴───────────┴───────┴─────────────┴──────────────────┘
Slow Queries: 8 of 36
Path: output/docs_with_fts_index_10000000.parquet
Parquet Row Count: 10000000
```

[Embedding User-Defined Indexes in Apache Parquet Files]: https://datafusion.apache.org/blog/2025/07/14/user-defined-parquet-indexes/

[Tantivy full-text search index]: https://github.com/quickwit-oss/tantivy

[DataFusion TableProvider]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html#table-provider-and-scan

[Tantivy Query]: https://docs.rs/tantivy/latest/tantivy/query/trait.Query.html

[Sargable (Search ARGument ABLE)]: https://en.wikipedia.org/wiki/Sargable