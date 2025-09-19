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

## Leading Wildcard Prevents Predicate Pushdown

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

The steps involved in optimizing the query are:

1. Extract the pattern `dairy cow` from the predicate:
   `title LIKE '%dairy cow%'`.
2. Tokenize: `[dairy, cow]` for creating a Tantivy full-text index query.
3. Search the Tantivy full-text index for matches.
4. Resolve the matches into a list of `id` column values which are sargable.
5. Rewrite `title LIKE '%dairy cow%` into `id IN (...)` format, which can now
   use the min/max statistics on the
   `id` column to prune row groups and data pages
   avoiding a full-table scan of the Parquet data source.

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

The physical plan now contains the rewritten predicate, where the values are
matching IDs for `dairy cow` found in the full-text index. This new sargable
predicate can utilize the min/max statistics on the `id` column to prune row
groups and data pages, avoiding a full table scan.

### Short-Circuiting Zero Matches

```sql
SELECT id,
       title
FROM t
WHERE title LIKE '%cow cow%'
```

If the full text index yields no matches, and since the query contains no other
predicates there will be zero rows in the final result. This case is optimized
as a no-op, and does not attempt to read the Parquet data source.

```text
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

In the test cases which return zero rows, this results in a speedup in the range
of 2X to 70X. The variability arises from the time it takes to complete a full-text
index search with no matches returned for different queries.

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

When building the index, we include both the `title` (text) column and the
`id` (integer) column in the Tantivy document.

```rust
let mut schema_builder = SchemaBuilder::new();

schema_builder.add_u64_field("id", INDEXED | STORED);
schema_builder.add_text_field("title", TEXT);

schema_builder.build();
```

Typically, the original string values are discarded after indexing the terms.
When querying the index, and matches are returned, we then use the `id` column
to retrieve the complete matching text values stored in the Parquet file.

### Transformed Physical Plan

The steps involved in optimizing the query are:

1. Extract the search pattern from the wildcard `LIKE` predicate.
2. Tokenize the pattern to construct a Tantivy search query.
3. Search the full-text index.
4. From the search results returned by Tantivy, retrieve the stored `id` values.
5. Create a new predicate:
   `id IN (...)` which contains the values returned from the previous step.
6. Pushdown this predicate into the Parquet data source for skipping rows.

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

[DataFusion TableProvider]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html#table-provider-and-scan

[Tantivy Query]: https://docs.rs/tantivy/latest/tantivy/query/trait.Query.html

[Sargable (Search ARGument ABLE)]: https://en.wikipedia.org/wiki/Sargable