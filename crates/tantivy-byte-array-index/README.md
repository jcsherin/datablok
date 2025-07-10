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
RUST_LOG=info cargo run -p tantivy-byte-array-index
```

To check the code for formatting, linting, and other issues, you can use the
verification script:

```zsh
./scripts/verify.sh tantivy-byte-array-index
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
