### Byte Array Index

The Tantivy library can create a full-text search index that can be stored
in a custom byte array (`Vec<u8>`) format. This byte array is then embedded
within a Parquet file as a secondary index, placed between the data pages
and the metadata. This approach ensures backwards compatibility, as standard
Parquet readers ignore the unrecognized data. Apache DataFusion, however,
can be extended to leverage this embedded index to accelerate text-based
`LIKE` queries.

### Sketch

The following sketch outlines the multi-stage pipeline used to implement the
byte array index described above.

The process begins by building the index in memory using [RamDirectory].
While `RamDirectory` contains all the necessary data, the metadata required to
assemble it into a single-byte array is private to its implementation.

To overcome this, we use the `persist` method provided by `RamDirectory` to copy
its contents into `TmpDirectory`, an intermediate representation that
exposes this crucial file metadata. This enables a final, single-pass
transformation that assembles the contents into a `BlobDirectory`.

The `BlobDirectory` uses a compact, archive-like format. It begins with a 4-byte
magic number, followed by a header containing the file metadata. The contents of
each index file are written sequentially after the header. The format concludes
by repeating the magic number, which allows for simple integrity checks to
detect incomplete writes.

[RamDirectory]: https://docs.rs/tantivy/latest/tantivy/directory/struct.RamDirectory.html

