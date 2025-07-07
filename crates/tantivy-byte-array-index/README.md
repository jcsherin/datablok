### Byte Array Index

The Tantivy library can create a full-text search index that can be stored
in a custom byte array (`Vec<u8>`) format. This byte array is then embedded
within a Parquet file as a secondary index, placed between the data pages
and the metadata. This approach ensures backwards compatibility, as standard
Parquet readers ignore the unrecognized data. Apache DataFusion, however,
can be extended to leverage this embedded index to accelerate text-based
`LIKE` queries.