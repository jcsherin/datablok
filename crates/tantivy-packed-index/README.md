### Packed Inverted Index

Pack the inverted index created by Tantivy into a byte array. The abstract model of the inverted index is a directory
which contains files.

The [Trait Directory](https://docs.rs/tantivy/latest/tantivy/directory/trait.Directory.html) can be implemented to
represent the inverted index as a byte array.

The byte array requires a file format specification for serializing to disk. The files in the inverted index will be
stored as metadata, and the contents of the files can be stored as raw data. The metadata can is read into memory
for querying the data. The raw data needs to be loaded into memory as needed. If necessary buffer pool management
can be used to restrict memory usage, and not have to worry about the entire inverted index fitting into memory.

Why not use [RamDirectory](https://docs.rs/tantivy/latest/tantivy/directory/struct.RamDirectory.html)?

It is ephemeral and does not serialize to disk. The goal is to build write once, read forever binary blob to storage
which can also be loaded into memory for full-text querying.

The non-goal is to implement updates/deletes to the binary blob after index is created etc. This is write once, read
_forever_.

#### TODO

- [ ] Define a file format (magic bytes, header, metadata, raw data)
- [ ] Implement `Directory` trait for `PackedInvertedIndex`
    - [ ] Be able to create the index
    - [ ] Be able to read/query the index (keyword / prefix / wildcard)
- [ ] Use [ImHex](https://github.com/WerWolv/ImHex) for inspecting storage format
