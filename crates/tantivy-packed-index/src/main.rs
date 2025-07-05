use std::path::PathBuf;

const MAGIC_NUMBER: u32 = u32::from_le_bytes(*b"PIX1");

#[allow(dead_code)]
#[derive(Debug)]
struct PackedIndexHeader {
    magic_number: u32,
    version: u16,
    size: u64,
    files: Vec<FileMetadata>,
}

#[allow(dead_code)]
#[derive(Debug)]
struct FileMetadata {
    path: PathBuf,
    offset: u64,
    length: u64,
}

fn main() {
    let file1 = FileMetadata {
        path: PathBuf::from("meta.json"),
        offset: 0,
        length: 256,
    };
    let file2 = FileMetadata {
        path: PathBuf::from("a.idx"),
        offset: 256,
        length: 1024,
    };
    let file3 = FileMetadata {
        path: PathBuf::from("postings.info"),
        offset: 256 + 1024,
        length: 4096,
    };

    let header = PackedIndexHeader {
        magic_number: MAGIC_NUMBER, // "PKIX" as defined in the Canvas
        version: 1,
        size: 0,
        files: vec![file1, file2, file3],
    };

    println!("{header:#?}");
}
