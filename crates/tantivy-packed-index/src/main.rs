use crate::packed_index::{FileMetadata, Header};
use std::path::PathBuf;

mod packed_index {
    use std::path::PathBuf;

    pub const MAGIC_NUMBER: u32 = u32::from_le_bytes(*b"PIX1");

    #[allow(dead_code)]
    #[derive(Debug)]
    pub struct Header {
        magic_number: u32,
        version: u16,
        size: u64,
        files: Vec<FileMetadata>,
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    pub struct FileMetadata {
        path: PathBuf,
        offset: u64,
        length: u64,
    }

    impl Default for Header {
        fn default() -> Self {
            Self {
                magic_number: MAGIC_NUMBER,
                version: 1,
                size: 0,
                files: Vec::new(),
            }
        }
    }

    impl Header {
        pub fn add_file_metadata(&mut self, file_metadata: FileMetadata) -> &mut Self {
            self.files.push(file_metadata);
            self
        }
    }

    impl FileMetadata {
        pub fn new(path: PathBuf, offset: u64, length: u64) -> Self {
            Self {
                path,
                offset,
                length,
            }
        }
    }
}

fn main() {
    let file1 = FileMetadata::new(PathBuf::from("meta.json"), 0, 256);
    let file2 = FileMetadata::new(PathBuf::from("abcd.idx"), 256, 1024);
    let file3 = FileMetadata::new(PathBuf::from("postings.info"), 256 + 1024, 4096);

    let mut header = Header::default();

    header
        .add_file_metadata(file1)
        .add_file_metadata(file2)
        .add_file_metadata(file3);

    println!("{header:#?}");
}
