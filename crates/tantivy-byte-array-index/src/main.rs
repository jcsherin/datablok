mod common;
mod doc;
mod error;
mod index;
mod query;
mod query_session;

use crate::common::{Config, SchemaFields};
use crate::doc::{DocIdMapper, DocMapper, DocSchema, examples};
use crate::error::Result;
use crate::index::IndexBuilder;
use crate::query_session::QuerySession;
use log::info;
use query::boolean_query;
use std::ffi::OsStr;
use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::{Directory, HasLen};
use thiserror::Error;

#[derive(Error, Debug)]
enum IndexFormatError {
    #[error("Malformed header detected. Header size: {actual} is insufficient to hold metadata.")]
    TruncatedHeader { actual: usize },

    #[error("Error parsing header from bytes.")]
    ParseError(#[from] std::io::Error),
}

#[derive(Debug, Default, PartialEq, Clone)]
struct FileMetadata {
    /// Offset in data section from where the file contents start
    data_offset: u64,
    /// Length of the file contents slice stored in data section in bytes
    data_size: u64,
    /// Length of the file path in bytes
    path_len: u8,
    /// File path
    path: PathBuf,
}

impl FileMetadata {
    const DATA_OFFSET_BYTES: u8 = 8;
    const DATA_SIZE_BYTES: u8 = 8;
    const PATH_LEN_BYTES: u8 = 1;

    const fn header_size() -> usize {
        (Self::DATA_OFFSET_BYTES + Self::DATA_SIZE_BYTES + Self::PATH_LEN_BYTES) as usize
    }

    pub fn new(path: PathBuf, data_size: u64, data_offset: u64) -> Self {
        let path_len = path.as_os_str().as_bytes().len() as u8;

        Self {
            path,
            path_len,
            data_offset,
            data_size,
        }
    }

    fn to_bytes(&self, path: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::header_size() + path.len());

        bytes.extend(self.data_offset.to_le_bytes());
        bytes.extend(self.data_size.to_le_bytes());
        bytes.extend(self.path_len.to_le_bytes());
        bytes.extend(path);

        bytes
    }

    fn from_cursor(
        cursor: &mut std::io::Cursor<Vec<u8>>,
    ) -> std::result::Result<Self, IndexFormatError> {
        let mut u64_buffer = [0u8; 8];
        cursor.read_exact(&mut u64_buffer)?;
        let data_offset = u64::from_le_bytes(u64_buffer);

        cursor.read_exact(&mut u64_buffer)?;
        let data_size = u64::from_le_bytes(u64_buffer);

        let mut path_len_buffer = [0u8; 1];
        cursor.read_exact(&mut path_len_buffer)?;
        let path_len = u8::from_le_bytes(path_len_buffer);

        let mut path_buffer = vec![0u8; path_len as usize];
        cursor.read_exact(&mut path_buffer)?;
        let path = PathBuf::from(OsStr::from_bytes(&path_buffer));

        Ok(FileMetadata::new(path, data_size, data_offset))
    }
}

impl From<&FileMetadata> for Vec<u8> {
    fn from(value: &FileMetadata) -> Self {
        value.to_bytes(value.path.as_os_str().as_bytes())
    }
}

#[derive(Debug, PartialEq, Clone)]
struct Header {
    version: u8,
    file_count: u32,
    file_metadata_size: u32,
    file_metadata_crc32: u32,
    file_metadata_list: Vec<FileMetadata>,
}
const MAGIC_BYTES: &[u8; 4] = b"FTEP"; // Full-Text index Embedded in Parquet
const VERSION: u8 = 1;

impl Default for Header {
    fn default() -> Self {
        Self {
            version: VERSION,
            file_count: 0,
            file_metadata_size: 0,
            file_metadata_crc32: 0,
            file_metadata_list: Vec::new(),
        }
    }
}

impl Header {
    const MAGIC_BYTES_LEN: u8 = 4;
    const VERSION_LEN: u8 = 1;
    const FILE_COUNT_LEN: u8 = 4;
    const FILE_METADATA_SIZE_LEN: u8 = 4;
    const FILE_METADATA_CRC32_LEN: u8 = 4;

    const fn header_size() -> usize {
        (Self::MAGIC_BYTES_LEN
            + Self::VERSION_LEN
            + Self::FILE_COUNT_LEN
            + Self::FILE_METADATA_SIZE_LEN
            + Self::FILE_METADATA_CRC32_LEN) as usize
    }
}

const HEADER_SIZE: usize = Header::header_size();

struct HeaderBuilder {
    inner: Header,
}

impl HeaderBuilder {
    pub fn new() -> Self {
        Self {
            inner: Header::default(),
        }
    }

    pub fn with_file_metadata(mut self, file_metadata: &FileMetadata) -> HeaderBuilder {
        self.inner.file_count += 1;
        self.inner.file_metadata_list.push(file_metadata.clone());
        self
    }

    pub fn with_version(mut self, version: u8) -> HeaderBuilder {
        self.inner.version = version;
        self
    }

    pub fn with_file_metadata_size(mut self, size: u32) -> HeaderBuilder {
        self.inner.file_metadata_size = size;
        self
    }

    pub fn with_file_metadata_crc32(mut self, crc32: u32) -> HeaderBuilder {
        self.inner.file_metadata_crc32 = crc32;
        self
    }

    pub fn build(mut self) -> Header {
        let total_metadata_size: u64 = self
            .inner
            .file_metadata_list
            .iter()
            .map(|fm| FileMetadata::header_size() + fm.path.as_os_str().len())
            .sum::<usize>() as u64;
        self.inner.file_metadata_size = total_metadata_size as u32; // back-fill

        let data_block_offset = HEADER_SIZE as u64 + total_metadata_size;

        let mut current_offset = data_block_offset;
        for file_metadata in self.inner.file_metadata_list.iter_mut() {
            file_metadata.data_offset = current_offset; // back-fill

            current_offset += file_metadata.data_size;
        }

        self.inner
    }
}

impl From<Header> for Vec<u8> {
    fn from(value: Header) -> Self {
        let mut bytes = Vec::new();

        bytes.extend(MAGIC_BYTES);
        bytes.push(VERSION);
        bytes.extend(value.file_count.to_le_bytes());

        let mut file_metadata_bytes = Vec::new();
        for file_metadata in value.file_metadata_list {
            file_metadata_bytes.extend::<Vec<u8>>((&file_metadata).into());
        }

        let file_metadata_size = file_metadata_bytes.len() as u32;
        let file_metadata_crc32 = crc32fast::hash(file_metadata_bytes.as_slice());

        bytes.extend(file_metadata_size.to_le_bytes());
        bytes.extend(file_metadata_crc32.to_le_bytes());
        bytes.extend(file_metadata_bytes);

        bytes
    }
}

impl TryFrom<Vec<u8>> for Header {
    type Error = IndexFormatError;

    fn try_from(bytes: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        if bytes.len() < HEADER_SIZE {
            return Err(IndexFormatError::TruncatedHeader {
                actual: bytes.len(),
            });
        }

        let mut cursor = std::io::Cursor::new(bytes);

        let mut header_builder = HeaderBuilder::new();

        let mut u32_buffer = [0u8; 4];
        cursor.read_exact(&mut u32_buffer)?; // magic bytes
        debug_assert!(u32_buffer.eq(MAGIC_BYTES));

        let mut u8_buffer = [0u8; 1];
        cursor.read_exact(&mut u8_buffer)?;
        debug_assert_eq!(u8_buffer[0], VERSION);
        header_builder = header_builder.with_version(u8_buffer[0]);

        let mut file_count_buffer = [0u8; 4];
        cursor.read_exact(&mut file_count_buffer)?;
        let file_count = u32::from_le_bytes(file_count_buffer);

        let mut file_metadata_size_buffer = [0u8; 4];
        cursor.read_exact(&mut file_metadata_size_buffer)?;
        let file_metadata_size = u32::from_le_bytes(file_metadata_size_buffer);
        header_builder = header_builder.with_file_metadata_size(file_metadata_size);

        let mut file_metadata_crc32_buffer = [0u8; 4];
        cursor.read_exact(&mut file_metadata_crc32_buffer)?;
        let file_metadata_crc32 = u32::from_le_bytes(file_metadata_crc32_buffer);
        header_builder = header_builder.with_file_metadata_crc32(file_metadata_crc32);

        let mut file_metadata_list_buffer = vec![0u8; file_metadata_size as usize];
        cursor.read_exact(&mut file_metadata_list_buffer)?;

        let crc32 = crc32fast::hash(&file_metadata_list_buffer);
        debug_assert!(crc32 == file_metadata_crc32);

        // Cursor that wraps the entire file metadata block
        let mut cursor = std::io::Cursor::new(file_metadata_list_buffer);
        for _ in 0..file_count {
            let file_metadata = FileMetadata::from_cursor(&mut cursor)?;
            header_builder = header_builder.with_file_metadata(&file_metadata);
        }

        Ok(header_builder.build())
    }
}

fn main() -> Result<()> {
    setup_logging();

    let config = Config::default();
    let schema = DocSchema::new(&config).into_schema();
    let original_docs = examples();

    let fields = SchemaFields::new(&schema, &config)?;

    let index = IndexBuilder::new(schema)
        .index_and_commit(
            config.index_writer_memory_budget_in_bytes,
            &fields,
            original_docs,
        )?
        .build();

    let metadata_file = PathBuf::from("meta.json");
    let dir = index.directory();

    let mut file_count: u32 = 0;
    let mut total_bytes: u32 = 0;

    let mut header_builder = HeaderBuilder::new();
    for path in dir.list_managed_files() {
        let data_size = if path.eq(&metadata_file) {
            let contents = dir
                .atomic_read(&path)
                .unwrap_or_else(|e| panic!("Error: {e} while reading metadata file: {path:?}"));

            contents.len()
        } else {
            let file_slice = dir
                .open_read(&path)
                .unwrap_or_else(|e| panic!("Error: {e} while opening file: {path:?}"));

            file_slice.len()
        };

        let file_metadata = FileMetadata::new(path.clone(), data_size as u64, 0);
        header_builder = header_builder.with_file_metadata(&file_metadata);

        info!(
            "Path size={0}, size in bytes={data_size}, path={path:?}",
            path.as_os_str().len()
        );

        let bytes: Vec<u8> = (&file_metadata).into();
        total_bytes += bytes.len() as u32;
        file_count += 1;
        info!(
            "file count:{file_count} current total bytes: {total_bytes} in encoded form {:?}",
            u32::to_le_bytes(total_bytes)
        );
    }

    let header = header_builder.build();

    info!(
        "file count: {file_count} ({:?}) Total bytes: {total_bytes} in encoded form {:?}",
        u32::to_le_bytes(file_count),
        u32::to_le_bytes(total_bytes)
    );

    let header_bytes: Vec<u8> = header.clone().into();
    // info!("header: {header_bytes:?}");

    let roundtripped_header: Header = header_bytes.try_into().unwrap();
    info!("roundtripped_header: {roundtripped_header:#?}");

    debug_assert_eq!(header.version, roundtripped_header.version);
    debug_assert_eq!(header.file_count, roundtripped_header.file_count);
    assert_eq!(
        header.file_metadata_size,
        roundtripped_header.file_metadata_size
    );
    // assert_eq!(header.file_metadata_crc32, roundtripped_header.file_metadata_crc32);
    for (left, right) in header
        .file_metadata_list
        .iter()
        .zip(roundtripped_header.file_metadata_list.iter())
    {
        debug_assert_eq!(left, right)
    }

    info!("magic: {MAGIC_BYTES:?}");

    let query_session = QuerySession::new(&index)?;
    let doc_mapper = DocMapper::new(query_session.searcher(), &config, original_docs);

    let query = boolean_query::title_contains_diary_and_not_girl(&query_session.schema())?;

    info!("Matches count: {}", query_session.search(&query, &Count)?);

    let results = query_session.search(&query, &DocSetCollector)?;
    for doc_address in results {
        let Ok(Some(doc_id)) = doc_mapper.get_doc_id(doc_address) else {
            info!("Failed to get doc id from doc address: {doc_address:?}");
            continue;
        };

        if let Some(doc) = doc_mapper.get_original_doc(doc_id) {
            info!("Matched Doc [ID={doc_id:?}]: {doc:?}")
        } else {
            info!("Failed to reverse map id: {doc_id:?} to a document")
        }
    }

    Ok(())
}

/// Initializes the logger.
fn setup_logging() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
}
