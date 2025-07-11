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
enum FileMetadataError {
    #[error(
        "Malformed header detected. Header size: {bytes_len} is insufficient to hold metadata."
    )]
    MalformedHeader { bytes_len: usize },

    #[error(
        "Path len in header is {expected} bytes, but stored path truncated after {actual} bytes"
    )]
    TruncatedPath { expected: usize, actual: usize },

    #[error("Parsing FileMetadata from bytes failed")]
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

    pub fn new(path: PathBuf, data_size: u64) -> Self {
        let path_len = path.as_os_str().as_bytes().len() as u8;

        Self {
            path,
            path_len,
            data_offset: 0,
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
}

const FILE_METADATA_HEADER_SIZE: usize = FileMetadata::header_size();

impl From<FileMetadata> for Vec<u8> {
    fn from(value: FileMetadata) -> Self {
        value.to_bytes(value.path.as_os_str().as_bytes())
    }
}

impl TryFrom<Vec<u8>> for FileMetadata {
    type Error = FileMetadataError;

    fn try_from(bytes: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        if bytes.len() < FILE_METADATA_HEADER_SIZE {
            return Err(FileMetadataError::MalformedHeader {
                bytes_len: bytes.len(),
            });
        }

        let mut cursor = std::io::Cursor::new(bytes);

        let mut u64_buffer = [0u8; 8];
        cursor.read_exact(&mut u64_buffer)?;
        let data_offset = u64::from_le_bytes(u64_buffer);

        cursor.read_exact(&mut u64_buffer)?;
        let data_size = u64::from_le_bytes(u64_buffer);

        let mut path_len_buffer = [0u8; 1];
        cursor.read_exact(&mut path_len_buffer)?;
        let path_len = u8::from_le_bytes(path_len_buffer);

        let remaining_len = cursor.get_ref().len() - (cursor.position() as usize);
        if remaining_len < path_len.into() {
            return Err(FileMetadataError::TruncatedPath {
                expected: path_len.into(),
                actual: remaining_len,
            });
        }

        let mut path_buffer = vec![0u8; path_len as usize];
        cursor.read_exact(&mut path_buffer)?;
        let path = PathBuf::from(OsStr::from_bytes(&path_buffer));

        Ok(Self {
            data_offset,
            data_size,
            path_len,
            path,
        })
    }
}

#[derive(Debug)]
struct Header {
    version: u8,
    file_count: u32,
    _file_metadata_size: u32,
    _file_metadata_crc32: u32,
    file_metadata_list: Vec<FileMetadata>,
}
const MAGIC_BYTES: &[u8; 4] = b"FTEP"; // Full-Text index Embedded in Parquet
const VERSION: u8 = 1;

impl Default for Header {
    fn default() -> Self {
        Self {
            version: VERSION,
            file_count: 0,
            _file_metadata_size: 0,
            _file_metadata_crc32: 0,
            file_metadata_list: Vec::new(),
        }
    }
}

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

    pub fn build(self) -> Header {
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
            file_metadata_bytes.extend::<Vec<u8>>(file_metadata.into());
        }

        let file_metadata_crc32 = crc32fast::hash(file_metadata_bytes.as_slice());

        bytes.extend(file_metadata_bytes.len().to_le_bytes());
        bytes.extend(file_metadata_crc32.to_le_bytes());
        bytes.extend(file_metadata_bytes);

        bytes
    }
}

#[derive(Error, Debug)]
pub enum HeaderError {}

impl TryFrom<Vec<u8>> for HeaderBuilder {
    type Error = HeaderError;

    fn try_from(_value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        todo!()
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

        let file_metadata = FileMetadata::new(path.clone(), data_size as u64);
        header_builder = header_builder.with_file_metadata(&file_metadata);

        info!(
            "Path size={0}, size in bytes={data_size}, path={path:?}",
            path.as_os_str().len()
        );

        let bytes: Vec<u8> = file_metadata.into();
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

    info!("header: {:?}", Into::<Vec<u8>>::into(header));
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

#[cfg(test)]
mod tests {
    use crate::FileMetadata;
    use std::os::unix::ffi::OsStrExt;
    use std::path::PathBuf;

    #[test]
    fn file_metadata_roundtrip_bytes() {
        let path = PathBuf::from("abf90497f35b4d37a4f7843aa5780be2.fieldnorm");
        let file_metadata = FileMetadata::new(path, 42);

        let bytes: Vec<u8> = file_metadata.clone().into();
        let round_tripped: FileMetadata = bytes.try_into().unwrap();

        assert_eq!(round_tripped, file_metadata);
    }

    #[test]
    fn file_metadata_incomplete_header_error() {
        let bytes: Vec<u8> = vec![1, 2, 3, 4, 5];
        let header_size = bytes.len();

        let result: Result<FileMetadata, _> = bytes.try_into();

        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "Malformed header detected. Header size: {header_size} is insufficient to hold metadata.",
            )
        );
    }

    #[test]
    fn file_metadata_truncated_data_error() {
        // Path length is 42 bytes but there is only 8 bytes of path data
        let data_offset_bytes = u64::to_le_bytes(200);
        let data_size_bytes = u64::to_le_bytes(1024);

        let path_length = 42;
        let path_length_bytes = u8::to_le_bytes(path_length);

        let path = PathBuf::from("meta.json");
        let path_bytes = path.as_os_str().as_bytes();

        let mut bytes: Vec<u8> = Vec::new();

        bytes.extend(data_offset_bytes);
        bytes.extend(data_size_bytes);
        bytes.extend(path_length_bytes);
        bytes.extend(path_bytes);

        let result: Result<FileMetadata, _> = bytes.try_into();

        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "Path len in header is {path_length} bytes, but stored path truncated after {} bytes",
                path_bytes.len()
            )
        );
    }
}
