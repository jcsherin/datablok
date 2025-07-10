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
use std::array::TryFromSliceError;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::{Directory, HasLen};
use thiserror::Error;

#[derive(Debug, PartialEq, Clone)]
struct FileMetadata {
    path: PathBuf,
    file_size: u32,
}

impl FileMetadata {
    const HEADER_SIZE_IN_BYTES: u8 = 8;

    pub fn new(path: PathBuf, size_in_bytes: u32) -> Self {
        Self {
            path,
            file_size: size_in_bytes,
        }
    }

    fn to_bytes(&self, path_as_bytes: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + 4 + path_as_bytes.len());

        let path_len = path_as_bytes.len() as u32;

        bytes.extend(path_len.to_le_bytes());
        bytes.extend(self.file_size.to_le_bytes());
        bytes.extend(path_as_bytes);

        bytes
    }
}

impl From<FileMetadata> for Vec<u8> {
    fn from(value: FileMetadata) -> Self {
        let path_as_bytes = value.path.as_os_str().as_bytes();

        value.to_bytes(path_as_bytes)
    }
}

#[derive(Error, Debug)]
enum FileMetadataError {
    #[error("Header does not contain path length and data size.")]
    IncompleteHeader,

    #[error("Path name got truncated.")]
    PathNameTruncated,

    #[error("Failed to convert slice to array")]
    ConversionError(#[from] TryFromSliceError),
}

impl TryFrom<Vec<u8>> for FileMetadata {
    type Error = Box<dyn std::error::Error>;

    fn try_from(bytes: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        let input_size = bytes.len();

        if input_size < FileMetadata::HEADER_SIZE_IN_BYTES as usize {
            return Err(Box::new(FileMetadataError::IncompleteHeader));
        }

        let path_len_bytes: [u8; 4] = bytes[0..4].try_into()?;
        let path_len = u32::from_le_bytes(path_len_bytes) as usize;

        let file_size_bytes: [u8; 4] = bytes[4..8].try_into()?;
        let file_size = u32::from_le_bytes(file_size_bytes);

        if input_size < (FileMetadata::HEADER_SIZE_IN_BYTES as usize + path_len) {
            return Err(Box::new(FileMetadataError::PathNameTruncated));
        }

        let path_bytes = &bytes[8..(8 + path_len)];
        let os_str = OsStr::from_bytes(path_bytes);
        let path = PathBuf::from(os_str);

        Ok(Self { path, file_size })
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

    for path in dir.list_managed_files() {
        let size_in_bytes = if path.eq(&metadata_file) {
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

        let file_metadata = FileMetadata::new(path.clone(), size_in_bytes as u32);

        info!(
            "Path size={0}, size in bytes={size_in_bytes}, path={path:?}",
            path.as_os_str().len()
        );

        let bytes: Vec<u8> = file_metadata.into();
        info!("bytes: {bytes:?}");
    }

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

        let result: Result<FileMetadata, _> = bytes.try_into();

        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Header does not contain path length and data size."
        );
    }

    #[test]
    fn file_metadata_truncated_data_error() {
        // Path length is 50 bytes but the data is only 5 bytes
        let bytes: Vec<u8> = vec![
            50, 0, 0, 0, // Path length = 50
            10, 0, 0, 0, // File size = 10
            1, 2, 3, 4, 5, // Only 5 bytes of path data
        ];

        let result: Result<FileMetadata, _> = bytes.try_into();

        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.to_string(), "Path name got truncated.");
    }
}
