use crate::custom_index::file_metadata::FileMetadata;
use crate::index::ImmutableIndex;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tantivy::directory::FileSlice;
use tantivy::{Directory, HasLen, INDEX_FORMAT_VERSION};

const TANTIVY_METADATA_PATH: &str = "meta.json";

/// A logical view of the directory contents of a Tantivy index.
pub enum LogicalFileSlice {
    /// In-memory binary blob of `meta.json`
    MetaJson(Vec<u8>),
    /// Logical slice of index binary files
    IndexDataFile(FileSlice),
}

pub struct LogicalFileSliceStat {
    pub total_bytes: u64,
    pub footer_bytes: u8,
}

impl LogicalFileSlice {
    pub fn try_new(index: &ImmutableIndex, path: &Path) -> crate::error::Result<Self> {
        if path == Path::new(TANTIVY_METADATA_PATH) {
            let meta_bytes = index.directory().atomic_read(path)?;
            Ok(LogicalFileSlice::MetaJson(meta_bytes))
        } else {
            let data_bytes = index.directory().open_read(path)?;
            Ok(LogicalFileSlice::IndexDataFile(data_bytes))
        }
    }

    pub fn size_in_bytes(&self) -> u64 {
        match self {
            LogicalFileSlice::MetaJson(bytes) => bytes.len() as u64,
            LogicalFileSlice::IndexDataFile(bytes) => bytes.len() as u64,
        }
    }

    pub fn try_append_to_buffer(
        &self,
        buf: &mut Vec<u8>,
        draft: &FileMetadata,
    ) -> crate::error::Result<LogicalFileSliceStat> {
        match self {
            LogicalFileSlice::MetaJson(bytes) => {
                debug_assert_eq!(
                    draft.data_content_len as usize,
                    bytes.len(),
                    "Expected size of MetaJson to match the size in draft FileMetadata. Draft: {draft:?}"
                );

                buf.extend_from_slice(bytes);

                Ok(LogicalFileSliceStat {
                    total_bytes: bytes.len() as u64,
                    footer_bytes: 0,
                })
            }
            LogicalFileSlice::IndexDataFile(bytes) => {
                debug_assert_eq!(
                    draft.data_content_len as usize,
                    bytes.len(),
                    "Expected size of IndexDataFile to match the size in draft FileMetadata. Draft: {draft:?}"
                );

                let mut crc_hasher = Hasher::new();
                let mut total_bytes = 0;
                for chunk in bytes.stream_file_chunks() {
                    let chunk = chunk?;

                    total_bytes += chunk.len() as u64;
                    crc_hasher.update(&chunk);

                    buf.extend(chunk.as_slice());
                }
                let crc_hash = crc_hasher.finalize();
                let footer_bytes = TantivyFooterHack::new(crc_hash).to_bytes()?;
                total_bytes += footer_bytes.len() as u64;

                buf.extend(&footer_bytes);

                Ok(LogicalFileSliceStat {
                    total_bytes,
                    footer_bytes: footer_bytes.len() as u8,
                })
            }
        }
    }
}

const TANTIVY_FOOTER_MAGIC_NUMBER: u32 = 1337;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct TantivyVersion {
    major: u32,
    minor: u32,
    patch: u32,
    index_format_version: u32,
}

impl Default for TantivyVersion {
    fn default() -> Self {
        Self {
            major: 0,
            minor: 24,
            patch: 1,
            index_format_version: INDEX_FORMAT_VERSION,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TantivyFooterHack {
    version: TantivyVersion,
    crc: u32,
}

impl TantivyFooterHack {
    fn new(crc: u32) -> Self {
        Self {
            version: TantivyVersion::default(),
            crc,
        }
    }

    fn to_bytes(&self) -> crate::error::Result<Vec<u8>> {
        let mut payload = serde_json::to_string(&self)?.into_bytes();
        let payload_len = u32::to_le_bytes(payload.len() as u32);
        let footer_magic = u32::to_le_bytes(TANTIVY_FOOTER_MAGIC_NUMBER);

        payload.extend_from_slice(&payload_len);
        payload.extend_from_slice(&footer_magic);

        Ok(payload)
    }
}
