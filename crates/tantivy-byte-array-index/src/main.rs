mod common;
mod doc;
mod error;
mod index;
mod query;
mod query_session;

use crate::common::{Config, SchemaFields};
use crate::doc::{DocIdMapper, DocMapper, DocSchema, examples};
use crate::error::Error as LocalError;
use crate::error::Result;
use crate::index::{ImmutableIndex, IndexBuilder};
use crate::query_session::QuerySession;
use crc32fast::Hasher;
use datafusion_common::arrow::array::{ArrayRef, StringArray};
use datafusion_common::arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::arrow::record_batch::RecordBatch;
use log::{info, trace};
use query::boolean_query;
use serde::{Deserialize, Serialize};
use stable_deref_trait::StableDeref;
use std::ffi::OsStr;
use std::fmt::{Debug, Formatter};
use std::io::{BufWriter, Error, ErrorKind, Read, Write};
use std::ops::{Deref, Range};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tantivy::collector::{Count, DocSetCollector};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken, FileHandle, FileSlice, OwnedBytes, TerminatingWrite, WatchCallback, WatchHandle,
    WritePtr,
};
use tantivy::{Directory, HasLen, INDEX_FORMAT_VERSION, Index};

#[derive(Debug, Default, PartialEq, Clone)]
struct FileMetadata {
    /// Offset in data section from where the file contents start
    data_offset: u64,
    /// Logical length of index file with the footer removed
    data_content_len: u64,
    /// Length of the byte serialized footer
    data_footer_len: u8,
    /// Length of the file path in bytes
    path_len: u8,
    /// File path
    path: PathBuf,
}

impl FileMetadata {
    const DATA_OFFSET_BYTES: u8 = 8;
    const DATA_CONTENT_LEN_BYTES: u8 = 8;
    const DATA_FOOTER_LEN_BYTES: u8 = 1;
    const PATH_LEN_BYTES: u8 = 1;

    const fn header_size() -> usize {
        (Self::DATA_OFFSET_BYTES
            + Self::DATA_CONTENT_LEN_BYTES
            + Self::DATA_FOOTER_LEN_BYTES
            + Self::PATH_LEN_BYTES) as usize
    }

    pub fn new(
        path: PathBuf,
        data_content_len: u64,
        data_offset: u64,
        data_footer_len: u8,
    ) -> Self {
        let path_len = path.as_os_str().as_bytes().len() as u8;

        Self {
            path,
            path_len,
            data_offset,
            data_content_len,
            data_footer_len,
        }
    }

    fn to_bytes(&self, path: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::header_size() + path.len());

        bytes.extend(self.data_offset.to_le_bytes());
        bytes.extend(self.data_content_len.to_le_bytes());
        bytes.extend(self.data_footer_len.to_le_bytes());
        bytes.extend(self.path_len.to_le_bytes());
        bytes.extend(path);

        bytes
    }

    fn from_cursor(cursor: &mut std::io::Cursor<Vec<u8>>) -> std::result::Result<Self, LocalError> {
        let mut u64_buffer = [0u8; 8];
        cursor.read_exact(&mut u64_buffer)?;
        let data_offset = u64::from_le_bytes(u64_buffer);

        cursor.read_exact(&mut u64_buffer)?;
        let data_size = u64::from_le_bytes(u64_buffer);

        let mut u8_buffer = [0u8; 1];
        cursor.read_exact(u8_buffer.as_mut())?;
        let data_footer_len = u8::from_le_bytes(u8_buffer);

        cursor.read_exact(&mut u8_buffer)?;
        let path_len = u8::from_le_bytes(u8_buffer);

        let mut path_buffer = vec![0u8; path_len as usize];
        cursor.read_exact(&mut path_buffer)?;
        let path = PathBuf::from(OsStr::from_bytes(&path_buffer));

        Ok(FileMetadata::new(
            path,
            data_size,
            data_offset,
            data_footer_len,
        ))
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
    total_data_block_size: u64,
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
            total_data_block_size: 0,
            file_metadata_size: 0,
            file_metadata_crc32: 0,
            file_metadata_list: Vec::new(),
        }
    }
}

impl Header {
    fn set_file_count(mut self, count: u32) -> Self {
        self.file_count = count;
        self
    }

    fn set_version(mut self, version: u8) -> Self {
        self.version = version;
        self
    }

    fn set_file_metadata_size(mut self, size: u32) -> Self {
        self.file_metadata_size = size;
        self
    }

    fn set_file_metadata_crc32(mut self, crc32: u32) -> Self {
        self.file_metadata_crc32 = crc32;
        self
    }

    fn set_total_data_block_size(mut self, size: u64) -> Self {
        self.total_data_block_size = size;
        self
    }

    fn set_file_metadata_list(mut self, list: &[FileMetadata]) -> Self {
        self.file_metadata_list = list.to_vec();
        self
    }
}

impl From<Header> for Vec<u8> {
    fn from(value: Header) -> Self {
        let mut bytes = Vec::new();

        bytes.extend(MAGIC_BYTES);
        bytes.push(VERSION);
        bytes.extend(value.file_count.to_le_bytes());
        bytes.extend(value.total_data_block_size.to_le_bytes());

        let mut file_metadata_bytes: Vec<u8> = Vec::new();
        let mut hasher = Hasher::new();
        for file_metadata in value.file_metadata_list {
            let bytes: Vec<u8> = (&file_metadata).into();

            hasher.update(&bytes);
            file_metadata_bytes.extend(&bytes);
        }

        let file_metadata_size = file_metadata_bytes.len() as u32;
        let file_metadata_crc32 = hasher.finalize();

        bytes.extend(file_metadata_size.to_le_bytes());
        bytes.extend(file_metadata_crc32.to_le_bytes());
        bytes.extend(file_metadata_bytes);

        bytes
    }
}

impl TryFrom<Vec<u8>> for Header {
    type Error = LocalError;

    fn try_from(bytes: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        let mut cursor = std::io::Cursor::new(bytes);

        let mut magic_bytes = [0u8; 4];
        cursor.read_exact(&mut magic_bytes)?; // magic bytes
        debug_assert!(magic_bytes.eq(MAGIC_BYTES));

        let mut version = [0u8; 1];
        cursor.read_exact(&mut version)?;
        debug_assert_eq!(version.first(), Some(&VERSION));

        let mut file_count = [0u8; 4];
        cursor.read_exact(&mut file_count)?;

        let mut total_data_block_size = [0u8; 8];
        cursor.read_exact(&mut total_data_block_size)?;

        let mut file_metadata_size = [0u8; 4];
        cursor.read_exact(&mut file_metadata_size)?;

        let mut file_metadata_crc32 = [0u8; 4];
        cursor.read_exact(&mut file_metadata_crc32)?;

        let mut file_metadata_list_bytes =
            vec![0u8; u32::from_le_bytes(file_metadata_size) as usize];
        cursor.read_exact(&mut file_metadata_list_bytes)?;

        let checksum = crc32fast::hash(&file_metadata_list_bytes);
        debug_assert!(checksum == u32::from_le_bytes(file_metadata_crc32));

        // Cursor that wraps the entire file metadata block
        let mut cursor = std::io::Cursor::new(file_metadata_list_bytes);
        let mut file_metadata_list: Vec<FileMetadata> = vec![];
        for _ in 0..u32::from_le_bytes(file_count) {
            let file_metadata = FileMetadata::from_cursor(&mut cursor)?;
            file_metadata_list.push(file_metadata);
        }

        let header = Header::default()
            .set_version(version[0])
            .set_file_count(u32::from_le_bytes(file_count))
            .set_total_data_block_size(u64::from_le_bytes(total_data_block_size))
            .set_file_metadata_size(u32::from_le_bytes(file_metadata_size))
            .set_file_metadata_crc32(u32::from_le_bytes(file_metadata_crc32))
            .set_file_metadata_list(&file_metadata_list);

        Ok(header)
    }
}

#[derive(Clone)]
struct DataBlock {
    data: Arc<[u8]>,
    range: Range<usize>,
}

impl Debug for DataBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("DataBlock");

        let display_limit = 10; // no. of bytes to display

        debug_struct.field("range", &self.range);

        if self.data.len() < display_limit {
            debug_struct.field("data", &self.data);
        } else {
            let head = &self.data[0..display_limit];
            let tail = &self.data[self.data.len() - display_limit..];

            debug_struct.field("data_head", &head);
            debug_struct.field("data_tail", &tail);
        }

        debug_struct.finish()
    }
}

impl DataBlock {
    fn new(data: Vec<u8>) -> Self {
        let range = 0..data.len();
        Self {
            data: Arc::from(data),
            range,
        }
    }

    fn slice_from(&self, range: Range<usize>) -> DataBlock {
        let new_start = self.range.start + range.start;
        let new_end = self.range.start + range.end;

        assert!(range.end <= self.range.len(), "Range out of bounds");

        Self {
            data: self.data.clone(),
            range: new_start..new_end,
        }
    }
}

impl Deref for DataBlock {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data[self.range.clone()]
    }
}

unsafe impl StableDeref for DataBlock {}

impl FileHandle for DataBlock {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        if range.end > self.range.len() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Input range out of bounds",
            ));
        }

        let slice = self.slice_from(range);

        Ok(OwnedBytes::new(slice))
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

    fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut payload = serde_json::to_string(&self)?.into_bytes();
        let payload_len = u32::to_le_bytes(payload.len() as u32);
        let footer_magic = u32::to_le_bytes(TANTIVY_FOOTER_MAGIC_NUMBER);

        payload.extend_from_slice(&payload_len);
        payload.extend_from_slice(&footer_magic);

        Ok(payload)
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct InnerDirectory {
    file_map: std::collections::HashMap<PathBuf, DataBlock>,
}

impl InnerDirectory {
    #[allow(dead_code)]
    fn new(header: Header, data_block: DataBlock) -> Arc<RwLock<InnerDirectory>> {
        let mut fs = std::collections::HashMap::new();

        for (id, file_metadata) in header.file_metadata_list.iter().enumerate() {
            let range = (file_metadata.data_offset as usize)
                ..((file_metadata.data_offset
                + file_metadata.data_content_len
                + file_metadata.data_footer_len as u64) as usize);
            trace!("[{id}] Range: {}..{}", range.start, range.end);

            let sub_data_block = data_block.slice_from(range); // zero-copy slice

            fs.insert(file_metadata.path.clone(), sub_data_block);
            trace!("[{id}] Inserted key: {:?}", file_metadata.path.clone());
        }

        Arc::new(RwLock::new(Self { file_map: fs }))
    }

    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        self.file_map
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_owned()))
            .cloned()
            .map(|data_block| Arc::new(data_block) as Arc<dyn FileHandle>)
    }

    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        Ok(self.file_map.contains_key(path))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct ReadOnlyArchiveDirectory {
    inner: Arc<RwLock<InnerDirectory>>,
}

impl ReadOnlyArchiveDirectory {
    #[allow(dead_code)]
    fn new(header: Header, data: DataBlock) -> ReadOnlyArchiveDirectory {
        Self {
            inner: InnerDirectory::new(header, data),
        }
    }
}

impl Directory for ReadOnlyArchiveDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        self.inner.read().unwrap().get_file_handle(path)
    }

    fn delete(&self, _path: &Path) -> std::result::Result<(), DeleteError> {
        Ok(()) // no-op
    }

    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        self.inner.read().unwrap().exists(path)
    }

    fn open_write(&self, path: &Path) -> std::result::Result<WritePtr, OpenWriteError> {
        if path
            .file_name()
            .is_some_and(|name| name == ".tantivy-meta.lock")
        {
            Ok(BufWriter::new(Box::new(NoopWriter)))
        } else {
            panic!(
                "Attempted to write to a read-only directory for path: {:?}",
                path.display()
            );
        }
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        let bytes =
            self.open_read(path)?
                .read_bytes()
                .map_err(|io_error| OpenReadError::IoError {
                    io_error: Arc::new(io_error),
                    filepath: path.to_path_buf(),
                })?;
        Ok(bytes.as_slice().to_owned())
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> std::io::Result<()> {
        todo!()
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        todo!()
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

struct NoopWriter;

impl Drop for NoopWriter {
    fn drop(&mut self) {}
}
impl Write for NoopWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len()) // report that all the bytes were written successfully
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(()) // yay!
    }
}

impl TerminatingWrite for NoopWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> std::io::Result<()> {
        self.flush()
    }
}

fn run_search_queries(query_session: &QuerySession, doc_mapper: &DocMapper) -> Result<()> {
    let print_search_results = |query| -> Result<()> {
        let results = query_session.search(query, &DocSetCollector)?;

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
    };

    let q1 = boolean_query::title_contains_diary_and_not_girl(&query_session.schema())?;
    info!("Q1: title:+diary AND title:-girl");
    let count = query_session.search(&q1, &Count)?;
    debug_assert_eq!(count, 1);
    info!("Matches count: {count}");
    print_search_results(&q1)?;
    info!("***");

    let q2 = boolean_query::title_contains_diary_or_cow(&query_session.schema())?;
    info!("Q2: title:diary OR title:cow");
    let count = query_session.search(&q2, &Count)?;
    debug_assert_eq!(count, 4);
    info!("Matches count: {}", query_session.search(&q2, &Count)?);
    print_search_results(&q2)?;
    info!("***");

    let q3 = boolean_query::combine_term_and_phrase_query(&query_session.schema())?;
    info!("Q3: title:diary OR title:\"dairy cow\"");
    let count = query_session.search(&q3, &Count)?;
    debug_assert_eq!(count, 4);
    info!("Matches count: {}", query_session.search(&q3, &Count)?);
    print_search_results(&q3)?;
    info!("***");

    Ok(())
}

const TANTIVY_METADATA_PATH: &str = "meta.json";
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

    let metadata_path: PathBuf = PathBuf::from(TANTIVY_METADATA_PATH);
    let dir = index.directory();

    // +----------------+
    // | Query Baseline |
    // +----------------+
    // These identical queries should work on our read-only archive directory implementation.

    let query_session = QuerySession::new(&index)?;
    let doc_mapper = DocMapper::new(query_session.searcher(), &config, original_docs);

    info!(">>> Querying RamDirectory");
    run_search_queries(&query_session, &doc_mapper)?;
    info!("---");

    // These are the stages for preparing the index as a sequence of bytes.
    //      1. Draft the FileMetadata.
    //      2. Build the data block one file at a time.
    //          a. Back-fill pending fields in FileMetadata (offset, footer, crc)
    //      4. Build the Header.

    // +--------------------+
    // | Draft FileMetadata |
    // +--------------------+
    //      - Path length
    //      - Path
    //      - Logical file size (footer is removed by Tantivy)
    //
    // The other fields are known only while building the data block. So it will be back-filled
    // when the data blocks are being written.

    let mut file_metadata_list = Vec::<FileMetadata>::new();
    enum LogicalSlice {
        MetaJson(Vec<u8>),
        IndexDataFile(FileSlice),
    }

    let get_logical_slice = |path: &Path| -> Result<LogicalSlice> {
        let logical_slice = if path.eq(&metadata_path) {
            LogicalSlice::MetaJson(dir.atomic_read(path)?)
        } else {
            LogicalSlice::IndexDataFile(dir.open_read(path)?)
        };

        Ok(logical_slice)
    };

    for (i, path) in dir.list_managed_files().iter().enumerate() {
        let logical_size = get_logical_slice(path).map(|value| match value {
            LogicalSlice::MetaJson(inner) => inner.len(),
            LogicalSlice::IndexDataFile(inner) => inner.len(),
        })?;
        let draft = FileMetadata::new(path.to_path_buf(), logical_size as u64, 0, 0);

        trace!("[{i}] {draft:#?}");

        file_metadata_list.push(draft);
    }

    // +------------+
    // | Data Block |
    // +------------+
    // The Tantivy directory strips the footer and serves only the logical file contents. So when
    // assembling the data block implement a workaround - manually reconstruct and append the footer
    // for all index data binary files (except the meta.json).
    //
    // Now we can back-fill the `data_footer_len` field in `FileMetadata`. We can also now compute
    // and back-fill the `data_offset` of the next `FileMetadata` entry.

    let mut data_block_bytes = Vec::<u8>::new();
    let mut current_offset = 0;
    for (i, file_metadata) in file_metadata_list.iter_mut().enumerate() {
        let contents = get_logical_slice(&file_metadata.path)?;
        let mut physical_size: u64 = 0;
        match contents {
            LogicalSlice::MetaJson(data) => {
                debug_assert_eq!(file_metadata.data_content_len as usize, data.len(),);
                physical_size = data.len() as u64;

                // Back-fill FileMetadata properties
                debug_assert_eq!(file_metadata.data_offset, 0);
                debug_assert_eq!(file_metadata.data_footer_len, 0);

                file_metadata.data_offset = current_offset;
                file_metadata.data_footer_len = 0; // footer is only for binary index files

                // Append bytes to data block
                data_block_bytes.extend(data);
            }
            LogicalSlice::IndexDataFile(file_slice) => {
                let mut crc_hasher = Hasher::new();
                for chunk in file_slice.stream_file_chunks() {
                    let chunk = chunk?;

                    physical_size += chunk.len() as u64;
                    crc_hasher.update(&chunk);

                    data_block_bytes.extend(chunk.as_slice());
                }

                debug_assert_eq!(file_metadata.data_content_len, physical_size);

                let crc_hash = crc_hasher.finalize();

                // Back-fill FileMetadata properties
                debug_assert_eq!(file_metadata.data_offset, 0);
                debug_assert_eq!(file_metadata.data_footer_len, 0);

                file_metadata.data_offset = current_offset;

                let footer = TantivyFooterHack::new(crc_hash).to_bytes()?;
                physical_size += footer.len() as u64;
                file_metadata.data_footer_len = footer.len() as u8;

                data_block_bytes.extend(footer);
            }
        }
        // Update offset for (N+1)th file
        current_offset += physical_size;

        trace!("[{i}] Data block size: {}", data_block_bytes.len());
        trace!("[{i}] FileMetadata (after back-fill) {file_metadata:#?}");
    }
    let data_block = DataBlock::new(data_block_bytes);

    // +--------------+
    // | Header Block |
    // +--------------+
    let file_count = file_metadata_list.len();
    let total_data_block_size = data_block.len();

    let file_metadata_list_bytes = file_metadata_list
        .iter()
        .fold(Vec::<u8>::new(), |acc, fm| [acc, fm.into()].concat());
    let file_metadata_list_bytes_size = file_metadata_list_bytes.len();
    let file_metadata_list_crc32 = crc32fast::hash(&file_metadata_list_bytes);

    let header = Header::default()
        .set_version(VERSION)
        .set_file_count(file_count as u32)
        .set_file_metadata_size(file_metadata_list_bytes_size as u32)
        .set_file_metadata_crc32(file_metadata_list_crc32)
        .set_total_data_block_size(total_data_block_size as u64)
        .set_file_metadata_list(&file_metadata_list);

    trace!("Magic: {MAGIC_BYTES:?}");
    trace!(
        "[Header] data block size:{}, file metadata block size: {} file metadata block crc32:{}",
        header.total_data_block_size, header.file_metadata_size, header.file_metadata_crc32
    );

    let header_bytes: Vec<u8> = header.into();
    let roundtrip_header = Header::try_from(header_bytes)?;
    trace!(
        "[Header] data block size:{}, file metadata block size: {} file metadata block crc32:{}",
        roundtrip_header.total_data_block_size,
        roundtrip_header.file_metadata_size,
        roundtrip_header.file_metadata_crc32
    );
    trace!("[Header] Round trip header:{roundtrip_header:#?}");

    // +----------------------+
    // | Blob Index Directory |
    // +----------------------+
    let archive_dir = ReadOnlyArchiveDirectory::new(roundtrip_header, data_block);
    trace!("Read-only Archive Directory:{archive_dir:?}");

    let schema = DocSchema::new(&config).into_schema();
    let read_only_index = Index::open_or_create(archive_dir, schema)?;
    let index_wrapper = ImmutableIndex::new(read_only_index);

    // +---------------------------+
    // | Query Baseline Comparison |
    // +---------------------------+
    // The read-only directory of this index was constructed from an index. We verified earlier that
    // querying it works without problems. The same queries should return identical results when
    // applied against the read-only archive directory implementation.

    let query_session = QuerySession::new(&index_wrapper)?;
    let doc_mapper = DocMapper::new(query_session.searcher(), &config, original_docs);

    info!(">>> Querying ReadOnlyArchiveDirectory");
    run_search_queries(&query_session, &doc_mapper)?;

    // +------------------------------------------+
    // | Create Parquet File With Full-Text Index |
    // +------------------------------------------+

    let title_field = Field::new("title", DataType::Utf8, true);
    let body_field = Field::new("body", DataType::Utf8, true);

    let schema = Schema::new(vec![title_field, body_field]);

    let title_array: ArrayRef = Arc::new(
        examples()
            .iter()
            .map(|doc| Some(doc.title()))
            .collect::<StringArray>(),
    );
    let body_array: ArrayRef = Arc::new(
        examples().iter().map(|doc| doc.body()).collect::<StringArray>(),
    );
    let batch = RecordBatch::try_new(Arc::new(schema), vec![title_array, body_array])?;
    info!(">>> Creating RecordBatch");
    info!("[RecordBatch] {batch:?}");

    Ok(())
}

/// Initializes the logger.
fn setup_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
}
