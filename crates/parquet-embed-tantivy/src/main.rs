mod common;
mod doc;
mod error;
mod index;
mod query;
mod query_session;

use crate::common::{Config, SchemaFields};
use crate::doc::{examples, DocIdMapper, DocMapper, DocSchema};
use crate::error::Error as LocalError;
use crate::error::Error::ParquetMetadata;
use crate::error::Result;
use crate::index::{ImmutableIndex, IndexBuilder};
use crate::query_session::QuerySession;
use async_trait::async_trait;
use crc32fast::Hasher;
use datafusion::arrow::array::UInt64Array;
use datafusion::physical_expr::create_physical_expr;
use datafusion::prelude::SessionContext;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::arrow::array::{ArrayRef, StringArray};
use datafusion_common::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::arrow::record_batch::RecordBatch;
use datafusion_common::{DFSchema, DataFusionError, ScalarValue};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::expr::Expr;
use datafusion_expr::{col, lit, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::execution_plan::ExecutionPlan;
use log::{info, trace};
use parquet::arrow::ArrowWriter;
use parquet::data_type::AsBytes;
use parquet::errors::ParquetError;
use parquet::file::metadata::KeyValue;
use parquet::file::reader::{FileReader, SerializedFileReader};
use query::boolean_query;
use serde::{Deserialize, Serialize};
use stable_deref_trait::StableDeref;
use std::any::Any;
use std::ffi::OsStr;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{BufWriter, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut, Range};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tantivy::collector::{Count, DocSetCollector};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken, FileHandle, FileSlice, OwnedBytes, TerminatingWrite, WatchCallback, WatchHandle,
    WritePtr,
};
use tantivy::query::{PhraseQuery, Query};
use tantivy::schema::{Schema as IndexSchema, Value};
use tantivy::{Directory, HasLen, Index, TantivyDocument, Term, INDEX_FORMAT_VERSION};

const TANTIVY_METADATA_PATH: &str = "meta.json";

/// A logical view of the directory contents of a Tantivy index.
enum LogicalFileSlice {
    /// In-memory binary blob of `meta.json`
    MetaJson(Vec<u8>),
    /// Logical slice of index binary files
    IndexDataFile(FileSlice),
}

struct LogicalFileSliceStat {
    total_bytes: u64,
    footer_bytes: u8,
}

impl LogicalFileSlice {
    fn try_new(index: &ImmutableIndex, path: &Path) -> Result<Self> {
        if path == Path::new(TANTIVY_METADATA_PATH) {
            let meta_bytes = index.directory().atomic_read(path)?;
            Ok(LogicalFileSlice::MetaJson(meta_bytes))
        } else {
            let data_bytes = index.directory().open_read(path)?;
            Ok(LogicalFileSlice::IndexDataFile(data_bytes))
        }
    }

    fn size_in_bytes(&self) -> u64 {
        match self {
            LogicalFileSlice::MetaJson(bytes) => bytes.len() as u64,
            LogicalFileSlice::IndexDataFile(bytes) => bytes.len() as u64,
        }
    }

    fn try_append_to_buffer(
        &self,
        buf: &mut Vec<u8>,
        draft: &FileMetadata,
    ) -> Result<LogicalFileSliceStat> {
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

/// A draft manifest of the physical files managed by the Tantivy index.
///
/// The content related fields in `FileMetadata` are backfilled when the data block are assembled,
/// to create a final version of the manifest.
pub struct DraftManifest(Vec<FileMetadata>);

impl Deref for DraftManifest {
    type Target = Vec<FileMetadata>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DraftManifest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl DraftManifest {
    pub fn try_new(index: &ImmutableIndex) -> Result<Self> {
        let inner = index
            .directory()
            .list_managed_files()
            .iter()
            .map(|path| {
                let file = LogicalFileSlice::try_new(index, path)?;
                Ok(FileMetadata::new(
                    PathBuf::from(path),
                    file.size_in_bytes(),
                    0,
                    0,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        inner.iter().for_each(|draft| trace!("{draft:?}"));

        Ok(Self(inner))
    }

    pub fn try_into(mut self, index: &ImmutableIndex) -> Result<(Header, DataBlock)> {
        let mut buf: Vec<u8> = vec![];
        let mut current_offset = 0;

        for meta in self.iter_mut() {
            let slice = LogicalFileSlice::try_new(index, &meta.path)?;
            let stat = slice.try_append_to_buffer(&mut buf, meta)?;

            meta.data_offset = current_offset;
            meta.data_footer_len = stat.footer_bytes;

            current_offset += stat.total_bytes;

            trace!("Data block size: {}", stat.total_bytes);
            trace!("FileMetadata after backfill: {meta:?}");
        }

        let final_manifest = FinalManifest(self.0);
        let data_block = DataBlock::new(buf);

        let header = final_manifest.into_header(data_block.len() as u64);

        Ok((header, data_block))
    }
}

struct FinalManifest(Vec<FileMetadata>);

impl Deref for FinalManifest {
    type Target = Vec<FileMetadata>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FinalManifest {
    pub fn into_header(self, data_block_size: u64) -> Header {
        let file_count = self.len();

        let file_metadata_list_bytes = self
            .iter()
            .flat_map(|meta| -> Vec<u8> { meta.into() })
            .collect::<Vec<_>>();
        let file_metadata_list_bytes_size = file_metadata_list_bytes.len();
        let file_metadata_list_crc32 = crc32fast::hash(&file_metadata_list_bytes);

        let header = Header::default()
            .set_version(VERSION)
            .set_file_count(file_count as u32)
            .set_file_metadata_size(file_metadata_list_bytes_size as u32)
            .set_file_metadata_crc32(file_metadata_list_crc32)
            .set_total_data_block_size(data_block_size)
            .set_file_metadata_list(&self);

        trace!("Magic: {MAGIC_BYTES:?}");
        trace!("Header: data_block_size={}", header.total_data_block_size);
        trace!("Header: file_metadata_size={}", header.file_metadata_size);
        trace!("Header: file_metadata_crc32={}", header.file_metadata_crc32);

        header
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub struct FileMetadata {
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
pub struct Header {
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

    fn from_reader<R: Read>(mut reader: R) -> Result<Self> {
        let mut magic_bytes = [0u8; 4];
        reader.read_exact(&mut magic_bytes)?; // magic bytes
        debug_assert!(magic_bytes.eq(MAGIC_BYTES));

        let mut version = [0u8; 1];
        reader.read_exact(&mut version)?;
        debug_assert_eq!(version.first(), Some(&VERSION));

        let mut file_count = [0u8; 4];
        reader.read_exact(&mut file_count)?;

        let mut total_data_block_size = [0u8; 8];
        reader.read_exact(&mut total_data_block_size)?;

        let mut file_metadata_size = [0u8; 4];
        reader.read_exact(&mut file_metadata_size)?;

        let mut file_metadata_crc32 = [0u8; 4];
        reader.read_exact(&mut file_metadata_crc32)?;

        let mut file_metadata_list_bytes =
            vec![0u8; u32::from_le_bytes(file_metadata_size) as usize];
        reader.read_exact(&mut file_metadata_list_bytes)?;

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
        Self::from_reader(&mut cursor)
    }
}

#[derive(Clone)]
pub struct DataBlock {
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
            let range_start = file_metadata.data_offset as usize;
            let range_end = range_start
                + file_metadata.data_content_len as usize
                + file_metadata.data_footer_len as usize;
            let range = range_start..range_end;
            trace!("[{id}] {range:?}");

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

struct FullTextIndex {
    path: PathBuf,
    index: Index,
    index_schema: Arc<IndexSchema>,
    arrow_schema: SchemaRef,
}

impl FullTextIndex {
    fn try_open(
        path: &Path,
        index_schema: Arc<IndexSchema>,
        arrow_schema: SchemaRef,
    ) -> Result<Self> {
        let dir = Self::try_read_directory(path)?;
        let index = Index::open_or_create(dir, index_schema.as_ref().clone())?;

        Ok(Self {
            path: path.to_path_buf(),
            index,
            index_schema,
            arrow_schema,
        })
    }

    fn try_read_directory(path: &Path) -> Result<ReadOnlyArchiveDirectory> {
        let mut file = File::open(path)?;

        let reader = SerializedFileReader::new(file.try_clone()?)?;

        let index_offset = Self::try_index_offset(path, reader)?;

        file.seek(SeekFrom::Start(index_offset))?;
        let serialized_header = Header::from_reader(&file)?;

        let mut data_block_buffer = vec![0u8; serialized_header.total_data_block_size as usize];
        file.read_exact(&mut data_block_buffer)?;
        let serialized_data = DataBlock::new(data_block_buffer);

        Ok(ReadOnlyArchiveDirectory::new(
            serialized_header,
            serialized_data,
        ))
    }

    fn try_index_offset(path: &Path, reader: SerializedFileReader<File>) -> Result<u64> {
        let index_offset = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "Could not find key_value_metadata in file: {}",
                    path.display()
                ))
            })?
            .iter()
            .find(|kv| kv.key == FULL_TEXT_INDEX_KEY)
            .and_then(|kv| kv.value.as_deref())
            .ok_or_else(|| {
                ParquetError::General(format!(
                    "Could not find a valid value for key `{}` in file {}",
                    FULL_TEXT_INDEX_KEY,
                    path.display()
                ))
            })?
            .parse::<u64>()
            .map_err(|e| ParquetError::General(e.to_string()))?;

        Ok(index_offset)
    }
}

impl Debug for FullTextIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullTextIndex")
            .field("path", &self.path)
            .field("index", &self.index)
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

#[async_trait]
impl TableProvider for FullTextIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let mut phrase: Option<&str> = None;

        // Currently handles only a single wildcard LIKE query on the `title` column. A generalized
        // implementation will use: [`PruningPredicate`]
        //
        // [`PruningPredicate`]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/pruning/struct.PruningPredicate.html
        if filters.len() == 1 {
            if let Expr::Like(like) = filters.first().unwrap() {
                if !like.negated {
                    if let (
                        Expr::Column(col),
                        Expr::Literal(ScalarValue::Utf8(Some(pattern)), None),
                    ) = (&*like.expr, &*like.pattern)
                    {
                        info!("Column: {}, Pattern: {pattern}", col.name);
                        if col.name == "title" {
                            if let Some(inner) =
                                pattern.strip_prefix('%').and_then(|s| s.strip_suffix('%'))
                            {
                                phrase = Some(inner);
                            }
                        }
                    }
                }
            }
        }

        let mut matching_doc_ids = Vec::new();
        if let Some(inner) = phrase {
            info!("phrase: {inner}");

            let title_field = self
                .index_schema
                .get_field("title")
                .map_err(|e| DataFusionError::External(e.into()))?;
            let id_field = self
                .index_schema
                .get_field("id")
                .map_err(|e| DataFusionError::External(e.into()))?;
            let title_phrase_query = Box::new(PhraseQuery::new(
                inner
                    .split_whitespace()
                    .map(|s| Term::from_field_text(title_field, s))
                    .collect(),
            )) as Box<dyn Query>;

            let reader = self
                .index
                .reader()
                .map_err(|e| DataFusionError::External(e.into()))?;
            let searcher = reader.searcher();

            if let Ok(matches) = searcher.search(&*title_phrase_query, &DocSetCollector) {
                let matched_ids = matches
                    .iter()
                    .filter_map(|doc_address| {
                        searcher
                            .doc::<TantivyDocument>(*doc_address)
                            .ok()? // discard the error if doc doesn't exist in the index
                            .get_first(id_field)
                            .and_then(|v| v.as_u64())
                    })
                    .collect::<Vec<_>>();

                matching_doc_ids.extend(matched_ids);
            }
        }

        info!("Matching doc ids from full-text index: {matching_doc_ids:?}");

        // constructing the `id IN (...)` expression to pushdown into parquet file
        let ids: Vec<Expr> = matching_doc_ids.iter().map(|doc_id| lit(*doc_id)).collect();
        let id_filter = col("id").in_list(ids, false);
        info!("filter expr: {id_filter}");

        let df_schema = DFSchema::try_from(self.arrow_schema.clone())?;
        let physical_predicate =
            create_physical_expr(&id_filter, &df_schema, state.execution_props())?;
        info!("physical expr: {physical_predicate}");

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let source = Arc::new(
            ParquetSource::default()
                .with_enable_page_index(true)
                .with_predicate(physical_predicate)
                .with_pushdown_filters(true),
        );

        let absolute_path = std::fs::canonicalize(&self.path)?;
        let len = std::fs::metadata(&absolute_path)?.len();
        let partitioned_file = PartitionedFile::new(absolute_path.to_string_lossy(), len);

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, self.schema().clone(), source)
                .with_file(partitioned_file)
                .build();

        Ok(Arc::new(DataSourceExec::new(Arc::new(file_scan_config))))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

fn run_search_queries(query_session: &QuerySession, doc_mapper: &DocMapper) -> Result<()> {
    let print_search_results = |query| -> Result<()> {
        let results = query_session.search(query, &DocSetCollector)?;

        for doc_address in results {
            let Ok(Some(doc_id)) = doc_mapper.get_doc_id(doc_address) else {
                trace!("Failed to get doc id from doc address: {doc_address:?}");
                continue;
            };

            if let Some(doc) = doc_mapper.get_original_doc(doc_id) {
                trace!("Matched Doc [ID={doc_id:?}]: {doc:?}")
            } else {
                trace!("Failed to reverse map id: {doc_id:?} to a document")
            }
        }
        Ok(())
    };

    let q1 = boolean_query::title_contains_diary_and_not_girl(&query_session.schema())?;
    trace!("Q1: title:+diary AND title:-girl");
    let count = query_session.search(&q1, &Count)?;
    debug_assert_eq!(count, 1);
    trace!("Matches count: {count}");
    print_search_results(&q1)?;
    trace!("***");

    let q2 = boolean_query::title_contains_diary_or_cow(&query_session.schema())?;
    trace!("Q2: title:diary OR title:cow");
    let count = query_session.search(&q2, &Count)?;
    debug_assert_eq!(count, 4);
    trace!("Matches count: {}", query_session.search(&q2, &Count)?);
    print_search_results(&q2)?;
    trace!("***");

    let q3 = boolean_query::combine_term_and_phrase_query(&query_session.schema())?;
    trace!("Q3: title:diary OR title:\"dairy cow\"");
    let count = query_session.search(&q3, &Count)?;
    debug_assert_eq!(count, 4);
    trace!("Matches count: {}", query_session.search(&q3, &Count)?);
    print_search_results(&q3)?;
    trace!("***");

    Ok(())
}

const FULL_TEXT_INDEX_KEY: &str = "tantivy_index_offset";

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();

    let config = Config::default();
    let schema = Arc::new(DocSchema::new(&config).into_schema());
    let original_docs = examples();

    let fields = SchemaFields::new(schema.clone(), &config)?;

    let index = IndexBuilder::new(schema.clone())
        .index_and_commit(
            config.index_writer_memory_budget_in_bytes,
            &fields,
            original_docs,
        )?
        .build();

    // +----------------+
    // | Query Baseline |
    // +----------------+
    // These identical queries should work on our read-only archive directory implementation.

    let query_session = QuerySession::new(&index)?;
    let doc_mapper = DocMapper::new(query_session.searcher(), &config, original_docs);

    trace!(">>> Querying RamDirectory");
    run_search_queries(&query_session, &doc_mapper)?;
    trace!("---");

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

    // +------------+
    // | Data Block |
    // +------------+
    // The Tantivy directory strips the footer and serves only the logical file contents. So when
    // assembling the data block implement a workaround - manually reconstruct and append the footer
    // for all index data binary files (except the meta.json).
    //
    // Now we can back-fill the `data_footer_len` field in `FileMetadata`. We can also now compute
    // and back-fill the `data_offset` of the next `FileMetadata` entry.

    // +--------------+
    // | Header Block |
    // +--------------+

    let (header, data_block) = DraftManifest::try_new(&index)?.try_into(&index)?;

    let header_bytes: Vec<u8> = header.clone().into();
    let roundtrip_header = Header::try_from(header_bytes.clone())?;
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
    let archive_dir = ReadOnlyArchiveDirectory::new(roundtrip_header, data_block.clone());
    trace!("Read-only Archive Directory:{archive_dir:?}");

    let read_only_index = Index::open_or_create(archive_dir, schema.as_ref().clone())?;
    let index_wrapper = ImmutableIndex::new(read_only_index);

    // +---------------------------+
    // | Query Baseline Comparison |
    // +---------------------------+
    // The read-only directory of this index was constructed from an index. We verified earlier that
    // querying it works without problems. The same queries should return identical results when
    // applied against the read-only archive directory implementation.

    let query_session = QuerySession::new(&index_wrapper)?;
    let doc_mapper = DocMapper::new(query_session.searcher(), &config, original_docs);

    trace!(">>> Querying ReadOnlyArchiveDirectory");
    run_search_queries(&query_session, &doc_mapper)?;

    // +---------------------------------------+
    // | Embed Full-Text Index in Parquet File |
    // +---------------------------------------+
    // The `RecordBatch`es are written first. This is followed by the byte serialized full-text
    // index. The offset of the index is added to `FileMetadata.key_value_metadata`. The Parquet
    // file size is now larger because of the embedded full text index. This is backwards compatible
    // with readers who will skip the index embedded within the file.

    let id_field = Field::new("id", DataType::UInt64, false);
    let title_field = Field::new("title", DataType::Utf8, true);
    let body_field = Field::new("body", DataType::Utf8, true);

    let arrow_schema_ref = Arc::new(Schema::new(vec![id_field, title_field, body_field]));

    let id_array: ArrayRef = Arc::new(
        examples()
            .iter()
            .map(|doc| doc.id())
            .collect::<UInt64Array>(),
    );
    let title_array: ArrayRef = Arc::new(
        examples()
            .iter()
            .map(|doc| Some(doc.title()))
            .collect::<StringArray>(),
    );
    let body_array: ArrayRef = Arc::new(
        examples()
            .iter()
            .map(|doc| doc.body())
            .collect::<StringArray>(),
    );
    let batch = RecordBatch::try_new(
        arrow_schema_ref.clone(),
        vec![id_array, title_array, body_array],
    )?;

    let saved_path = PathBuf::from("fat.parquet");
    let file = File::create(&saved_path)?;

    let mut writer = ArrowWriter::try_new(file, arrow_schema_ref.clone(), None)?;

    writer.write(&batch)?;
    writer.flush()?;

    let offset = writer.bytes_written();

    writer.write_all(header_bytes.as_bytes())?;
    writer.write_all(data_block.deref())?;

    info!("Index will be written to offset: {offset}");
    info!(
        "Index size: {} bytes",
        header_bytes.len() + data_block.len()
    );

    // Store the full-text index offset in `FileMetadata.key_value_metadata` for reading it back
    // later.
    writer.append_key_value_metadata(KeyValue::new(
        FULL_TEXT_INDEX_KEY.to_string(),
        offset.to_string(),
    ));

    writer.close()?;
    info!("Wrote Parquet file to: {}", saved_path.display());

    // +-------------------------------------+
    // | Read Offset from key_value_metadata |
    // +-------------------------------------+

    let mut file = File::open(&saved_path)?;

    let reader = SerializedFileReader::new(file.try_clone()?)?;
    let meta = reader.metadata().file_metadata();

    let kvs = meta.key_value_metadata().ok_or_else(|| {
        ParquetMetadata("Could not find key_value_metadata in FileMetadata".to_string())
    })?;
    let kv = kvs
        .iter()
        .find(|kv| kv.key == FULL_TEXT_INDEX_KEY)
        .ok_or(ParquetMetadata(format!(
            "Could not find key:{FULL_TEXT_INDEX_KEY} in key_value_metadata",
        )))?;
    let full_text_index_offset = kv
        .value
        .as_deref()
        .ok_or_else(|| ParquetError::General("Missing index offset".into()))?
        .parse::<u64>()
        .map_err(|e| ParquetError::General(e.to_string()))?;
    info!("Index {FULL_TEXT_INDEX_KEY} offset: {full_text_index_offset}");

    // +------------------------------------------+
    // | Read Full-Text Index Embedded In Parquet |
    // +------------------------------------------+

    file.seek(SeekFrom::Start(full_text_index_offset))?;
    let index_header = Header::from_reader(&file)?;

    let mut data_block_buffer = vec![0u8; index_header.total_data_block_size as usize];
    file.read_exact(&mut data_block_buffer)?;
    let index_data = DataBlock::new(data_block_buffer);

    let index_dir = ReadOnlyArchiveDirectory::new(index_header, index_data);

    let read_only_index = Index::open_or_create(index_dir, schema.as_ref().clone())?;
    let index_wrapper = ImmutableIndex::new(read_only_index);

    let query_session = QuerySession::new(&index_wrapper)?;
    let doc_mapper = DocMapper::new(query_session.searcher(), &config, original_docs);

    info!(">>> Querying full-text index embedded within Parquet");
    run_search_queries(&query_session, &doc_mapper)?;

    let provider = Arc::new(FullTextIndex::try_open(
        &saved_path,
        schema.clone(),
        arrow_schema_ref.clone(),
    )?);
    let ctx = SessionContext::new();
    ctx.register_table("t", provider)?;

    let sql = "SELECT * FROM t where title LIKE '%dairy cow%'";
    info!("Executing Query: {sql}");
    let df = ctx.sql(sql).await?;
    let result = df.to_string().await?;

    info!("\n{result}");

    let explain_query = format!("EXPLAIN FORMAT TREE {sql}");
    let df = ctx.sql(&explain_query).await?;
    let result = df.to_string().await?;

    info!("\n{result}");

    Ok(())
}

/// Initializes the logger.
fn setup_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
}
