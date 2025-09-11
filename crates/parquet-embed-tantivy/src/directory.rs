use crate::custom_index::data_block::DataBlock;
use crate::custom_index::header::Header;
use log::trace;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    AntiCallToken, FileHandle, TerminatingWrite, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::Directory;

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

    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
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
pub struct ReadOnlyArchiveDirectory {
    inner: Arc<RwLock<InnerDirectory>>,
}

impl ReadOnlyArchiveDirectory {
    #[allow(dead_code)]
    pub fn new(header: Header, data: DataBlock) -> ReadOnlyArchiveDirectory {
        Self {
            inner: InnerDirectory::new(header, data),
        }
    }
}

impl Directory for ReadOnlyArchiveDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.inner.read().unwrap().get_file_handle(path)
    }

    fn delete(&self, _path: &Path) -> Result<(), DeleteError> {
        Ok(()) // no-op
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.read().unwrap().exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
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
