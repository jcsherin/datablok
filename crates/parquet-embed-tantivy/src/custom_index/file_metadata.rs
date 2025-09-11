use crate::error::Error as LocalError;
use std::ffi::OsStr;
use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

#[derive(Debug, Default, PartialEq, Clone)]
pub struct FileMetadata {
    /// Offset in data section from where the file contents start
    pub data_offset: u64,
    /// Logical length of index file with the footer removed
    pub data_content_len: u64,
    /// Length of the byte serialized footer
    pub data_footer_len: u8,
    /// Length of the file path in bytes
    pub path_len: u8,
    /// File path
    pub path: PathBuf,
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

    pub fn from_cursor(cursor: &mut std::io::Cursor<Vec<u8>>) -> Result<Self, LocalError> {
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
