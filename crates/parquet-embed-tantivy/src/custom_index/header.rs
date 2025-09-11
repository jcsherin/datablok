use crate::custom_index::file_metadata::FileMetadata;
use crc32fast::Hasher;
use std::io::Read;

#[derive(Debug, PartialEq, Clone)]
pub struct Header {
    version: u8,
    file_count: u32,
    pub total_data_block_size: u64,
    pub file_metadata_size: u32,
    pub file_metadata_crc32: u32,
    pub file_metadata_list: Vec<FileMetadata>,
}
pub const MAGIC_BYTES: &[u8; 4] = b"FTEP"; // Full-Text index Embedded in Parquet
pub const VERSION: u8 = 1;

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
    pub(crate) fn set_file_count(mut self, count: u32) -> Self {
        self.file_count = count;
        self
    }

    pub(crate) fn set_version(mut self, version: u8) -> Self {
        self.version = version;
        self
    }

    pub(crate) fn set_file_metadata_size(mut self, size: u32) -> Self {
        self.file_metadata_size = size;
        self
    }

    pub(crate) fn set_file_metadata_crc32(mut self, crc32: u32) -> Self {
        self.file_metadata_crc32 = crc32;
        self
    }

    pub(crate) fn set_total_data_block_size(mut self, size: u64) -> Self {
        self.total_data_block_size = size;
        self
    }

    pub(crate) fn set_file_metadata_list(mut self, list: &[FileMetadata]) -> Self {
        self.file_metadata_list = list.to_vec();
        self
    }

    pub fn from_reader<R: Read>(mut reader: R) -> crate::error::Result<Self> {
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
    type Error = crate::error::Error;

    fn try_from(bytes: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        let mut cursor = std::io::Cursor::new(bytes);
        Self::from_reader(&mut cursor)
    }
}
