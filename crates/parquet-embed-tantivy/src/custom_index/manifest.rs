use crate::custom_index::data_block::DataBlock;
use crate::custom_index::file_metadata::FileMetadata;
use crate::custom_index::file_slice::LogicalFileSlice;
use crate::custom_index::header::{Header, MAGIC_BYTES, VERSION};
use crate::index::TantivyDocIndex;
use log::trace;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;

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
    pub fn try_new(index: &TantivyDocIndex) -> crate::error::Result<Self> {
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
            .collect::<crate::error::Result<Vec<_>>>()?;

        inner.iter().for_each(|draft| trace!("{draft:?}"));

        Ok(Self(inner))
    }

    pub fn try_into(
        mut self,
        index: &TantivyDocIndex,
    ) -> crate::error::Result<(Header, DataBlock)> {
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
