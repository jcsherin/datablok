use crate::custom_index::data_block::DataBlock;
use crate::custom_index::header::Header;
use crate::error::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use log::trace;
use parquet::arrow::ArrowWriter;
use parquet::data_type::AsBytes;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::ops::Deref;
use std::path::PathBuf;

pub struct ParquetWriter {
    path: PathBuf,
    writer: ArrowWriter<File>,
}

impl ParquetWriter {
    pub fn try_new(
        path: PathBuf,
        arrow_schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        let file = File::create(&path)?;
        let arrow_writer = ArrowWriter::try_new(file, arrow_schema.clone(), props)?;

        Ok(Self {
            path,
            writer: arrow_writer,
        })
    }

    pub fn write_record_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.close()?;
        trace!("Wrote Parquet file to: {}", self.path.display());

        Ok(())
    }

    pub fn write_index_and_close(mut self, header: Header, data_block: DataBlock) -> Result<()> {
        self.writer.flush()?;

        let offset = self.writer.bytes_written();

        let header_bytes: Vec<u8> = header.clone().into();
        self.writer.write_all(header_bytes.as_bytes())?;
        self.writer.write_all(data_block.deref())?;

        trace!("Index will be written to offset: {offset}");
        trace!(
            "Index size: {} bytes",
            header_bytes.len() + data_block.len()
        );

        // Store the full-text index offset in `FileMetadata.key_value_metadata` for reading it back
        // later.
        self.writer.append_key_value_metadata(KeyValue::new(
            crate::index::FULL_TEXT_INDEX_KEY.to_string(),
            offset.to_string(),
        ));

        self.writer.close()?;
        trace!("Wrote Parquet file to: {}", self.path.display());

        Ok(())
    }
}
