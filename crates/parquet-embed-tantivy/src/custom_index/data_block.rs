use stable_deref_trait::StableDeref;
use std::fmt::{Debug, Formatter};
use std::io::{Error, ErrorKind};
use std::ops::{Deref, Range};
use std::sync::Arc;
use tantivy::directory::{FileHandle, OwnedBytes};

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
    pub fn new(data: Vec<u8>) -> Self {
        let range = 0..data.len();
        Self {
            data: Arc::from(data),
            range,
        }
    }

    pub fn slice_from(&self, range: Range<usize>) -> DataBlock {
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
