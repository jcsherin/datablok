use datafusion_common::arrow::error::ArrowError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("Field '{0}' not found in schema")]
    FieldNotFound(String),

    #[error("Footer serialization to JSON failed")]
    Json(#[from] serde_json::Error),

    #[error("Error opening Tantivy index file")]
    DirRead(#[from] tantivy::directory::error::OpenReadError),

    #[error("Error streaming chunk from index data file")]
    FailedChunkRead(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
}

pub type Result<T> = std::result::Result<T, Error>;
