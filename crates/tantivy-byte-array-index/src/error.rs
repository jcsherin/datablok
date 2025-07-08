use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("Field '{0}' not found in schema")]
    FieldNotFound(String),
}

pub type Result<T> = std::result::Result<T, Error>;
