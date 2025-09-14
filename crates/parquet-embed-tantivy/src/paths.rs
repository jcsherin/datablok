use std::path::PathBuf;

pub enum DocFileType {
    Standard,
    WithIndex,
}
/// Generates the standard filename for a docs parquet file.
pub fn docs_parquet_filename(file_type: DocFileType, target_size: u64) -> PathBuf {
    let base_name = match file_type {
        DocFileType::Standard => "docs",
        DocFileType::WithIndex => "docs_with_fts_index",
    };

    let file_name = format!("{base_name}_{target_size}");

    let mut path = PathBuf::from(file_name);
    path.set_extension("parquet");

    path
}
