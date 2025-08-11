use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_nested_common::prelude::get_contact_schema;
use parquet_nested_parallel::pipeline::{PipelineConfig, run_pipeline};
use std::fs::File;
use std::path::{Path, PathBuf};
use tempfile::Builder;

#[test]
fn test_pipeline_produces_valid_output() {
    // Setup test environment and configuration.
    let temp_dir = Builder::new()
        .prefix("pipeline-correctness-test")
        .tempdir()
        .unwrap();

    let config = PipelineConfig {
        target_contacts: 10_000,
        num_writers: 4,
        num_producers: 4, // Use 4 producers for the test
        record_batch_size: 1024,
        output_dir: temp_dir.path().to_path_buf(),
    };

    // Run the pipeline.
    let _ = run_pipeline(&config).expect("Pipeline failed to run");

    // Verify the output is correct.
    let output_files = find_parquet_files(temp_dir.path());

    // Assert that correct number of files were created.
    assert_eq!(
        output_files.len(),
        config.num_writers,
        "Expected {} output files, but found {}",
        config.num_writers,
        output_files.len()
    );

    let mut total_rows = 0;

    let expected_schema = get_contact_schema();
    for file_path in output_files.iter() {
        let parquet_file = File::open(file_path).expect("Test failed: could not open output file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_file)
            .expect("Test failed: could not create parquet reader");

        // Assert that the schema we read back matches.
        assert_eq!(
            builder.schema(),
            &expected_schema,
            "Parquet file schema: {actual} does not match expected schema: {expected}.",
            actual = builder.schema(),
            expected = &expected_schema,
        );

        total_rows += builder.metadata().file_metadata().num_rows() as usize;
    }

    // Assert total number of rows written across all files matches target count in config.
    assert_eq!(
        total_rows,
        config.target_contacts,
        "Total rows: {total_rows} in output files do not match target contacts: {target_contacts}.",
        target_contacts = config.target_contacts,
    );

    // Teardown: The `temp_dir` is cleaned up automatically when it gets dropped here.
}

/// Helper function to find and sort Parquet files in a directory.
fn find_parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("Failed to read test output directory")
        .filter_map(|entry| {
            let entry = entry.expect("Failed to read directory entry");
            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "parquet") {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    paths.sort();
    paths
}
