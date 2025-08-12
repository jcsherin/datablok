use log::info;
use once_cell::sync::Lazy;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_nested_common::prelude::get_contact_schema;
use parquet_nested_parallel::pipeline::{PipelineConfig, PipelineConfigBuilder, run_pipeline};
use std::fs::File;
use std::path::{Path, PathBuf};
use tempfile::{Builder, TempDir};

static INIT: Lazy<()> = Lazy::new(|| {
    let _ = env_logger::builder().is_test(true).try_init();
});

fn assert_pipeline_properties(temp_dir: &TempDir, config: &PipelineConfig) {
    let _ = run_pipeline(&config).expect("Pipeline failed to run");

    // Verify the output is correct.
    let output_files = find_parquet_files(temp_dir.path());

    // Assert that correct number of files were created.
    assert_eq!(
        output_files.len(),
        config.num_writers(),
        "Expected {} output files, but found {}",
        config.num_writers(),
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
        config.target_contacts(),
        "Total rows: {total_rows} in output files do not match target contacts: {target_contacts}.",
        target_contacts = config.target_contacts(),
    );

    // Teardown: The `temp_dir` is cleaned up automatically when it gets dropped here.
}

macro_rules! pipeline_test {
    ($name:ident, $num_writers:expr) => {
        #[test]
        fn $name() {
            Lazy::force(&INIT);
            let total_threads = rayon::current_num_threads();

            // This is to skip tests in GitHub Actions CI with less cpu cores (~4)
            if total_threads <= $num_writers {
                info!(
                    "Skipping test because num_writers ({}) >= total_threads ({})",
                    $num_writers, total_threads
                );
                return;
            }

            let temp_dir = Builder::new()
                .prefix("pipeline-correctness-test")
                .tempdir()
                .unwrap();

            let config = PipelineConfigBuilder::new()
                .with_target_contacts(10_000)
                .with_record_batch_size(1024)
                .with_output_dir(temp_dir.path().to_path_buf())
                .with_output_filename("test_output".to_string())
                .with_num_writers($num_writers)
                .try_build()
                .unwrap();

            assert_pipeline_properties(&temp_dir, &config);
        }
    };
}

pipeline_test!(test_pipeline_with_1_writer, 1);
pipeline_test!(test_pipeline_with_2_writers, 2);
pipeline_test!(test_pipeline_with_3_writers, 3);
pipeline_test!(test_pipeline_with_4_writers, 4);
pipeline_test!(test_pipeline_with_5_writers, 5);
pipeline_test!(test_pipeline_with_6_writers, 6);
pipeline_test!(test_pipeline_with_7_writers, 7);
pipeline_test!(test_pipeline_with_8_writers, 8);

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

#[test]
fn test_config_error_with_zero_producers() {
    Lazy::force(&INIT);
    // Use a number of writers guaranteed to cause a ZeroProducers error.
    let expected_num_writers = rayon::current_num_threads() + 1;
    let expected_total_threads = rayon::current_num_threads();

    let config_result = PipelineConfigBuilder::new()
        .with_num_writers(expected_num_writers)
        .try_build();

    match config_result {
        Err(
            error @ parquet_nested_parallel::pipeline::PipelineConfigError::ZeroProducers {
                total_threads,
                num_writers,
            },
        ) => {
            assert_eq!(
                total_threads, expected_total_threads,
                "Total threads in error did not match expected."
            );
            assert_eq!(
                num_writers, expected_num_writers,
                "Number of writers in error did not match expected."
            );

            let expected_error_message = format!(
                "No. of producer threads must be greater than zero. Total threads: {}. Writer threads: {}.",
                expected_total_threads, expected_num_writers
            );
            assert_eq!(
                error.to_string(),
                expected_error_message,
                "Error message did not match expected."
            );
        }
        Ok(_) => panic!("Expected ZeroProducers error, but got Ok result."),
    }
}
