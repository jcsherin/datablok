use clap::Parser;
use log::{LevelFilter, info};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[clap(short, long)]
    input: String,

    #[clap(short, long)]
    output: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Info)
        .init();

    let cli = Cli::parse();

    info!("Input file: {}", cli.input);
    info!("Output file: {}", cli.output);

    // Columns to extract from input hits.parquet
    const COLUMN_NAMES: &[&str] = &[
        "WatchID",
        "Title",
        "EventTime",
        "UserID",
        "URL",
        "SearchPhrase",
    ];

    // Reading Parquet file into Arrow RecordBatch
    // https://arrow.apache.org/rust/parquet/arrow/index.html#example-reading-parquet-file-into-arrow-recordbatch
    let input_file = File::open(&cli.input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(input_file).unwrap();

    let projection_mask =
        ProjectionMask::columns(builder.parquet_schema(), COLUMN_NAMES.iter().copied());
    let reader = builder.with_projection(projection_mask).build()?;

    let mut row_count = 0;
    for batch_result in reader {
        let record_batch = batch_result?;
        row_count += record_batch.num_rows();
    }

    info!("Total rows read: {}", row_count);

    Ok(())
}
