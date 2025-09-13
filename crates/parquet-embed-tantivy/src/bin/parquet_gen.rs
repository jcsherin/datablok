use clap::Parser;
use log::trace;
use parquet_embed_tantivy::common::setup_logging;
use parquet_embed_tantivy::data_generator::title::TitleGenerator;

#[derive(Parser, Debug)]
#[command(name = "parquet_gen")]
#[command(about = "Generate input parquet files")]
#[command(
    long_about = "Generate two identical parquet files, with a full-text index embedded in one of them."
)]
#[command(version)]
struct Args {
    /// Total number of rows to generate
    #[arg(short, long, default_value_t = 100)]
    target_size: u64,
}

fn main() {
    setup_logging();

    let args = Args::parse();

    for (i, title) in (0..args.target_size).zip(TitleGenerator::new()) {
        trace!("{i} --> {title}");
    }
}
