use anyhow::Result;
use datafusion::prelude::*;
use log::info;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("Hello, DataFusion!\n");

    // Create a new DataFusion session context
    let ctx = SessionContext::new();

    // Get the configuration state
    let state = ctx.state();

    info!("DataFusion Session Configuration:");
    info!("---------------------------------");
    info!("Batch Size: {}", state.config().batch_size());
    info!("Target Partitions: {}", state.config().target_partitions());

    let tz = state
        .config()
        .options()
        .execution
        .time_zone
        .as_deref()
        .unwrap_or("None");
    info!("Timezone: {}\n", tz);

    info!("Setup is working correctly!\n");

    Ok(())
}
