use anyhow::Result;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Hello, DataFusion! (With Tokio and Anyhow)");

    // Create a new DataFusion session context
    let ctx = SessionContext::new();

    // Get the configuration state
    let state = ctx.state();

    println!("\nDataFusion Session Configuration:");
    println!("---------------------------------");
    println!("Batch Size: {}", state.config().batch_size());
    println!("Target Partitions: {}", state.config().target_partitions());

    let tz = state
        .config()
        .options()
        .execution
        .time_zone
        .as_deref()
        .unwrap_or("None");
    println!("Timezone: {}", tz);

    println!("\nSetup is working correctly!");

    Ok(())
}
