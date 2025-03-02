mod databento_async;
mod errors;
mod utils;

use log::{error, info};
use utils::*;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match setup_logging() {
        Ok(_) => info!("Initialize the logger."),
        Err(e) => error!("Error: {}", e),
    }

    info!("Application starts.");
    match databento_async::download_historical_ohlcv().await {
        Ok(_) => info!("Download historical ohlcv job finished."),
        Err(e) => error!("Download historical ohlcv job failed: {}", e),
    }

    Ok(())
}
