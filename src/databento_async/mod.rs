use crate::databento_async::job_requests::{
    find_existing_job, poll_job_status, process_completed_job, submit_job,
};
use crate::errors::MarketDataError;
use crate::utils::load_config;
use databento::HistoricalClient;
use log::{error, info};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::mpsc;

pub mod job_requests;

pub fn create_client(api_key: &str) -> Result<HistoricalClient, databento::Error> {
    HistoricalClient::builder()
        .key(api_key)
        .expect("Invalid API key")
        .build()
}

pub async fn download_historical_ohlcv() -> Result<(), Box<dyn std::error::Error>> {
    let max_jobs = 10;
    let active_jobs = Arc::new(AtomicUsize::new(0));
    let config = load_config().await?;
    let poll_start_time = OffsetDateTime::parse(&config.download_config.poll_start_time, &Rfc3339)?;
    let start_time = OffsetDateTime::parse(&config.download_config.start_time, &Rfc3339)?;
    let end_time = OffsetDateTime::parse(&config.download_config.end_time, &Rfc3339)?;

    // Set up channels for job processing
    let (submission_tx, mut submission_rx) = mpsc::channel(32);
    let (finished_tx, mut finished_rx) = mpsc::channel(32);

    let submission_handle = tokio::spawn({
        let config = config.clone();
        let active_jobs = active_jobs.clone();
        async move {
            for symbol in &config.download_config.symbol {
                while active_jobs.load(Ordering::SeqCst) >= max_jobs {
                    info!("Reach the active jobs cap: {}.", max_jobs);
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                let client = create_client(&config.api.key)?;

                if let Some(job_id) =
                    find_existing_job(client.clone(), &symbol, poll_start_time.clone()).await?
                {
                    info!("Reusing existing job {} for  {}", job_id, &symbol);
                    active_jobs.fetch_add(1, Ordering::SeqCst);
                    info!("Active jobs: {}", active_jobs.load(Ordering::SeqCst));
                    submission_tx
                        .send((job_id, symbol.clone()))
                        .await
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                "Submission channel send reusing job error",
                            )
                        })?;
                    continue;
                }

                let job_id = submit_job(
                    client,
                    &config.download_config,
                    symbol,
                    start_time,
                    end_time,
                )
                .await?;

                active_jobs.fetch_add(1, Ordering::SeqCst);
                info!("Active jobs: {}", active_jobs.load(Ordering::SeqCst));

                submission_tx
                    .send((job_id, symbol.clone()))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "Submission channel send error")
                    })?;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Ok::<_, MarketDataError>(())
        }
    });

    let polling_handle = tokio::spawn({
        let api_key = config.api.key.clone();
        async move {
            while let Some((job_id, symbol)) = submission_rx.recv().await {
                let api_key = api_key.clone();
                let finished_tx = finished_tx.clone();
                let poll_start_time = poll_start_time.clone();
                // This is interesting. If I don't create a separate task, it will block the program here.
                tokio::spawn(async move {
                    let client = create_client(&api_key).unwrap();
                    poll_job_status(client, &job_id, poll_start_time).await?;
                    finished_tx.send((job_id, symbol)).await.map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "Polling channel send error")
                    })
                });
            }
            Ok::<_, MarketDataError>(())
        }
    });

    let download_handle = tokio::spawn({
        let config = config.clone();
        let active_jobs = active_jobs.clone();
        async move {
            while let Some((job_id, symbol)) = finished_rx.recv().await {
                let api_key = config.api.key.clone();
                let output_path = config.download_config.output_path.clone();

                // Spawn a new task for each download and properly handle its result
                let job_id_clone = job_id.clone();
                let symbol_clone = symbol.clone();
                let download_task = tokio::spawn(async move {
                    let client = create_client(&api_key).unwrap();
                    process_completed_job(client, &job_id, &symbol, output_path.clone()).await
                });

                // Wait for the download task to complete and handle any errors
                match download_task.await {
                    Ok(Ok(())) => {
                        active_jobs.fetch_sub(1, Ordering::SeqCst);
                        info!(
                            "Successfully processed job {} for symbol {}. Active jobs: {}.",
                            job_id_clone,
                            symbol_clone,
                            active_jobs.load(Ordering::SeqCst)
                        );
                    }
                    Ok(Err(e)) => {
                        error!(
                            "Error processing job {} for symbol {}: {}",
                            job_id_clone, symbol_clone, e
                        );
                        // You might want to implement retry logic here
                    }
                    Err(e) => {
                        error!(
                            "Task failed for job {} symbol {}: {}",
                            job_id_clone, symbol_clone, e
                        );
                        // Handle task cancellation or panic
                    }
                }
            }
            Ok::<_, MarketDataError>(())
        }
    });

    let _ = tokio::try_join!(submission_handle, polling_handle, download_handle)?;
    Ok(())
}
