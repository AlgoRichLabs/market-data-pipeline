pub mod errors;

use databento::{
    dbn::{Encoding, SType, Schema},
    historical::batch::{DownloadParams, JobState, ListJobsParams, SubmitJobParams},
    HistoricalClient, Symbols,
};
use errors::MarketDataError;
use log::{error, info};
use simplelog::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{fs, io, path::PathBuf, time::Duration, vec};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::mpsc;

#[derive(serde::Deserialize, Clone)]
struct ApiConfig {
    key: String,
}

#[derive(serde::Deserialize, Clone)]
struct DownloadConfig {
    dataset: String,
    start_time: String,
    end_time: String,
    symbol: Vec<String>,
    output_path: PathBuf,
    poll_start_time: String,
}

#[derive(serde::Deserialize, Clone)]
struct ConfigFile {
    api: ApiConfig,
    download_config: DownloadConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logging().await?;
    info!("Application starts.");
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
                // let api_key = config.api.key.clone();
                // let output_path = config.download_config.output_path.clone();
                // let client = create_client(&api_key).unwrap();
                // process_completed_job(client, &job_id, &symbol, output_path.clone()).await?;
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

async fn setup_logging() -> io::Result<()> {
    let log_file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open("app.log")?;

    CombinedLogger::init(vec![WriteLogger::new(
        LevelFilter::Info,
        Config::default(),
        log_file,
    )])
    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

async fn load_config() -> Result<ConfigFile, MarketDataError> {
    let config_content = fs::read_to_string("config.toml")?;
    let config: ConfigFile = toml::from_str(&config_content)?;
    Ok(config)
}

fn create_client(api_key: &str) -> Result<HistoricalClient, databento::Error> {
    HistoricalClient::builder()
        .key(api_key)
        .expect("Invalid API key")
        .build()
}

async fn submit_job(
    mut client: HistoricalClient,
    config: &DownloadConfig,
    symbol: &str,
    start_time: OffsetDateTime,
    end_time: OffsetDateTime,
) -> Result<String, databento::Error> {
    let params = SubmitJobParams::builder()
        .dataset(&config.dataset)
        .date_time_range((start_time, end_time))
        .symbols(vec![symbol.to_string()])
        .stype_in(SType::RawSymbol)
        .schema(Schema::Ohlcv1H)
        .encoding(Encoding::Csv)
        .pretty_px(true)
        .pretty_ts(true)
        .build();

    let job = client.batch().submit_job(&params).await?;
    info!("Submitted job {} for {}", job.id, symbol);
    Ok(job.id)
}

async fn poll_job_status(
    mut client: HistoricalClient,
    job_id: &str,
    poll_start_time: OffsetDateTime,
) -> Result<(), io::Error> {
    loop {
        let list_job_params = ListJobsParams::builder()
            .states(vec![
                JobState::Done,
                JobState::Expired,
                JobState::Processing,
                JobState::Received,
                JobState::Queued,
            ])
            .since(poll_start_time)
            .build();

        let job_ids = client
            .batch()
            .list_jobs(&list_job_params)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        if let Some(job) = job_ids.iter().find(|job| job.id == job_id) {
            match job.state {
                JobState::Done => {
                    info!("Job: {} is Done.", job_id);
                    return Ok(());
                }
                JobState::Received | JobState::Queued | JobState::Processing => {
                    info!("Job {} is {:?}", job_id, job.state);
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                _ => {
                    error!("Job {} expired.", job_id);
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Job {} was expired", job_id),
                    ));
                }
            };
        } else {
            info!("No job found.");
        }
    }
}

async fn process_completed_job(
    client: HistoricalClient,
    job_id: &str,
    symbol: &str,
    output_path: PathBuf,
) -> Result<(), MarketDataError> {
    let target_dir = output_path.clone().join(symbol);
    if target_dir.exists() {
        info!(
            "Directory already exists for {}. Skipping downloading.",
            symbol
        );
        return Ok(());
    }

    let params = DownloadParams::builder()
        .output_dir(output_path.clone())
        .job_id(job_id.to_owned())
        .build();

    match client.clone().batch().download(&params).await {
        Ok(files) => {
            let target_dir = output_path.clone().join(symbol);
            organize_files(files, target_dir)?;
            fs::remove_dir_all(output_path.join(job_id))?;
            info!("Finish downloading symbol: {}.", symbol);
            Ok(())
        }
        Err(e) => {
            error!(
                "Download failed for job {} symbol {}: {}. Need to resubmit job later.",
                job_id, symbol, e
            );
            Ok(())
        }
    }
}

async fn find_existing_job(
    mut client: HistoricalClient,
    symbol: &str,
    poll_start_time: OffsetDateTime,
) -> Result<Option<String>, MarketDataError> {
    let list_job_params = ListJobsParams::builder().since(poll_start_time).build();

    let jobs = client.batch().list_jobs(&list_job_params).await?;

    let mut valid_jobs = Vec::new();
    for job in jobs {
        if job.state == JobState::Expired {
            continue;
        }

        let matches_symbol = match &job.symbols {
            Symbols::All => true, // Job includes all symbols
            Symbols::Symbols(symbols_vec) => symbols_vec.contains(&symbol.to_string()),
            _ => false,
        };

        if matches_symbol {
            valid_jobs.push((job.ts_received, job.id));
        }
    }
    valid_jobs.sort_by(|a, b| b.0.cmp(&a.0));

    Ok(valid_jobs.get(0).map(|(_, id)| id.clone()))
}

fn organize_files(download_files: Vec<PathBuf>, folder: PathBuf) -> io::Result<()> {
    if !folder.exists() {
        fs::create_dir_all(&folder)?;
    }

    for file in download_files {
        if let Some(file_name_os) = file.file_name() {
            let file_name = file_name_os.to_string_lossy();
            if file_name.ends_with(".csv.zst") || file_name.contains("metadata") {
                let destination = folder.join(&*file_name);
                fs::rename(&file, destination)?;
            }
        }
    }
    Ok(())
}
