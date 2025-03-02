use crate::errors::MarketDataError;
use crate::utils::{organize_files, DownloadConfig};
use databento::dbn::{Encoding, SType, Schema};
use databento::historical::batch::{DownloadParams, JobState, ListJobsParams, SubmitJobParams};
use databento::{HistoricalClient, Symbols};
use log::{error, info};
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io};
use time::OffsetDateTime;

pub async fn submit_job(
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

pub async fn poll_job_status(
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

pub async fn process_completed_job(
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

pub async fn find_existing_job(
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
