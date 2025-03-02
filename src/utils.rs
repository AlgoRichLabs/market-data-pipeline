use crate::errors::MarketDataError;
use log::LevelFilter;
use simplelog::{CombinedLogger, Config, WriteLogger};
use std::path::PathBuf;
use std::{fs, io};

#[derive(serde::Deserialize, Clone)]
pub struct ApiConfig {
    pub key: String,
}

#[derive(serde::Deserialize, Clone)]
pub struct DownloadConfig {
    pub dataset: String,
    pub start_time: String,
    pub end_time: String,
    pub symbol: Vec<String>,
    pub output_path: PathBuf,
    pub poll_start_time: String,
}

#[derive(serde::Deserialize, Clone)]
pub struct ConfigFile {
    pub api: ApiConfig,
    pub download_config: DownloadConfig,
}

pub fn setup_logging() -> io::Result<()> {
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

pub async fn load_config() -> Result<ConfigFile, MarketDataError> {
    let config_content = fs::read_to_string("config.toml")?;
    let config: ConfigFile = toml::from_str(&config_content)?;
    Ok(config)
}

pub fn organize_files(download_files: Vec<PathBuf>, folder: PathBuf) -> io::Result<()> {
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
