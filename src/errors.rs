use std::{fmt, io};

#[derive(Debug)]
pub enum MarketDataError {
    IoError(io::Error),
    DatabentoError(databento::Error),
    TimeParseError(time::error::Parse),
    ConfigError(toml::de::Error),
}

impl From<io::Error> for MarketDataError {
    fn from(err: io::Error) -> Self {
        MarketDataError::IoError(err)
    }
}

impl From<databento::Error> for MarketDataError {
    fn from(err: databento::Error) -> Self {
        MarketDataError::DatabentoError(err)
    }
}

impl From<time::error::Parse> for MarketDataError {
    fn from(err: time::error::Parse) -> Self {
        MarketDataError::TimeParseError(err)
    }
}

impl From<toml::de::Error> for MarketDataError {
    fn from(err: toml::de::Error) -> Self {
        MarketDataError::ConfigError(err)
    }
}

impl fmt::Display for MarketDataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarketDataError::IoError(e) => write!(f, "I/O error: {}", e),
            MarketDataError::DatabentoError(e) => write!(f, "Databento error: {}", e),
            MarketDataError::TimeParseError(e) => write!(f, "Time parse error: {}", e),
            MarketDataError::ConfigError(e) => write!(f, "Config error: {}", e),
        }
    }
}

impl std::error::Error for MarketDataError {}
