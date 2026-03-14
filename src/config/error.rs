use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Configuration loading failed: {0}")]
    LoadError(#[source] Box<figment::Error>),

    #[error("Missing required secret: {0}")]
    MissingSecret(String),

    #[error("Missing required config file: {0}")]
    MissingConfig(String),

    #[error("Missing required environment setting: {0}")]
    MissingEnvironment(String),

    #[error("Unknown environment: {0}")]
    UnknownEnvironment(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

impl From<figment::Error> for ConfigError {
    fn from(value: figment::Error) -> Self {
        Self::LoadError(Box::new(value))
    }
}

impl From<dkg_blockchain::ConfigError> for ConfigError {
    fn from(value: dkg_blockchain::ConfigError) -> Self {
        match value {
            dkg_blockchain::ConfigError::MissingSecret(message) => Self::MissingSecret(message),
            dkg_blockchain::ConfigError::InvalidConfig(message) => Self::InvalidConfig(message),
        }
    }
}

impl From<dkg_repository::ConfigError> for ConfigError {
    fn from(value: dkg_repository::ConfigError) -> Self {
        match value {
            dkg_repository::ConfigError::MissingSecret(message) => Self::MissingSecret(message),
            dkg_repository::ConfigError::InvalidConfig(message) => Self::InvalidConfig(message),
        }
    }
}
