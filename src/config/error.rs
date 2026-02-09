use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum ConfigError {
    #[error("Configuration loading failed: {0}")]
    LoadError(#[from] Box<figment::Error>),

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
