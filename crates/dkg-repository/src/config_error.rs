use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Missing required secret: {0}")]
    MissingSecret(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}
