use blockchain::BlockchainManagerConfig;
use config::{Environment, File};
use network::NetworkManagerConfig;
use repository::RepositoryManagerConfig;
use serde::{Deserialize, Deserializer};
use std::env;
use validation::ValidationManagerConfig;

use crate::handlers::http_api_handler::http_api_router::HttpApiConfig;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub is_dev_env: bool,
    pub managers: ManagersConfig,
    pub http_api: HttpApiConfig,
}

#[derive(Debug, Deserialize)]
pub struct ManagersConfig {
    pub network: NetworkManagerConfig,
    pub repository: RepositoryManagerConfig,
    pub blockchain: BlockchainManagerConfig,
    pub validation: ValidationManagerConfig,
}

pub fn initialize_configuration() -> Config {
    // Start building the configuration
    let mut builder = config::Config::builder();

    // Load the default configuration from config.json
    let node_env = env::var("NODE_ENV").unwrap_or_else(|_| "development".to_string());
    let default_config_path = format!("./config/{}.json", node_env);
    builder = builder.add_source(File::with_name(&default_config_path));

    // Parse CLI arguments for a custom config file
    let matches = clap::Command::new("Your Application Name")
        .arg(
            clap::Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file"),
        )
        .get_matches();

    let config_file_path: Option<&String> = matches.get_one("config");

    if let Some(config_path) = config_file_path {
        // If user provides a custom config file, add it as a source
        builder = builder.add_source(File::with_name(config_path));
    } else if let Ok(ot_noderc_data) = std::fs::read_to_string(".origintrail_noderc") {
        // If user has a .origintrail_noderc file, add it as a source
        builder = builder.add_source(File::from_str(&ot_noderc_data, config::FileFormat::Json));
    }

    // Convert it into your Config type
    builder
        .build()
        .expect("Unable to build Config")
        .try_deserialize::<Config>()
        .expect("Error deserializing config")
}
