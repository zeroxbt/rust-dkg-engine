use blockchain::BlockchainConfig;
use clap::Arg;
use clap::Command;
use dotenvy::dotenv;
use network::NetworkConfig;
use repository::RepositoryConfig;
use serde::Deserialize;
use serde_json::Value;
use std::env;
use std::fs;

use crate::controllers::http_api::http_api_router::HttpApiConfig;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub managers: ManagersConfig,
    pub http_api: HttpApiConfig,
}

#[derive(Debug, Deserialize)]
pub struct ManagersConfig {
    pub network: NetworkConfig,
    pub repository: RepositoryConfig,
    pub blockchain: BlockchainConfig,
}

pub fn initialize_configuration() -> Config {
    // Load the .env file
    dotenv().ok();

    // Parse CLI arguments for a custom config file
    let matches = Command::new("Your Application Name")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file"),
        )
        .get_matches();

    let config_file_path: Option<&String> = matches.get_one("config");

    // Fetch the NODE_ENV or default to "development"
    let node_env = env::var("NODE_ENV").unwrap_or_else(|_| "development".to_string());

    // Load the default configuration from config.json
    let config_data =
        fs::read_to_string("./config/config.json").expect("Failed to read the config.json");

    let config_json: Value = serde_json::from_str(&config_data).expect("Error parsing config.json");
    let mut default_config = config_json[node_env.as_str()].clone();

    if let Some(config_path) = config_file_path {
        let user_config_data =
            fs::read_to_string(config_path).expect("Failed to read the user config file");
        let user_config_json: Value =
            serde_json::from_str(&user_config_data).expect("Error parsing user config");
        deep_merge(&mut default_config, &user_config_json);
    } else if let Ok(ot_noderc_data) = fs::read_to_string(".origintrail_noderc") {
        // If user has a .origintrail_noderc file, merge it
        let ot_noderc_json: Value =
            serde_json::from_str(&ot_noderc_data).expect("Error parsing .origintrail_noderc");
        deep_merge(&mut default_config, &ot_noderc_json);
    }

    serde_json::from_value(default_config).expect("Error deserializing config")
}

fn deep_merge(a: &mut Value, b: &Value) {
    match (a, b) {
        (&mut Value::Object(ref mut a), Value::Object(b)) => {
            for (k, v) in b {
                deep_merge(a.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (a, b) => {
            *a = b.clone();
        }
    }
}
