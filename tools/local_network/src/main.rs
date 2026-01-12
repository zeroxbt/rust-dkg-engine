mod local_blockchain;
use std::fs;

use clap::{Arg, Command, value_parser};
use serde_json::Value;

const PRIVATE_KEYS_PATH: &str = "./tools/local_network/src/private_keys.json";
const PUBLIC_KEYS_PATH: &str = "./tools/local_network/src/public_keys.json";

fn open_terminal_with_command(command: &str) {
    std::process::Command::new("osascript")
        .args([
            "-e",
            &format!("tell app \"Terminal\" to do script \"{}\"", command),
        ])
        .output()
        .expect("Failed to start terminal with command");
}

fn drop_database(database_name: &str) {
    let drop_command = format!(
        "echo \"DROP DATABASE IF EXISTS {}\" | mysql -u root",
        database_name
    );
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(&drop_command)
        .output()
        .expect("Failed to execute drop database command");

    if !output.status.success() {
        eprintln!("Failed to drop database '{}'.", database_name);
    }
}

#[tokio::main]
async fn main() {
    const NUM_CONFIGS: usize = 4;
    let private_keys: Vec<String> =
        serde_json::from_str(&std::fs::read_to_string(PRIVATE_KEYS_PATH).unwrap()).unwrap();
    let public_keys: Vec<String> =
        serde_json::from_str(&std::fs::read_to_string(PUBLIC_KEYS_PATH).unwrap()).unwrap();

    let matches = Command::new("Your Application Name")
        .arg(
            Arg::new("nodes")
                .short('n')
                .long("nodes")
                .value_parser(value_parser!(usize))
                .help("Number of nodes in the network"),
        )
        .get_matches();

    let nodes: usize = *matches.get_one("nodes").unwrap_or(&NUM_CONFIGS);

    // Read TOML template
    let toml_template_path = "tools/local_network/.node_config_template.toml";
    let toml_template_str =
        fs::read_to_string(toml_template_path).expect("Failed to read the TOML template file");

    for i in 0..=nodes - 1 {
        // Calculate ports and names
        let network_port = 9000 + i;
        let http_port = 8900 + i;
        let database_name = format!("operationaldb{}", i);
        let data_folder = format!("data{}", i);
        let node_name = format!("LocalNode{}", i);
        let node_symbol = format!("LN{}", i);

        // Generate TOML config by replacing placeholders
        let toml_config = toml_template_str
            .replace("{{HTTP_PORT}}", &http_port.to_string())
            .replace("{{NETWORK_PORT}}", &network_port.to_string())
            .replace("{{DATA_FOLDER}}", &data_folder)
            .replace(
                "{{BOOTSTRAP_NODES}}",
                "\"/ip4/127.0.0.1/tcp/9102/p2p/12D3KooWF1nhFmNp4F1ni6aL3EHcayULrrEBAuutsgPLVr2poadQ\"",
            )
            .replace("{{DATABASE_NAME}}", &database_name)
            .replace(
                "{{OPERATIONAL_WALLET_PUBLIC}}",
                public_keys.get(i).unwrap(),
            )
            .replace(
                "{{OPERATIONAL_WALLET_PRIVATE}}",
                private_keys.get(i).unwrap(),
            )
            .replace(
                "{{MANAGEMENT_WALLET_PUBLIC}}",
                public_keys.get(public_keys.len() - 1 - i).unwrap(),
            )
            .replace(
                "{{MANAGEMENT_WALLET_PRIVATE}}",
                private_keys.get(private_keys.len() - 1 - i).unwrap(),
            )
            .replace("{{NODE_NAME}}", &node_name)
            .replace("{{NODE_SYMBOL}}", &node_symbol);

        let toml_file_name = format!("tools/local_network/.node{}_config.toml", i);
        fs::write(&toml_file_name, toml_config).expect("Failed to write the TOML config file");
        println!("Generated {}", toml_file_name);
    }

    let current_dir = std::env::current_dir().expect("Failed to get current directory");
    let current_dir_str = current_dir.to_str().expect("Failed to convert Path to str");

    local_blockchain::LocalBlockchain::run().await.unwrap();

    for i in 0..=nodes - 1 {
        // Drop the database for this config
        let database_name = format!("operationaldb{}", i);
        drop_database(&database_name);

        let config_path = format!("tools/local_network/.node{}_config.toml", i);
        open_terminal_with_command(&format!(
            "cd {} && cargo run -- --config {}",
            current_dir_str, config_path
        ));
    }
}
