mod local_blockchain;
use std::{fs, path::Path};

use clap::{Arg, ArgAction, Command, value_parser};
use libp2p::{PeerId, identity};

const DEFAULT_NODES: usize = 12;
const HTTP_PORT_BASE: usize = 8900;
const NETWORK_PORT_BASE: usize = 9100;
const HARDHAT_PORT: u16 = 8545;
const BOOTSTRAP_NODE_INDEX: usize = 0;
const BOOTSTRAP_KEY_FILENAME: &str = "private_key";

const PRIVATE_KEYS_PATH: &str = "./tools/local_network/src/private_keys.json";
const PUBLIC_KEYS_PATH: &str = "./tools/local_network/src/public_keys.json";
const MANAGEMENT_PRIVATE_KEYS_PATH: &str =
    "./dkg-engine/test/bdd/steps/api/datasets/privateKeys-management-wallets.json";
const MANAGEMENT_PUBLIC_KEYS_PATH: &str =
    "./dkg-engine/test/bdd/steps/api/datasets/publicKeys-management-wallets.json";

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

fn load_keys(path: &str) -> Vec<String> {
    serde_json::from_str(&fs::read_to_string(path).expect("Failed to read key file"))
        .expect("Failed to parse key file")
}

fn ensure_bootstrap_peer_id(data_folder: &str) -> PeerId {
    let key_path = Path::new(data_folder).join(BOOTSTRAP_KEY_FILENAME);

    let keypair = if key_path.exists() {
        let mut key_bytes = fs::read(&key_path).expect("Failed to read bootstrap key file");
        let ed25519_keypair = identity::ed25519::Keypair::try_from_bytes(&mut key_bytes)
            .expect("Failed to parse bootstrap key file");
        ed25519_keypair.into()
    } else {
        fs::create_dir_all(data_folder).expect("Failed to create bootstrap data folder");
        let keypair = identity::Keypair::generate_ed25519();
        let ed25519_keypair = keypair
            .clone()
            .try_into_ed25519()
            .expect("Failed to convert bootstrap keypair");
        fs::write(&key_path, ed25519_keypair.to_bytes())
            .expect("Failed to write bootstrap key file");
        keypair
    };

    PeerId::from(keypair.public())
}

#[tokio::main]
async fn main() {
    let private_keys = load_keys(PRIVATE_KEYS_PATH);
    let public_keys = load_keys(PUBLIC_KEYS_PATH);
    let management_private_keys = load_keys(MANAGEMENT_PRIVATE_KEYS_PATH);
    let management_public_keys = load_keys(MANAGEMENT_PUBLIC_KEYS_PATH);

    let matches = Command::new("Your Application Name")
        .arg(
            Arg::new("nodes")
                .short('n')
                .long("nodes")
                .value_parser(value_parser!(usize))
                .help("Number of nodes in the network"),
        )
        .arg(
            Arg::new("set-parameters")
                .long("set-parameters")
                .action(ArgAction::SetTrue)
                .help("Apply test ParametersStorage values after blockchain startup"),
        )
        .get_matches();

    let nodes: usize = *matches.get_one("nodes").unwrap_or(&DEFAULT_NODES);
    let set_parameters = matches.get_flag("set-parameters");

    let bootstrap_data_folder = format!("data{}", BOOTSTRAP_NODE_INDEX);
    let bootstrap_peer_id = ensure_bootstrap_peer_id(&bootstrap_data_folder);
    let bootstrap_multiaddr = format!(
        "/ip4/127.0.0.1/tcp/{}/p2p/{}",
        NETWORK_PORT_BASE + BOOTSTRAP_NODE_INDEX,
        bootstrap_peer_id
    );

    // Read TOML template
    let toml_template_path = "tools/local_network/.node_config_template.toml";
    let toml_template_str =
        fs::read_to_string(toml_template_path).expect("Failed to read the TOML template file");

    for i in 0..nodes {
        // Calculate ports and names
        let network_port = NETWORK_PORT_BASE + i;
        let http_port = HTTP_PORT_BASE + i;
        let database_name = format!("operationaldb{}", i);
        let data_folder = format!("data{}", i);
        let node_name = format!("LocalNode{}", i + 1);
        let node_symbol = format!("LN{}", i + 1);
        let key_index = i + 1;

        // Generate TOML config by replacing placeholders
        let toml_config = toml_template_str
            .replace("{{HTTP_PORT}}", &http_port.to_string())
            .replace("{{NETWORK_PORT}}", &network_port.to_string())
            .replace("{{DATA_FOLDER}}", &data_folder)
            .replace("{{APP_DATA_PATH}}", &data_folder)
            .replace("{{BOOTSTRAP_NODE}}", &bootstrap_multiaddr)
            .replace("{{DATABASE_NAME}}", &database_name)
            .replace(
                "{{OPERATIONAL_WALLET_PUBLIC}}",
                public_keys
                    .get(key_index)
                    .expect("Not enough operational public keys"),
            )
            .replace(
                "{{OPERATIONAL_WALLET_PRIVATE}}",
                private_keys
                    .get(key_index)
                    .expect("Not enough operational private keys"),
            )
            .replace(
                "{{MANAGEMENT_WALLET_PUBLIC}}",
                management_public_keys
                    .get(key_index)
                    .expect("Not enough management public keys"),
            )
            .replace(
                "{{MANAGEMENT_WALLET_PRIVATE}}",
                management_private_keys
                    .get(key_index)
                    .expect("Not enough management private keys"),
            )
            .replace("{{NODE_NAME}}", &node_name)
            .replace("{{NODE_SYMBOL}}", &node_symbol);

        let toml_file_name = format!("tools/local_network/.node{}_config.toml", i);
        fs::write(&toml_file_name, toml_config).expect("Failed to write the TOML config file");
        println!("Generated {}", toml_file_name);
    }

    let current_dir = std::env::current_dir().expect("Failed to get current directory");
    let current_dir_str = current_dir.to_str().expect("Failed to convert Path to str");

    local_blockchain::LocalBlockchain::run(HARDHAT_PORT, set_parameters)
        .await
        .unwrap();

    for i in 0..nodes {
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
