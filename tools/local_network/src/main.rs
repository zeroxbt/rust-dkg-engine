mod cleanup;
mod config_gen;
mod constants;
mod keys;
mod launch;
mod local_blockchain;

use std::{fs, net::TcpStream, path::Path, time::Duration};

use clap::{Arg, ArgAction, Command, value_parser};
use dkg_network::{KeyManager, PeerId};

use crate::{
    cleanup::{clear_rust_node_data, create_database, drop_database},
    config_gen::{RustConfigContext, render_rust_config},
    constants::{
        BOOTSTRAP_KEY_PATH, BOOTSTRAP_NODE_INDEX, DEFAULT_NODES, HARDHAT_BLOCKCHAIN_ID,
        HARDHAT_PORT, HTTP_PORT_BASE, NETWORK_PORT_BASE,
    },
    keys::NodeKeys,
    launch::{get_binary_path, open_terminal_with_command},
    local_blockchain::LocalBlockchain,
};

struct NodePlan {
    index: usize,
    network_port: usize,
    http_port: usize,
    data_folder: String,
    database_name: String,
    node_name: String,
    key_index: usize,
}

async fn ensure_bootstrap_peer_id(data_folder: &str) -> PeerId {
    let key_path = Path::new(data_folder).join(BOOTSTRAP_KEY_PATH);
    let keypair = KeyManager::load_or_generate(&key_path)
        .await
        .expect("Failed to load or generate bootstrap key");

    PeerId::from(keypair.public())
}

async fn get_bootstrap_multiaddr() -> String {
    let bootstrap_data_folder = format!("data{}", BOOTSTRAP_NODE_INDEX);
    let bootstrap_peer_id = ensure_bootstrap_peer_id(&bootstrap_data_folder).await;
    format!(
        "/ip4/127.0.0.1/tcp/{}/p2p/{}",
        NETWORK_PORT_BASE + BOOTSTRAP_NODE_INDEX,
        bootstrap_peer_id
    )
}

fn warn_if_port_in_use(port: u16, label: &str) {
    let address = format!("127.0.0.1:{port}");
    let timeout = Duration::from_millis(100);
    if TcpStream::connect_timeout(&address.parse().expect("Invalid address"), timeout).is_ok() {
        eprintln!("Warning: {label} port {port} appears to be in use");
    }
}

fn warn_on_port_range(base: usize, count: usize, label: &str) {
    for offset in 0..count {
        let port = (base + offset) as u16;
        warn_if_port_in_use(port, label);
    }
}

fn build_plan(nodes: usize) -> Vec<NodePlan> {
    let mut plans = Vec::with_capacity(nodes);

    for i in 0..nodes {
        plans.push(NodePlan {
            index: i,
            network_port: NETWORK_PORT_BASE + i,
            http_port: HTTP_PORT_BASE + i,
            database_name: format!("operationaldb{}", i),
            data_folder: format!("data{}", i),
            node_name: format!("LocalNode{}", i + 1),
            key_index: i + 1,
        });
    }

    plans
}

fn print_plan_summary(total_nodes: usize) {
    println!("=== Local Network Plan ===");
    println!("Rust nodes: {}", total_nodes);
    println!("Total nodes: {}", total_nodes);
    println!("Bootstrap: Rust node at index {}", BOOTSTRAP_NODE_INDEX);
    println!(
        "Ports: network {}-{}, http {}-{}",
        NETWORK_PORT_BASE,
        NETWORK_PORT_BASE + total_nodes.saturating_sub(1),
        HTTP_PORT_BASE,
        HTTP_PORT_BASE + total_nodes.saturating_sub(1)
    );
    println!();
}

fn warn_on_ports(total_nodes: usize) {
    warn_if_port_in_use(HARDHAT_PORT, "Hardhat");
    warn_on_port_range(NETWORK_PORT_BASE, total_nodes, "Network");
    warn_on_port_range(HTTP_PORT_BASE, total_nodes, "HTTP");
}

fn cleanup_nodes(plans: &[NodePlan]) {
    println!("=== Cleaning up data folders and databases ===");
    for plan in plans {
        drop_database(&plan.database_name);
        create_database(&plan.database_name);

        let is_bootstrap = plan.index == BOOTSTRAP_NODE_INDEX;
        clear_rust_node_data(&plan.data_folder, is_bootstrap);
    }
    println!();
}

fn generate_configs(plans: &[NodePlan], bootstrap_multiaddr: &str, keys: &NodeKeys) {
    for plan in plans {
        fs::create_dir_all(&plan.data_folder).expect("Failed to create data folder");

        let ctx = RustConfigContext {
            environment: "development".to_string(),
            http_port: plan.http_port,
            network_port: plan.network_port,
            app_data_path: plan.data_folder.clone(),
            bootstrap_node: bootstrap_multiaddr.to_string(),
            database_name: plan.database_name.clone(),
            blockchain_id: HARDHAT_BLOCKCHAIN_ID.to_string(),
            node_name: plan.node_name.clone(),
            operational_wallet_public: keys
                .public_keys
                .get(plan.key_index)
                .expect("Not enough operational public keys")
                .clone(),
            operational_wallet_private: keys
                .private_keys
                .get(plan.key_index)
                .expect("Not enough operational private keys")
                .clone(),
            management_wallet_public: keys
                .management_public_keys
                .get(plan.key_index)
                .expect("Not enough management public keys")
                .clone(),
            management_wallet_private: keys
                .management_private_keys
                .get(plan.key_index)
                .expect("Not enough management private keys")
                .clone(),
        };

        let toml_config = render_rust_config(&ctx);
        let config_path = Path::new(&plan.data_folder).join("config.toml");
        fs::write(&config_path, toml_config).expect("Failed to write the TOML config file");
        println!(
            "Generated {} (Rust node{})",
            config_path.display(),
            if plan.index == BOOTSTRAP_NODE_INDEX {
                " - BOOTSTRAP"
            } else {
                ""
            }
        );
    }
}

async fn launch_nodes(plans: &[NodePlan], current_dir_str: &str, binary_path: &str) {
    for plan in plans {
        let config_path = format!("{}/config.toml", plan.data_folder);
        open_terminal_with_command(&format!(
            "cd {} && {} --config {}",
            current_dir_str, binary_path, config_path
        ));
    }
}

#[tokio::main]
async fn main() {
    let matches = Command::new("Local Network Launcher")
        .about("Launch a local DKG network with Rust nodes")
        .arg(
            Arg::new("nodes")
                .short('n')
                .long("nodes")
                .value_parser(value_parser!(usize))
                .help("Number of Rust nodes in the network (default: 12)"),
        )
        .arg(
            Arg::new("set-parameters")
                .long("set-parameters")
                .action(ArgAction::SetTrue)
                .help("Apply test ParametersStorage values after blockchain startup"),
        )
        .arg(
            Arg::new("release")
                .long("release")
                .action(ArgAction::SetTrue)
                .help("Use release build (target/release) instead of debug (default)"),
        )
        .get_matches();

    let total_nodes: usize = *matches.get_one("nodes").unwrap_or(&DEFAULT_NODES);
    let set_parameters = matches.get_flag("set-parameters");
    let use_release = matches.get_flag("release");

    let keys = NodeKeys::load();
    keys.validate(total_nodes);

    let plans = build_plan(total_nodes);
    print_plan_summary(total_nodes);
    warn_on_ports(total_nodes);

    let bootstrap_multiaddr = get_bootstrap_multiaddr().await;

    cleanup_nodes(&plans);
    generate_configs(&plans, &bootstrap_multiaddr, &keys);

    let current_dir_str = std::env::current_dir()
        .expect("Failed to get current directory")
        .to_str()
        .expect("Failed to convert Path to str")
        .to_string();

    let binary_path = get_binary_path(use_release);
    println!("Using binary: {}", binary_path);

    LocalBlockchain::run(HARDHAT_PORT, set_parameters)
        .await
        .expect("failed to start local blockchain");

    launch_nodes(&plans, &current_dir_str, &binary_path).await;
}
