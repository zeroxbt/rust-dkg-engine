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
    cleanup::{clear_rust_node_data, create_database, delete_blazegraph_namespaces, drop_database},
    config_gen::{JsConfigContext, RustConfigContext, render_js_config, render_rust_config},
    constants::{
        BOOTSTRAP_KEY_PATH, BOOTSTRAP_NODE_INDEX, DEFAULT_JS_NODES, DEFAULT_NODES,
        HARDHAT_BLOCKCHAIN_ID, HARDHAT_PORT, HTTP_PORT_BASE, JS_BOOTSTRAP_PEER_ID,
        JS_BOOTSTRAP_PRIVATE_KEY, NETWORK_PORT_BASE,
    },
    keys::NodeKeys,
    launch::{get_binary_path, open_terminal_with_command},
    local_blockchain::LocalBlockchain,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeKind {
    Rust,
    Js,
}

struct NodePlan {
    index: usize,
    kind: NodeKind,
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

async fn bootstrap_info(js_first: bool) -> (String, bool) {
    if js_first {
        let multiaddr = format!(
            "/ip4/127.0.0.1/tcp/{}/p2p/{}",
            NETWORK_PORT_BASE + BOOTSTRAP_NODE_INDEX,
            JS_BOOTSTRAP_PEER_ID
        );
        println!("Using known JS bootstrap peer ID: {}", JS_BOOTSTRAP_PEER_ID);
        (multiaddr, true)
    } else {
        let bootstrap_data_folder = format!("data{}", BOOTSTRAP_NODE_INDEX);
        let bootstrap_peer_id = ensure_bootstrap_peer_id(&bootstrap_data_folder).await;
        let multiaddr = format!(
            "/ip4/127.0.0.1/tcp/{}/p2p/{}",
            NETWORK_PORT_BASE + BOOTSTRAP_NODE_INDEX,
            bootstrap_peer_id
        );
        (multiaddr, false)
    }
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

fn build_plan(rust_nodes: usize, js_nodes: usize, js_first: bool) -> Vec<NodePlan> {
    let total_nodes = rust_nodes + js_nodes;
    let mut plans = Vec::with_capacity(total_nodes);

    for i in 0..total_nodes {
        let is_js_node = if js_first {
            i < js_nodes
        } else {
            i >= rust_nodes
        };
        let kind = if is_js_node {
            NodeKind::Js
        } else {
            NodeKind::Rust
        };
        plans.push(NodePlan {
            index: i,
            kind,
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

fn print_plan_summary(rust_nodes: usize, js_nodes: usize, total_nodes: usize, js_first: bool) {
    println!("=== Local Network Plan ===");
    println!("Rust nodes: {}", rust_nodes);
    println!("JS nodes: {}", js_nodes);
    println!("Total nodes: {}", total_nodes);
    println!(
        "Bootstrap: {} node at index {}",
        if js_first { "JS" } else { "Rust" },
        BOOTSTRAP_NODE_INDEX
    );
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

fn cleanup_nodes(plans: &[NodePlan], js_first: bool) {
    println!("=== Cleaning up data folders and databases ===");
    for plan in plans {
        // Drop MySQL database (used by JS nodes)
        drop_database(&plan.database_name);
        // rust-dkg-engine no longer creates the database automatically on startup.
        create_database(&plan.database_name);

        // Delete Blazegraph namespaces for all nodes (JS nodes use Blazegraph)
        delete_blazegraph_namespaces(plan.index);

        match plan.kind {
            NodeKind::Js => {
                if Path::new(&plan.data_folder).exists() {
                    match fs::remove_dir_all(&plan.data_folder) {
                        Ok(_) => println!("Cleared JS node data folder: {}", plan.data_folder),
                        Err(e) => eprintln!(
                            "Failed to clear JS data folder '{}': {}",
                            plan.data_folder, e
                        ),
                    }
                }
            }
            NodeKind::Rust => {
                let is_bootstrap = !js_first && plan.index == BOOTSTRAP_NODE_INDEX;
                clear_rust_node_data(&plan.data_folder, is_bootstrap);
            }
        }
    }
    println!();
}

fn generate_configs(
    plans: &[NodePlan],
    js_first: bool,
    bootstrap_multiaddr: &str,
    bootstrap_is_js: bool,
    keys: &NodeKeys,
) {
    for plan in plans {
        fs::create_dir_all(&plan.data_folder).expect("Failed to create data folder");

        match plan.kind {
            NodeKind::Js => {
                let (bootstrap_nodes, bootstrap_private_key) =
                    if plan.index == BOOTSTRAP_NODE_INDEX && bootstrap_is_js {
                        (Vec::new(), Some(JS_BOOTSTRAP_PRIVATE_KEY.to_string()))
                    } else {
                        (vec![bootstrap_multiaddr.to_string()], None)
                    };

                let ctx = JsConfigContext {
                    http_port: plan.http_port,
                    network_port: plan.network_port,
                    app_data_path: plan.data_folder.clone(),
                    bootstrap_nodes,
                    bootstrap_private_key,
                    database_name: plan.database_name.clone(),
                    node_index: plan.index,
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

                let json_config = render_js_config(&ctx);
                let config_path = Path::new(&plan.data_folder).join(".origintrail_noderc");
                fs::write(&config_path, json_config).expect("Failed to write the JSON config file");
                println!(
                    "Generated {} (JS node{})",
                    config_path.display(),
                    if plan.index == BOOTSTRAP_NODE_INDEX && bootstrap_is_js {
                        " - BOOTSTRAP"
                    } else {
                        ""
                    }
                );
            }
            NodeKind::Rust => {
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
                    if plan.index == BOOTSTRAP_NODE_INDEX && !bootstrap_is_js {
                        " - BOOTSTRAP"
                    } else {
                        ""
                    }
                );
            }
        }
    }

    if js_first {
        println!();
    }
}

async fn launch_nodes(
    plans: &[NodePlan],
    kind: NodeKind,
    current_dir_str: &str,
    binary_path: &str,
) {
    for plan in plans.iter().filter(|plan| plan.kind == kind) {
        match plan.kind {
            NodeKind::Js => {
                let config_path = format!("{}/{}", plan.data_folder, ".origintrail_noderc");
                open_terminal_with_command(&format!(
                    "cd {}/dkg-engine && NODE_ENV=development node index.js ../{}",
                    current_dir_str, config_path
                ));
            }
            NodeKind::Rust => {
                let config_path = format!("{}/{}", plan.data_folder, "config.toml");
                open_terminal_with_command(&format!(
                    "cd {} && {} --config {}",
                    current_dir_str, binary_path, config_path
                ));
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let matches = Command::new("Local Network Launcher")
        .about("Launch a local DKG network with Rust and/or JS nodes")
        .arg(
            Arg::new("nodes")
                .short('n')
                .long("nodes")
                .value_parser(value_parser!(usize))
                .help("Number of Rust nodes in the network (default: 12)"),
        )
        .arg(
            Arg::new("js-nodes")
                .short('j')
                .long("js-nodes")
                .value_parser(value_parser!(usize))
                .help("Number of JS nodes to run (requires Blazegraph running on localhost:9999)"),
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

    let rust_nodes: usize = *matches.get_one("nodes").unwrap_or(&DEFAULT_NODES);
    let js_nodes: usize = *matches.get_one("js-nodes").unwrap_or(&DEFAULT_JS_NODES);
    let set_parameters = matches.get_flag("set-parameters");
    let use_release = matches.get_flag("release");
    let total_nodes = rust_nodes + js_nodes;

    let keys = NodeKeys::load();
    keys.validate(total_nodes);

    // When JS nodes are present, they come first (JS node 0 is bootstrap).
    // Otherwise, Rust node 0 is bootstrap.
    let js_first = js_nodes > 0;

    let plans = build_plan(rust_nodes, js_nodes, js_first);
    print_plan_summary(rust_nodes, js_nodes, total_nodes, js_first);
    warn_on_ports(total_nodes);

    if js_first {
        println!("=== Hybrid Network Mode ===");
        println!(
            "Running {} JS nodes (indices 0-{}) and {} Rust nodes (indices {}-{})",
            js_nodes,
            js_nodes - 1,
            rust_nodes,
            js_nodes,
            total_nodes - 1
        );
        println!("JS node 0 is the bootstrap node");
        println!("NOTE: JS nodes require Blazegraph running on localhost:9999");
        println!();
    }

    let (bootstrap_multiaddr, bootstrap_is_js) = bootstrap_info(js_first).await;

    cleanup_nodes(&plans, js_first);
    generate_configs(
        &plans,
        js_first,
        &bootstrap_multiaddr,
        bootstrap_is_js,
        &keys,
    );

    let current_dir_str = std::env::current_dir()
        .expect("Failed to get current directory")
        .to_str()
        .expect("Failed to convert Path to str")
        .to_string();

    // Get the pre-built binary path (will panic with instructions if not found)
    let binary_path = get_binary_path(use_release);
    println!("Using binary: {}", binary_path);

    LocalBlockchain::run(HARDHAT_PORT, set_parameters)
        .await
        .expect("failed to start local blockchain");

    if js_first {
        launch_nodes(&plans, NodeKind::Js, &current_dir_str, &binary_path).await;
        launch_nodes(&plans, NodeKind::Rust, &current_dir_str, &binary_path).await;

        println!();
        println!("=== Hybrid Network Started ===");
        println!(
            "JS nodes: 0-{} (ports {}-{})",
            js_nodes - 1,
            NETWORK_PORT_BASE,
            NETWORK_PORT_BASE + js_nodes - 1
        );
        println!(
            "Rust nodes: {}-{} (ports {}-{})",
            js_nodes,
            total_nodes - 1,
            NETWORK_PORT_BASE + js_nodes,
            NETWORK_PORT_BASE + total_nodes - 1
        );
        println!();
    } else {
        launch_nodes(&plans, NodeKind::Rust, &current_dir_str, &binary_path).await;
        launch_nodes(&plans, NodeKind::Js, &current_dir_str, &binary_path).await;

        if js_nodes > 0 {
            println!();
            println!("=== Hybrid Network Started ===");
            println!(
                "Rust nodes: 0-{} (ports {}-{})",
                rust_nodes - 1,
                NETWORK_PORT_BASE,
                NETWORK_PORT_BASE + rust_nodes - 1
            );
            println!(
                "JS nodes: {}-{} (ports {}-{})",
                rust_nodes,
                total_nodes - 1,
                NETWORK_PORT_BASE + rust_nodes,
                NETWORK_PORT_BASE + total_nodes - 1
            );
            println!();
        }
    }
}
