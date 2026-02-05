mod local_blockchain;
use std::{fs, path::Path, process::Command as StdCommand};

use clap::{Arg, ArgAction, Command, value_parser};
use libp2p::{PeerId, identity};

const BLAZEGRAPH_URL: &str = "http://localhost:9999";

const DEFAULT_NODES: usize = 12;
const DEFAULT_JS_NODES: usize = 0;
const HTTP_PORT_BASE: usize = 8900;
const NETWORK_PORT_BASE: usize = 9100;
const HARDHAT_PORT: u16 = 8545;
const HARDHAT_BLOCKCHAIN_ID: &str = "hardhat1:31337";
const BOOTSTRAP_NODE_INDEX: usize = 0;
const BOOTSTRAP_KEY_PATH: &str = "network/private_key";
const BINARY_NAME: &str = "rust-ot-node";

const PRIVATE_KEYS_PATH: &str = "./tools/local_network/src/private_keys.json";
const PUBLIC_KEYS_PATH: &str = "./tools/local_network/src/public_keys.json";
const MANAGEMENT_PRIVATE_KEYS_PATH: &str =
    "./dkg-engine/test/bdd/steps/api/datasets/privateKeys-management-wallets.json";
const MANAGEMENT_PUBLIC_KEYS_PATH: &str =
    "./dkg-engine/test/bdd/steps/api/datasets/publicKeys-management-wallets.json";

// Known bootstrap private key that generates peer ID: QmWyf3dtqJnhuCpzEDTNmNFYc5tjxTrXhGcUUmGHdg2gtj
const JS_BOOTSTRAP_PRIVATE_KEY: &str = "CAAS4QQwggJdAgEAAoGBALOYSCZsmINMpFdH8ydA9CL46fB08F3ELfb9qiIq+z4RhsFwi7lByysRnYT/NLm8jZ4RvlsSqOn2ZORJwBywYD5MCvU1TbEWGKxl5LriW85ZGepUwiTZJgZdDmoLIawkpSdmUOc1Fbnflhmj/XzAxlnl30yaa/YvKgnWtZI1/IwfAgMBAAECgYEAiZq2PWqbeI6ypIVmUr87z8f0Rt7yhIWZylMVllRkaGw5WeGHzQwSRQ+cJ5j6pw1HXMOvnEwxzAGT0C6J2fFx60C6R90TPos9W0zSU+XXLHA7AtazjlSnp6vHD+RxcoUhm1RUPeKU6OuUNcQVJu1ZOx6cAcP/I8cqL38JUOOS7XECQQDex9WUKtDnpHEHU/fl7SvCt0y2FbGgGdhq6k8nrWtBladP5SoRUFuQhCY8a20fszyiAIfxQrtpQw1iFPBpzoq1AkEAzl/s3XPGi5vFSNGLsLqbVKbvoW9RUaGN8o4rU9oZmPFL31Jo9FLA744YRer6dYE7jJMel7h9VVWsqa9oLGS8AwJALYwfv45Nbb6yGTRyr4Cg/MtrFKM00K3YEGvdSRhsoFkPfwc0ZZvPTKmoA5xXEC8eC2UeZhYlqOy7lL0BNjCzLQJBAMpvcgtwa8u6SvU5B0ueYIvTDLBQX3YxgOny5zFjeUR7PS+cyPMQ0cyql8jNzEzDLcSg85tkDx1L4wi31Pnm/j0CQFH/6MYn3r9benPm2bYSe9aoJp7y6ht2DmXmoveNbjlEbb8f7jAvYoTklJxmJCcrdbNx/iCj2BuAinPPgEmUzfQ=";
const JS_BOOTSTRAP_PEER_ID: &str = "QmWyf3dtqJnhuCpzEDTNmNFYc5tjxTrXhGcUUmGHdg2gtj";

// JS node config template (JSON format for .origintrail_noderc - required by JS implementation)
// Based on dkg-engine/tools/local-network-setup/.origintrail_noderc_template.json
const JS_NODE_CONFIG_TEMPLATE: &str = r#"{
    "logLevel": "trace",
    "modules": {
        "httpClient": {
            "enabled": true,
            "implementation": {
                "express-http-client": {
                    "package": "./http-client/implementation/express-http-client.js",
                    "config": {
                        "port": {{HTTP_PORT}}
                    }
                }
            }
        },
        "network": {
            "enabled": true,
            "implementation": {
                "libp2p-service": {
                    "package": "./network/implementation/libp2p-service.js",
                    "config": {
                        "port": {{NETWORK_PORT}},
                        "bootstrap": [{{BOOTSTRAP_NODES}}]{{PRIVATE_KEY_CONFIG}}
                    }
                }
            }
        },
        "repository": {
            "enabled": true,
            "implementation": {
                "sequelize-repository": {
                    "package": "./repository/implementation/sequelize/sequelize-repository.js",
                    "config": {
                        "database": "{{DATABASE_NAME}}"
                    }
                }
            }
        },
        "blockchain": {
            "implementation": {
                "hardhat1:31337": {
                    "enabled": true,
                    "package": "./blockchain/implementation/hardhat/hardhat-service.js",
                    "config": {
                        "hubContractAddress": "0x5FbDB2315678afecb367f032d93F642f64180aa3",
                        "rpcEndpoints": ["http://localhost:8545"],
                        "operationalWallets": [
                            {
                                "evmAddress": "{{OPERATIONAL_WALLET_PUBLIC}}",
                                "privateKey": "{{OPERATIONAL_WALLET_PRIVATE}}"
                            }
                        ],
                        "evmManagementWalletPublicKey": "{{MANAGEMENT_WALLET_PUBLIC}}",
                        "evmManagementWalletPrivateKey": "{{MANAGEMENT_WALLET_PRIVATE}}",
                        "nodeName": "{{NODE_NAME}}",
                        "initialStakeAmount": 50000,
                        "initialAskAmount": 0.2
                    }
                },
                "hardhat2:31337": {
                    "enabled": false
                }
            }
        },
        "tripleStore": {
            "enabled": true,
            "implementation": {
                "ot-blazegraph": {
                    "enabled": true,
                    "package": "./triple-store/implementation/ot-blazegraph/ot-blazegraph.js",
                    "config": {
                        "repositories": {
                            "dkg": {
                                "url": "http://localhost:9999",
                                "name": "dkg-{{NODE_INDEX}}",
                                "username": "admin",
                                "password": ""
                            },
                            "privateCurrent": {
                                "url": "http://localhost:9999",
                                "name": "private-current-{{NODE_INDEX}}",
                                "username": "admin",
                                "password": ""
                            },
                            "publicCurrent": {
                                "url": "http://localhost:9999",
                                "name": "public-current-{{NODE_INDEX}}",
                                "username": "admin",
                                "password": ""
                            }
                        }
                    }
                }
            }
        },
        "blockchainEvents": {
            "enabled": true,
            "implementation": {
                "ot-ethers": {
                    "enabled": true,
                    "package": "./blockchain-events/implementation/ot-ethers/ot-ethers.js",
                    "config": {
                        "blockchains": ["hardhat1:31337"],
                        "rpcEndpoints": {
                            "hardhat1:31337": ["http://localhost:8545"]
                        },
                        "hubContractAddress": {
                            "hardhat1:31337": "0x5FbDB2315678afecb367f032d93F642f64180aa3"
                        }
                    }
                }
            }
        }
    },
    "auth": {
        "ipWhitelist": ["::1", "127.0.0.1"]
    },
    "appDataPath": "{{APP_DATA_PATH}}"
}"#;

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

fn clear_rust_node_data(data_folder: &str, preserve_network_key: bool) {
    let data_path = Path::new(data_folder);

    if !data_path.exists() {
        return;
    }

    // If we need to preserve the network key, save it first
    let key_path = data_path.join(BOOTSTRAP_KEY_PATH);
    let saved_key = if preserve_network_key && key_path.exists() {
        fs::read(&key_path).ok()
    } else {
        None
    };

    // Remove the entire data folder
    match fs::remove_dir_all(data_path) {
        Ok(_) => println!("Cleared Rust node data folder: {}", data_path.display()),
        Err(e) => {
            eprintln!(
                "Failed to clear data folder '{}': {}",
                data_path.display(),
                e
            );
            return;
        }
    }

    // Recreate the data folder
    fs::create_dir_all(data_path).expect("Failed to recreate data folder");

    // Restore the network key if we saved it
    if let Some(key_bytes) = saved_key {
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent).expect("Failed to recreate network key directory");
        }
        fs::write(&key_path, key_bytes).expect("Failed to restore network key");
        println!("Restored network key: {}", key_path.display());
    }
}

/// Delete Blazegraph namespaces for a node.
/// Each JS node uses three namespaces: dkg-{i}, private-current-{i}, public-current-{i}
fn delete_blazegraph_namespaces(node_index: usize) {
    let namespaces = [
        format!("dkg-{}", node_index),
        format!("private-current-{}", node_index),
        format!("public-current-{}", node_index),
    ];

    for namespace in &namespaces {
        let url = format!("{}/blazegraph/namespace/{}", BLAZEGRAPH_URL, namespace);

        // Use curl to send DELETE request to Blazegraph
        let output = StdCommand::new("curl")
            .args(["-s", "-X", "DELETE", &url])
            .output();

        match output {
            Ok(result) => {
                if result.status.success() {
                    println!("Deleted Blazegraph namespace: {}", namespace);
                }
                // Silently ignore errors (namespace might not exist)
            }
            Err(_) => {
                // curl not available or other error - silently continue
            }
        }
    }
}

fn load_keys(path: &str) -> Vec<String> {
    serde_json::from_str(&fs::read_to_string(path).expect("Failed to read key file"))
        .expect("Failed to parse key file")
}

fn get_binary_path() -> String {
    let release_path = format!("./target/release/{}", BINARY_NAME);
    let debug_path = format!("./target/debug/{}", BINARY_NAME);

    if Path::new(&release_path).exists() {
        release_path
    } else if Path::new(&debug_path).exists() {
        debug_path
    } else {
        panic!(
            "Binary '{}' not found. Please run 'cargo build --release' or 'cargo build' first.",
            BINARY_NAME
        );
    }
}

fn ensure_bootstrap_peer_id(data_folder: &str) -> PeerId {
    let key_path = Path::new(data_folder).join(BOOTSTRAP_KEY_PATH);

    let keypair = if key_path.exists() {
        let mut key_bytes = fs::read(&key_path).expect("Failed to read bootstrap key file");
        let ed25519_keypair = identity::ed25519::Keypair::try_from_bytes(&mut key_bytes)
            .expect("Failed to parse bootstrap key file");
        ed25519_keypair.into()
    } else {
        // Ensure parent directories exist (data_folder/network/)
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent).expect("Failed to create bootstrap key directory");
        }
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
        .get_matches();

    let rust_nodes: usize = *matches.get_one("nodes").unwrap_or(&DEFAULT_NODES);
    let js_nodes: usize = *matches.get_one("js-nodes").unwrap_or(&DEFAULT_JS_NODES);
    let set_parameters = matches.get_flag("set-parameters");
    let total_nodes = rust_nodes + js_nodes;

    // When JS nodes are present, they come first (JS node 0 is bootstrap)
    // Otherwise, Rust node 0 is bootstrap
    let js_first = js_nodes > 0;

    if js_first {
        println!("=== Hybrid Network Mode ===");
        println!("Running {} JS nodes (indices 0-{}) and {} Rust nodes (indices {}-{})",
            js_nodes, js_nodes - 1, rust_nodes, js_nodes, total_nodes - 1);
        println!("JS node 0 is the bootstrap node");
        println!("NOTE: JS nodes require Blazegraph running on localhost:9999");
        println!();
    }

    // For JS-first mode, we use a known bootstrap private key that generates a deterministic peer ID
    let bootstrap_data_folder = format!("data{}", BOOTSTRAP_NODE_INDEX);
    let (bootstrap_multiaddr, bootstrap_is_js) = if js_first {
        // Use the known JS bootstrap peer ID from the hardcoded private key
        let multiaddr = format!(
            "/ip4/127.0.0.1/tcp/{}/p2p/{}",
            NETWORK_PORT_BASE + BOOTSTRAP_NODE_INDEX,
            JS_BOOTSTRAP_PEER_ID
        );
        println!("Using known JS bootstrap peer ID: {}", JS_BOOTSTRAP_PEER_ID);
        (multiaddr, true)
    } else {
        let bootstrap_peer_id = ensure_bootstrap_peer_id(&bootstrap_data_folder);
        let multiaddr = format!(
            "/ip4/127.0.0.1/tcp/{}/p2p/{}",
            NETWORK_PORT_BASE + BOOTSTRAP_NODE_INDEX,
            bootstrap_peer_id
        );
        (multiaddr, false)
    };

    // Read TOML template for Rust nodes
    let toml_template_path = "tools/local_network/config.template.toml";
    let toml_template_str =
        fs::read_to_string(toml_template_path).expect("Failed to read the TOML template file");

    // Clean up data folders and databases before generating configs
    println!("=== Cleaning up data folders and databases ===");
    for i in 0..total_nodes {
        let is_js_node = if js_first { i < js_nodes } else { i >= rust_nodes };
        let data_folder = format!("data{}", i);
        let database_name = format!("operationaldb{}", i);

        // Drop MySQL database (used by JS nodes)
        drop_database(&database_name);

        // Delete Blazegraph namespaces for all nodes (JS nodes use Blazegraph)
        delete_blazegraph_namespaces(i);

        if is_js_node {
            // For JS nodes, just clear the data folder entirely
            if Path::new(&data_folder).exists() {
                match fs::remove_dir_all(&data_folder) {
                    Ok(_) => println!("Cleared JS node data folder: {}", data_folder),
                    Err(e) => eprintln!("Failed to clear JS data folder '{}': {}", data_folder, e),
                }
            }
        } else {
            // For Rust nodes, preserve the bootstrap key if this is the bootstrap node
            let is_bootstrap = !js_first && i == BOOTSTRAP_NODE_INDEX;
            clear_rust_node_data(&data_folder, is_bootstrap);
        }
    }
    println!();

    // Generate configs for all nodes
    // When js_first: JS nodes at indices 0 to js_nodes-1, Rust nodes at js_nodes to total_nodes-1
    // Otherwise: Rust nodes at indices 0 to rust_nodes-1, JS nodes at rust_nodes to total_nodes-1
    for i in 0..total_nodes {
        let is_js_node = if js_first { i < js_nodes } else { i >= rust_nodes };

        // Calculate ports and names
        let network_port = NETWORK_PORT_BASE + i;
        let http_port = HTTP_PORT_BASE + i;
        let database_name = format!("operationaldb{}", i);
        let data_folder = format!("data{}", i);
        let node_name = format!("LocalNode{}", i + 1);
        let key_index = i + 1;

        // Ensure data folder exists
        fs::create_dir_all(&data_folder).expect("Failed to create data folder");

        if is_js_node {
            // For JS bootstrap node (index 0 when js_first), use empty bootstrap and add private key
            let (bootstrap_nodes, private_key_config) = if i == BOOTSTRAP_NODE_INDEX && bootstrap_is_js {
                // Bootstrap node: empty bootstrap array, include private key for deterministic peer ID
                (
                    String::new(),
                    format!(",\n                        \"privateKey\": \"{}\"", JS_BOOTSTRAP_PRIVATE_KEY)
                )
            } else {
                // Non-bootstrap node: use bootstrap multiaddr, no private key
                (
                    format!("\"{}\"", bootstrap_multiaddr),
                    String::new()
                )
            };

            // Generate JSON config for JS node
            let json_config = JS_NODE_CONFIG_TEMPLATE
                .replace("{{HTTP_PORT}}", &http_port.to_string())
                .replace("{{NETWORK_PORT}}", &network_port.to_string())
                .replace("{{APP_DATA_PATH}}", &data_folder)
                .replace("{{BOOTSTRAP_NODES}}", &bootstrap_nodes)
                .replace("{{PRIVATE_KEY_CONFIG}}", &private_key_config)
                .replace("{{DATABASE_NAME}}", &database_name)
                .replace("{{NODE_INDEX}}", &i.to_string())
                .replace("{{NODE_NAME}}", &node_name)
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
                );

            // Write .origintrail_noderc (JSON format for JS nodes)
            let config_path = Path::new(&data_folder).join(".origintrail_noderc");
            fs::write(&config_path, json_config).expect("Failed to write the JSON config file");
            println!("Generated {} (JS node{})", config_path.display(),
                if i == BOOTSTRAP_NODE_INDEX && bootstrap_is_js { " - BOOTSTRAP" } else { "" });
        } else {
            // For Rust bootstrap node (index 0 when not js_first), it can bootstrap itself
            let bootstrap_value = if i == BOOTSTRAP_NODE_INDEX && !bootstrap_is_js {
                bootstrap_multiaddr.clone() // Self-reference is fine for bootstrap
            } else {
                bootstrap_multiaddr.clone()
            };

            // Generate TOML config for Rust node
            let toml_config = toml_template_str
                .replace("{{HTTP_PORT}}", &http_port.to_string())
                .replace("{{NETWORK_PORT}}", &network_port.to_string())
                .replace("{{APP_DATA_PATH}}", &data_folder)
                .replace("{{BOOTSTRAP_NODE}}", &bootstrap_value)
                .replace("{{DATABASE_NAME}}", &database_name)
                .replace("{{BLOCKCHAIN_ID}}", HARDHAT_BLOCKCHAIN_ID)
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
                ;

            // Write config.toml to the data folder
            let config_path = Path::new(&data_folder).join("config.toml");
            fs::write(&config_path, toml_config).expect("Failed to write the TOML config file");

            println!("Generated {} (Rust node{})", config_path.display(),
                if i == BOOTSTRAP_NODE_INDEX && !bootstrap_is_js { " - BOOTSTRAP" } else { "" });
        }
    }

    let current_dir = std::env::current_dir().expect("Failed to get current directory");
    let current_dir_str = current_dir.to_str().expect("Failed to convert Path to str");

    // Get the pre-built binary path (will panic with instructions if not found)
    let binary_path = get_binary_path();
    println!("Using binary: {}", binary_path);

    local_blockchain::LocalBlockchain::run(HARDHAT_PORT, set_parameters)
        .await
        .unwrap();

    // Stagger node startups to avoid overwhelming the local blockchain with concurrent transactions
    const NODE_STARTUP_DELAY_MS: u64 = 500;

    tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;

    if js_first {
        // Start JS nodes first (JS node 0 is bootstrap)
        for i in 0..js_nodes {
            println!(
                "Waiting {}ms before starting JS node {} (index {})...",
                NODE_STARTUP_DELAY_MS, i, i
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(NODE_STARTUP_DELAY_MS)).await;

            let data_folder = format!("data{}", i);
            let config_path = format!("{}/{}", data_folder, ".origintrail_noderc");

            // Run JS node from dkg-engine directory
            // Config path needs to be relative to dkg-engine, so prepend ../
            // In development mode, JS node expects config path as plain argument (not --flag)
            open_terminal_with_command(&format!(
                "cd {}/dkg-engine && NODE_ENV=development node index.js ../{}",
                current_dir_str, config_path
            ));
        }

        // Start Rust nodes after
        for i in js_nodes..total_nodes {
            println!(
                "Waiting {}ms before starting Rust node {} (index {})...",
                NODE_STARTUP_DELAY_MS,
                i - js_nodes,
                i
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(NODE_STARTUP_DELAY_MS)).await;

            let data_folder = format!("data{}", i);

            // Config file path relative to workspace root
            let config_path = format!("{}/{}", data_folder, "config.toml");

            // Run Rust node from workspace root with --config flag
            open_terminal_with_command(&format!(
                "cd {} && {} --config {}",
                current_dir_str, binary_path, config_path
            ));
        }

        println!();
        println!("=== Hybrid Network Started ===");
        println!("JS nodes: 0-{} (ports {}-{})", js_nodes - 1, NETWORK_PORT_BASE, NETWORK_PORT_BASE + js_nodes - 1);
        println!("Rust nodes: {}-{} (ports {}-{})", js_nodes, total_nodes - 1, NETWORK_PORT_BASE + js_nodes, NETWORK_PORT_BASE + total_nodes - 1);
        println!();
    } else {
        // Start Rust nodes first (Rust node 0 is bootstrap)
        for i in 0..rust_nodes {
            println!(
                "Waiting {}ms before starting Rust node {}...",
                NODE_STARTUP_DELAY_MS, i
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(NODE_STARTUP_DELAY_MS)).await;

            let data_folder = format!("data{}", i);

            // Config file path relative to workspace root
            let config_path = format!("{}/{}", data_folder, "config.toml");

            // Run Rust node from workspace root with --config flag
            open_terminal_with_command(&format!(
                "cd {} && {} --config {}",
                current_dir_str, binary_path, config_path
            ));
        }

        // Start JS nodes after (if any)
        for i in rust_nodes..total_nodes {
            println!(
                "Waiting {}ms before starting JS node {} (index {})...",
                NODE_STARTUP_DELAY_MS,
                i - rust_nodes,
                i
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(NODE_STARTUP_DELAY_MS)).await;

            let data_folder = format!("data{}", i);
            let config_path = format!("{}/{}", data_folder, ".origintrail_noderc");

            // Run JS node from dkg-engine directory
            // Config path needs to be relative to dkg-engine, so prepend ../
            // In development mode, JS node expects config path as plain argument (not --flag)
            open_terminal_with_command(&format!(
                "cd {}/dkg-engine && NODE_ENV=development node index.js ../{}",
                current_dir_str, config_path
            ));
        }

        if js_nodes > 0 {
            println!();
            println!("=== Hybrid Network Started ===");
            println!("Rust nodes: 0-{} (ports {}-{})", rust_nodes - 1, NETWORK_PORT_BASE, NETWORK_PORT_BASE + rust_nodes - 1);
            println!("JS nodes: {}-{} (ports {}-{})", rust_nodes, total_nodes - 1, NETWORK_PORT_BASE + rust_nodes, NETWORK_PORT_BASE + total_nodes - 1);
            println!();
        }
    }
}
