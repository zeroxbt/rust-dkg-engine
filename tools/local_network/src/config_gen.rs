use serde::Serialize;
use serde_json::{Value, json};

pub(crate) struct JsConfigContext {
    pub(crate) http_port: usize,
    pub(crate) network_port: usize,
    pub(crate) app_data_path: String,
    pub(crate) bootstrap_nodes: Vec<String>,
    pub(crate) bootstrap_private_key: Option<String>,
    pub(crate) database_name: String,
    pub(crate) node_index: usize,
    pub(crate) node_name: String,
    pub(crate) operational_wallet_public: String,
    pub(crate) operational_wallet_private: String,
    pub(crate) management_wallet_public: String,
    pub(crate) management_wallet_private: String,
}

pub(crate) struct RustConfigContext {
    pub(crate) environment: String,
    pub(crate) http_port: usize,
    pub(crate) network_port: usize,
    pub(crate) app_data_path: String,
    pub(crate) bootstrap_node: String,
    pub(crate) database_name: String,
    pub(crate) blockchain_id: String,
    pub(crate) node_name: String,
    pub(crate) operational_wallet_public: String,
    pub(crate) operational_wallet_private: String,
    pub(crate) management_wallet_public: String,
    pub(crate) management_wallet_private: String,
}

#[derive(Serialize)]
struct RustConfig {
    environment: String,
    app_data_path: String,
    http_api: HttpApiConfig,
    managers: ManagersConfig,
}

#[derive(Serialize)]
struct HttpApiConfig {
    port: usize,
}

#[derive(Serialize)]
struct ManagersConfig {
    network: NetworkConfig,
    repository: RepositoryConfig,
    blockchain: Vec<BlockchainEntry>,
    triple_store: TripleStoreConfig,
}

#[derive(Serialize)]
struct NetworkConfig {
    port: usize,
    bootstrap: Vec<String>,
}

#[derive(Serialize)]
struct RepositoryConfig {
    database: String,
    host: String,
    port: u16,
    user: String,
    password: String,
}

#[derive(Serialize)]
struct BlockchainEntry {
    #[serde(rename = "Hardhat")]
    hardhat: HardhatConfig,
}

#[derive(Serialize)]
struct HardhatConfig {
    blockchain_id: String,
    hub_contract_address: String,
    evm_operational_wallet_address: String,
    evm_operational_wallet_private_key: String,
    evm_management_wallet_address: String,
    evm_management_wallet_private_key: String,
    rpc_endpoints: Vec<String>,
    node_name: String,
    tx_confirmations: u64,
    tx_receipt_timeout_ms: u64,
}

#[derive(Serialize)]
struct TripleStoreConfig {
    backend: String,
    timeouts: TripleStoreTimeouts,
}

#[derive(Serialize)]
struct TripleStoreTimeouts {
    query_ms: u64,
    insert_ms: u64,
    ask_ms: u64,
}

pub(crate) fn render_js_config(ctx: &JsConfigContext) -> String {
    let mut network_config = json!({
        "port": ctx.network_port,
        "bootstrap": ctx.bootstrap_nodes,
    });

    if let Some(private_key) = &ctx.bootstrap_private_key
        && let Value::Object(ref mut map) = network_config
    {
        map.insert("privateKey".to_string(), Value::String(private_key.clone()));
    }

    let config = json!({
        "logLevel": "trace",
        "modules": {
            "httpClient": {
                "enabled": true,
                "implementation": {
                    "express-http-client": {
                        "package": "./http-client/implementation/express-http-client.js",
                        "config": { "port": ctx.http_port }
                    }
                }
            },
            "network": {
                "enabled": true,
                "implementation": {
                    "libp2p-service": {
                        "package": "./network/implementation/libp2p-service.js",
                        "config": network_config
                    }
                }
            },
            "repository": {
                "enabled": true,
                "implementation": {
                    "sequelize-repository": {
                        "package": "./repository/implementation/sequelize/sequelize-repository.js",
                        "config": { "database": ctx.database_name }
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
                                    "evmAddress": ctx.operational_wallet_public,
                                    "privateKey": ctx.operational_wallet_private
                                }
                            ],
                            "evmManagementWalletPublicKey": ctx.management_wallet_public,
                            "evmManagementWalletPrivateKey": ctx.management_wallet_private,
                            "nodeName": ctx.node_name,
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
                                    "name": format!("dkg-{}", ctx.node_index),
                                    "username": "admin",
                                    "password": ""
                                },
                                "privateCurrent": {
                                    "url": "http://localhost:9999",
                                    "name": format!("private-current-{}", ctx.node_index),
                                    "username": "admin",
                                    "password": ""
                                },
                                "publicCurrent": {
                                    "url": "http://localhost:9999",
                                    "name": format!("public-current-{}", ctx.node_index),
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
        "appDataPath": ctx.app_data_path
    });

    serde_json::to_string_pretty(&config).expect("Failed to serialize JS config")
}

pub(crate) fn render_rust_config(ctx: &RustConfigContext) -> String {
    let config = RustConfig {
        environment: ctx.environment.clone(),
        app_data_path: ctx.app_data_path.clone(),
        http_api: HttpApiConfig {
            port: ctx.http_port,
        },
        managers: ManagersConfig {
            network: NetworkConfig {
                port: ctx.network_port,
                bootstrap: vec![ctx.bootstrap_node.clone()],
            },
            repository: RepositoryConfig {
                database: ctx.database_name.clone(),
                host: "localhost".to_string(),
                port: 3306,
                user: "root".to_string(),
                password: "".to_string(),
            },
            blockchain: vec![BlockchainEntry {
                hardhat: HardhatConfig {
                    blockchain_id: ctx.blockchain_id.clone(),
                    hub_contract_address: "0x5FbDB2315678afecb367f032d93F642f64180aa3".to_string(),
                    evm_operational_wallet_address: ctx.operational_wallet_public.clone(),
                    evm_operational_wallet_private_key: ctx.operational_wallet_private.clone(),
                    evm_management_wallet_address: ctx.management_wallet_public.clone(),
                    evm_management_wallet_private_key: ctx.management_wallet_private.clone(),
                    rpc_endpoints: vec![
                        "ws://localhost:8545".to_string(),
                        "http://localhost:8545".to_string(),
                    ],
                    node_name: ctx.node_name.clone(),
                    tx_confirmations: 1,
                    tx_receipt_timeout_ms: 300_000,
                },
            }],
            triple_store: TripleStoreConfig {
                backend: "oxigraph".to_string(),
                timeouts: TripleStoreTimeouts {
                    query_ms: 60_000,
                    insert_ms: 300_000,
                    ask_ms: 10_000,
                },
            },
        },
    };

    toml::to_string_pretty(&config).expect("Failed to serialize Rust config")
}
