use serde::Serialize;

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
    controllers: ControllersConfig,
    managers: ManagersConfig,
}

#[derive(Serialize)]
struct HttpApiConfig {
    port: usize,
}

#[derive(Serialize)]
struct ControllersConfig {
    http_api: HttpApiConfig,
}

#[derive(Serialize)]
struct ManagersConfig {
    network: NetworkConfig,
    repository: RepositoryConfig,
    blockchain: BlockchainConfigEntries,
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
struct BlockchainConfigEntries {
    hardhat1_31337: HardhatConfig,
}

#[derive(Serialize)]
struct HardhatConfig {
    enabled: bool,
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

pub(crate) fn render_rust_config(ctx: &RustConfigContext) -> String {
    let config = RustConfig {
        environment: ctx.environment.clone(),
        app_data_path: ctx.app_data_path.clone(),
        controllers: ControllersConfig {
            http_api: HttpApiConfig {
                port: ctx.http_port,
            },
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
            blockchain: BlockchainConfigEntries {
                hardhat1_31337: HardhatConfig {
                    enabled: true,
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
            },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_nested_controller_config() {
        let toml = render_rust_config(&RustConfigContext {
            environment: "development".to_string(),
            http_port: 8901,
            network_port: 9101,
            app_data_path: "data1".to_string(),
            bootstrap_node: "/ip4/127.0.0.1/tcp/9100/p2p/peer".to_string(),
            database_name: "operationaldb1".to_string(),
            blockchain_id: "hardhat1:31337".to_string(),
            node_name: "LocalNode1".to_string(),
            operational_wallet_public: "0x1".to_string(),
            operational_wallet_private: "0x2".to_string(),
            management_wallet_public: "0x3".to_string(),
            management_wallet_private: "0x4".to_string(),
        });

        let parsed: toml::Value = toml.parse().expect("Generated TOML should parse");
        assert!(parsed.get("controllers").is_some());
        assert!(parsed.get("http_api").is_none());

        let http_port = parsed
            .get("controllers")
            .and_then(|v| v.get("http_api"))
            .and_then(|v| v.get("port"))
            .and_then(|v| v.as_integer())
            .expect("controllers.http_api.port should be present");
        assert_eq!(http_port, 8901);

        assert!(
            parsed
                .get("managers")
                .and_then(|v| v.get("blockchain"))
                .and_then(|v| v.get("hardhat1_31337"))
                .is_some(),
            "managers.blockchain.hardhat1_31337 should be present"
        );
    }
}
