//! Typed default configurations for each environment.
//!
//! This module replaces the embedded `config/defaults.toml` file with
//! compile-time type-checked Rust code. Each environment (development,
//! testnet, mainnet) gets a fully constructed [`ConfigRaw`] via [`config_for`].
//!
//! Shared defaults are factored into helper functions to reduce duplication
//! and make it easy to see what differs between environments.

use std::path::PathBuf;

use dkg_blockchain::{BlockchainConfigRaw, BlockchainManagerConfigRaw, BlockchainRaw};
use dkg_key_value_store::KeyValueStoreManagerConfig;
use dkg_network::NetworkManagerConfig;
use dkg_repository::RepositoryManagerConfigRaw;
use dkg_triple_store::{TimeoutConfig, TripleStoreBackendType, TripleStoreManagerConfig};

use super::{ConfigError, ConfigRaw};
use crate::{
    controllers::{
        http_api_controller::{
            middleware::{AuthConfig, RateLimiterConfig},
            router::HttpApiConfig,
        },
        rpc_controller::{config::RpcConfig, rate_limiter::PeerRateLimiterConfig},
    },
    logger::{LogFormat, LoggerConfig, TelemetryConfig},
    managers::ManagersConfigRaw,
    periodic_tasks::tasks::{
        cleanup::{
            CleanupConfig, FinalityAcksCleanupConfig, OperationsCleanupConfig,
            ProofChallengesCleanupConfig, PublishTmpDatasetCleanupConfig,
        },
        paranet_sync::ParanetSyncConfig,
        proving::ProvingConfig,
        sync::SyncConfig,
    },
};

/// Returns the default [`ConfigRaw`] for the given environment name.
pub(crate) fn config_for(environment: &str) -> Result<ConfigRaw, ConfigError> {
    match environment {
        "development" => Ok(development()),
        "testnet" => Ok(testnet()),
        "mainnet" => Ok(mainnet()),
        _ => Err(ConfigError::UnknownEnvironment(environment.to_string())),
    }
}

// ── Shared defaults (identical across all environments) ─────────

fn cleanup() -> CleanupConfig {
    CleanupConfig {
        enabled: true,
        interval_secs: 3600,
        operations: OperationsCleanupConfig {
            ttl_secs: 86400,
            batch_size: 50000,
        },
        publish_tmp_dataset: PublishTmpDatasetCleanupConfig {
            ttl_secs: 43200,
            batch_size: 50000,
        },
        finality_acks: FinalityAcksCleanupConfig {
            ttl_secs: 86400,
            batch_size: 50000,
        },
        proof_challenges: ProofChallengesCleanupConfig {
            ttl_secs: 604800,
            batch_size: 1000,
        },
    }
}

fn paranet_sync() -> ParanetSyncConfig {
    ParanetSyncConfig {
        enabled: false,
        interval_secs: 60,
        batch_size: 50,
        max_in_flight: 3,
        retries_limit: 3,
        retry_delay_secs: 60,
        sync_paranets: Vec::new(),
    }
}

fn proving() -> ProvingConfig {
    ProvingConfig { enabled: true }
}

fn sync() -> SyncConfig {
    SyncConfig {
        enabled: true,
        period_catching_up_secs: 0,
        period_idle_secs: 30,
        no_peers_retry_delay_secs: 5,
        max_retry_attempts: 2,
        max_new_kcs_per_contract: 1000,
        filter_batch_size: 100,
        network_fetch_batch_size: 100,
        max_assets_per_fetch_batch: 10_000,
        pipeline_channel_buffer: 6,
        retry_base_delay_secs: 5,
        retry_max_delay_secs: 300,
        retry_jitter_secs: 2,
    }
}

fn http_api() -> HttpApiConfig {
    HttpApiConfig {
        enabled: true,
        port: 8900,
        rate_limiter: RateLimiterConfig {
            enabled: true,
            time_window_seconds: 60,
            max_requests: 100,
            burst_size: None,
        },
        auth: AuthConfig {
            enabled: true,
            ip_whitelist: vec!["127.0.0.1".to_string(), "::1".to_string()],
        },
    }
}

fn rpc() -> RpcConfig {
    RpcConfig {
        rate_limiter: PeerRateLimiterConfig {
            enabled: true,
            requests_per_second: 2,
            burst_size: 5,
        },
    }
}

fn key_value_store() -> KeyValueStoreManagerConfig {
    KeyValueStoreManagerConfig {
        max_concurrent_operations: 16,
    }
}

// ── Parameterized helpers (shared structure, varying values) ────

fn triple_store(url: &str) -> TripleStoreManagerConfig {
    TripleStoreManagerConfig {
        backend: TripleStoreBackendType::Oxigraph,
        url: url.to_string(),
        username: None,
        password: None,
        connect_max_retries: 10,
        connect_retry_frequency_ms: 10000,
        timeouts: TimeoutConfig {
            query_ms: 60000,
            insert_ms: 300000,
            ask_ms: 10000,
        },
        max_concurrent_operations: 16,
    }
}

fn repository(user: &str, max_connections: u32) -> RepositoryManagerConfigRaw {
    RepositoryManagerConfigRaw {
        user: user.to_string(),
        password: None,
        database: "dkg_operationaldb".to_string(),
        host: "localhost".to_string(),
        port: 3306,
        max_connections,
        min_connections: 1,
    }
}

fn network(bootstrap: Vec<String>) -> NetworkManagerConfig {
    NetworkManagerConfig {
        port: 9000,
        bootstrap,
        external_ip: None,
        idle_connection_timeout_secs: 300,
    }
}

fn telemetry(enabled: bool) -> TelemetryConfig {
    TelemetryConfig {
        enabled,
        otlp_endpoint: "http://localhost:4317".to_string(),
        service_name: "rust-dkg-engine".to_string(),
    }
}

// ── Per-environment constructors ────────────────────────────────

fn development() -> ConfigRaw {
    ConfigRaw {
        environment: "development".to_string(),
        app_data_path: PathBuf::from("data"),
        logger: LoggerConfig {
            level: "rust_dkg_engine=trace".to_string(),
            format: LogFormat::Pretty,
        },
        telemetry: telemetry(true),
        cleanup: cleanup(),
        sync: sync(),
        paranet_sync: paranet_sync(),
        proving: proving(),
        http_api: http_api(),
        rpc: rpc(),
        managers: ManagersConfigRaw {
            network: network(vec![
                "/ip4/127.0.0.1/tcp/9102/p2p/12D3KooWF1nhFmNp4F1ni6aL3EHcayULrrEBAuutsgPLVr2poadQ"
                    .to_string(),
            ]),
            repository: repository("root", 10),
            blockchain: BlockchainManagerConfigRaw(vec![
                BlockchainRaw::Hardhat(BlockchainConfigRaw {
                    enabled: true,
                    blockchain_id: "hardhat1:31337".into(),
                    hub_contract_address: "0x5FbDB2315678afecb367f032d93F642f64180aa3".to_string(),
                    rpc_endpoints: vec!["http://localhost:8545".to_string()],
                    operator_fee: Some(0),
                    evm_operational_wallet_address: None,
                    evm_operational_wallet_private_key: None,
                    evm_management_wallet_address: None,
                    evm_management_wallet_private_key: None,
                    node_name: "LocalNode0".to_string(),
                    substrate_rpc_endpoints: None,
                    max_rpc_requests_per_second: None,
                    tx_confirmations: 1,
                    tx_receipt_timeout_ms: 300000,
                }),
                BlockchainRaw::Hardhat(BlockchainConfigRaw {
                    enabled: false,
                    blockchain_id: "hardhat2:31337".into(),
                    hub_contract_address: "0x5FbDB2315678afecb367f032d93F642f64180aa3".to_string(),
                    rpc_endpoints: vec!["http://localhost:9545".to_string()],
                    operator_fee: Some(0),
                    evm_operational_wallet_address: None,
                    evm_operational_wallet_private_key: None,
                    evm_management_wallet_address: None,
                    evm_management_wallet_private_key: None,
                    node_name: "LocalNode1".to_string(),
                    substrate_rpc_endpoints: None,
                    max_rpc_requests_per_second: None,
                    tx_confirmations: 1,
                    tx_receipt_timeout_ms: 300000,
                }),
            ]),
            triple_store: triple_store("http://localhost:9999"),
            key_value_store: key_value_store(),
        },
    }
}

fn testnet() -> ConfigRaw {
    ConfigRaw {
        environment: "testnet".to_string(),
        app_data_path: PathBuf::from("data"),
        logger: LoggerConfig {
            level: "rust_dkg_engine=info".to_string(),
            format: LogFormat::Pretty,
        },
        telemetry: telemetry(false),
        cleanup: cleanup(),
        sync: sync(),
        paranet_sync: paranet_sync(),
        proving: proving(),
        http_api: http_api(),
        rpc: rpc(),
        managers: ManagersConfigRaw {
            network: network(vec![]),
            repository: repository("root", 120),
            blockchain: BlockchainManagerConfigRaw(vec![
                BlockchainRaw::NeuroWeb(BlockchainConfigRaw {
                    enabled: false,
                    blockchain_id: "otp:20430".into(),
                    hub_contract_address: "0xe233b5b78853a62b1e11ebe88bf083e25b0a57a6".to_string(),
                    rpc_endpoints: vec![
                        "https://lofar-testnet.origin-trail.network".to_string(),
                        "https://lofar-testnet.origintrail.network".to_string(),
                    ],
                    operator_fee: Some(0),
                    evm_operational_wallet_address: None,
                    evm_operational_wallet_private_key: None,
                    evm_management_wallet_address: None,
                    evm_management_wallet_private_key: None,
                    node_name: "TestNode".to_string(),
                    substrate_rpc_endpoints: None,
                    max_rpc_requests_per_second: None,
                    tx_confirmations: 1,
                    tx_receipt_timeout_ms: 300000,
                }),
                BlockchainRaw::Gnosis(BlockchainConfigRaw {
                    enabled: false,
                    blockchain_id: "gnosis:10200".into(),
                    hub_contract_address: "0x2c08AC4B630c009F709521e56Ac385A6af70650f".to_string(),
                    rpc_endpoints: vec!["https://rpc.chiadochain.net".to_string()],
                    operator_fee: Some(0),
                    evm_operational_wallet_address: None,
                    evm_operational_wallet_private_key: None,
                    evm_management_wallet_address: None,
                    evm_management_wallet_private_key: None,
                    node_name: "TestNode".to_string(),
                    substrate_rpc_endpoints: None,
                    max_rpc_requests_per_second: None,
                    tx_confirmations: 1,
                    tx_receipt_timeout_ms: 300000,
                }),
                BlockchainRaw::Base(BlockchainConfigRaw {
                    enabled: false,
                    blockchain_id: "base:84532".into(),
                    hub_contract_address: "0xf21CE8f8b01548D97DCFb36869f1ccB0814a4e05".to_string(),
                    rpc_endpoints: vec!["https://sepolia.base.org".to_string()],
                    operator_fee: Some(0),
                    evm_operational_wallet_address: None,
                    evm_operational_wallet_private_key: None,
                    evm_management_wallet_address: None,
                    evm_management_wallet_private_key: None,
                    node_name: "TestNode".to_string(),
                    substrate_rpc_endpoints: None,
                    max_rpc_requests_per_second: None,
                    tx_confirmations: 1,
                    tx_receipt_timeout_ms: 300000,
                }),
            ]),
            triple_store: triple_store("http://localhost:9999"),
            key_value_store: key_value_store(),
        },
    }
}

fn mainnet() -> ConfigRaw {
    ConfigRaw {
        environment: "mainnet".to_string(),
        app_data_path: PathBuf::from("data"),
        logger: LoggerConfig {
            level: "rust_dkg_engine=info".to_string(),
            format: LogFormat::Pretty,
        },
        telemetry: telemetry(false),
        cleanup: cleanup(),
        sync: sync(),
        paranet_sync: paranet_sync(),
        proving: proving(),
        http_api: http_api(),
        rpc: rpc(),
        managers: ManagersConfigRaw {
            network: network(vec![
                "/ip4/157.230.96.194/tcp/9000/p2p/QmZFcns6eGUosD96beHyevKu1jGJ1bA56Reg2f1J4q59Jt"
                    .to_string(),
                "/ip4/18.132.135.102/tcp/9000/p2p/QmemqyXyvrTAm7PwrcTcFiEEFx69efdR92GSZ1oQprbdja"
                    .to_string(),
            ]),
            repository: repository("root", 120),
            blockchain: BlockchainManagerConfigRaw(vec![
                BlockchainRaw::NeuroWeb(BlockchainConfigRaw {
                    enabled: false,
                    blockchain_id: "otp:2043".into(),
                    hub_contract_address: "0x0957e25BD33034948abc28204ddA54b6E1142D6F".to_string(),
                    rpc_endpoints: vec![
                        "https://astrosat-parachain-rpc.origin-trail.network".to_string(),
                        "https://astrosat.origintrail.network/".to_string(),
                        "https://astrosat-2.origintrail.network/".to_string(),
                    ],
                    operator_fee: Some(0),
                    evm_operational_wallet_address: None,
                    evm_operational_wallet_private_key: None,
                    evm_management_wallet_address: None,
                    evm_management_wallet_private_key: None,
                    node_name: "MainnetNode".to_string(),
                    substrate_rpc_endpoints: None,
                    max_rpc_requests_per_second: None,
                    tx_confirmations: 1,
                    tx_receipt_timeout_ms: 300000,
                }),
                BlockchainRaw::Gnosis(BlockchainConfigRaw {
                    enabled: false,
                    blockchain_id: "gnosis:100".into(),
                    hub_contract_address: "0x882D0BF07F956b1b94BBfe9E77F47c6fc7D4EC8f".to_string(),
                    rpc_endpoints: vec![],
                    operator_fee: Some(0),
                    evm_operational_wallet_address: None,
                    evm_operational_wallet_private_key: None,
                    evm_management_wallet_address: None,
                    evm_management_wallet_private_key: None,
                    node_name: "MainnetNode".to_string(),
                    substrate_rpc_endpoints: None,
                    max_rpc_requests_per_second: None,
                    tx_confirmations: 1,
                    tx_receipt_timeout_ms: 300000,
                }),
                BlockchainRaw::Base(BlockchainConfigRaw {
                    enabled: false,
                    blockchain_id: "base:8453".into(),
                    hub_contract_address: "0x99Aa571fD5e681c2D27ee08A7b7989DB02541d13".to_string(),
                    rpc_endpoints: vec![],
                    operator_fee: Some(0),
                    evm_operational_wallet_address: None,
                    evm_operational_wallet_private_key: None,
                    evm_management_wallet_address: None,
                    evm_management_wallet_private_key: None,
                    node_name: "MainnetNode".to_string(),
                    substrate_rpc_endpoints: None,
                    max_rpc_requests_per_second: None,
                    tx_confirmations: 1,
                    tx_receipt_timeout_ms: 300000,
                }),
            ]),
            triple_store: triple_store("http://localhost:9999"),
            key_value_store: key_value_store(),
        },
    }
}

#[cfg(test)]
mod tests {
    use figment::{Figment, providers::Serialized};

    use super::*;

    /// Verify that development defaults can round-trip through Figment.
    #[test]
    fn development_defaults_round_trip() {
        let config = config_for("development").expect("development defaults should resolve");
        let figment = Figment::from(Serialized::defaults(&config));
        let extracted: ConfigRaw = figment
            .extract()
            .expect("development defaults failed to extract");
        assert_eq!(extracted.environment, "development");
        assert_eq!(extracted.logger.level, "rust_dkg_engine=trace");
        assert!(extracted.telemetry.enabled);
        assert_eq!(extracted.managers.blockchain.0.len(), 2);
    }

    /// Verify that testnet defaults can round-trip through Figment.
    #[test]
    fn testnet_defaults_round_trip() {
        let config = config_for("testnet").expect("testnet defaults should resolve");
        let figment = Figment::from(Serialized::defaults(&config));
        let extracted: ConfigRaw = figment
            .extract()
            .expect("testnet defaults failed to extract");
        assert_eq!(extracted.environment, "testnet");
        assert_eq!(extracted.logger.level, "rust_dkg_engine=info");
        assert!(!extracted.telemetry.enabled);
        assert_eq!(extracted.managers.blockchain.0.len(), 3);
    }

    /// Verify that mainnet defaults can round-trip through Figment.
    #[test]
    fn mainnet_defaults_round_trip() {
        let config = config_for("mainnet").expect("mainnet defaults should resolve");
        let figment = Figment::from(Serialized::defaults(&config));
        let extracted: ConfigRaw = figment
            .extract()
            .expect("mainnet defaults failed to extract");
        assert_eq!(extracted.environment, "mainnet");
        assert_eq!(extracted.managers.blockchain.0.len(), 3);
        assert!(extracted.managers.network.bootstrap.len() == 2);
    }

    /// Verify that user TOML overrides merge correctly on top of typed defaults.
    #[test]
    fn user_toml_overrides_defaults() {
        use figment::providers::{Format, Toml};

        let defaults = config_for("development").expect("development defaults should resolve");
        let user_toml = r#"
            environment = "development"
            [managers.repository]
            max_connections = 50
        "#;
        let figment = Figment::from(Serialized::defaults(&defaults)).merge(Toml::string(user_toml));
        let config: ConfigRaw = figment.extract().expect("merge failed");
        assert_eq!(config.managers.repository.max_connections, 50);
        // Other defaults should be preserved
        assert_eq!(config.http_api.port, 8900);
        assert_eq!(config.managers.repository.user, "root");
    }

    #[test]
    fn unknown_environment_returns_error() {
        let error = config_for("staging").expect_err("unknown env should fail");
        assert!(matches!(error, ConfigError::UnknownEnvironment(env) if env == "staging"));
    }
}
