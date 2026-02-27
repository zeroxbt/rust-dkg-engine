use std::{collections::BTreeMap, time::Duration};

use alloy::{primitives::Address, signers::local::PrivateKeySigner};
use dkg_domain::BlockchainId;
use serde::{Deserialize, Serialize};

use super::chains::evm::GasConfig;
use crate::ConfigError;

/// Configuration for a blockchain network.
///
/// This struct contains all the settings needed to connect to and operate on
/// a specific blockchain network.
///
/// **Secret handling**: Private keys should be provided via configuration
/// (resolved at config load time) or environment variables:
/// - `EVM_OPERATIONAL_WALLET_PRIVATE_KEY` - operational wallet private key (required)
/// - `EVM_MANAGEMENT_WALLET_PRIVATE_KEY` - management wallet private key (optional)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockchainConfigRaw {
    /// Whether this blockchain is enabled in configuration.
    /// Disabled entries are ignored during config resolution.
    #[serde(default)]
    pub enabled: bool,
    /// Unique identifier for this blockchain instance.
    /// Format: "chaintype:chainid" (e.g., "hardhat:31337", "gnosis:100", "neuroweb:2043").
    /// The chain type determines the gas configuration and other chain-specific behavior.
    /// The chain ID is parsed from this field.
    pub blockchain_id: BlockchainId,

    /// Private key for the operational wallet (used for transactions).
    /// Set via EVM_OPERATIONAL_WALLET_PRIVATE_KEY env var or config file.
    pub evm_operational_wallet_private_key: Option<String>,

    /// Address for the operational wallet (20-byte EVM address).
    /// If omitted, it will be derived from the operational private key.
    pub evm_operational_wallet_address: Option<String>,

    /// Address for the management wallet (20-byte EVM address, used for admin operations).
    /// Must be provided in config for testnet/mainnet; no meaningful default.
    pub evm_management_wallet_address: Option<String>,

    /// Private key for the management wallet (optional, only needed for staking/admin ops).
    /// Set via EVM_MANAGEMENT_WALLET_PRIVATE_KEY env var or config file.
    pub evm_management_wallet_private_key: Option<String>,

    /// Hub contract address for this network.
    pub hub_contract_address: String,

    /// RPC endpoints for EVM JSON-RPC calls (supports HTTP and WebSocket).
    /// Multiple endpoints enable fallback if primary fails.
    #[serde(default)]
    pub rpc_endpoints: Vec<String>,

    /// Node name used in profile creation.
    /// This identifies the node on the network.
    pub node_name: String,

    /// Operator fee percentage for delegators (0-100).
    /// This is the percentage of rewards kept by the node operator.
    /// Only required on first profile creation.
    pub operator_fee: Option<u8>,

    /// Substrate RPC endpoints for parachain operations (NeuroWeb only).
    /// Used for EVM account mapping validation.
    /// Supports WebSocket (wss://) and HTTP (https://) endpoints.
    pub substrate_rpc_endpoints: Option<Vec<String>>,

    /// Maximum RPC requests per second (optional rate limiting).
    /// Set this based on your RPC provider's rate limits.
    /// Common values: 25 (free tier), 50-100 (paid tier), None (unlimited).
    pub max_rpc_requests_per_second: Option<u32>,

    /// Number of confirmations to wait for when fetching transaction receipts.
    pub tx_confirmations: u64,

    /// Timeout for waiting on transaction receipts in milliseconds.
    /// Set to 0 to disable the timeout.
    pub tx_receipt_timeout_ms: u64,
}

impl BlockchainConfigRaw {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Ensures the operational wallet private key is set.
    pub fn ensure_operational_wallet_private_key(&self) -> Result<(), ConfigError> {
        if self.evm_operational_wallet_private_key.is_none() {
            return Err(ConfigError::MissingSecret(
                "EVM_OPERATIONAL_WALLET_PRIVATE_KEY env var or evm_operational_wallet_private_key config required".to_string()
            ));
        }
        Ok(())
    }

    /// Ensures the management wallet address is set.
    pub fn ensure_management_wallet_address(&self) -> Result<(), ConfigError> {
        if self.evm_management_wallet_address.is_none() {
            return Err(ConfigError::MissingSecret(
                "evm_management_wallet_address must be set in blockchain config".to_string(),
            ));
        }
        Ok(())
    }

    /// Ensures at least one RPC endpoint is configured.
    pub fn ensure_rpc_endpoints(&self) -> Result<(), ConfigError> {
        if self.rpc_endpoints.is_empty() {
            return Err(ConfigError::InvalidConfig(
                "rpc_endpoints must include at least one endpoint".to_string(),
            ));
        }
        Ok(())
    }

    /// Ensures the RPC rate limit, if configured, is greater than zero.
    pub fn ensure_max_rpc_requests_per_second(&self) -> Result<(), ConfigError> {
        if self.max_rpc_requests_per_second == Some(0) {
            return Err(ConfigError::InvalidConfig(
                "max_rpc_requests_per_second must be greater than 0 when set".to_string(),
            ));
        }
        Ok(())
    }

    pub fn resolve(self) -> Result<BlockchainConfig, ConfigError> {
        let config = self;
        config.ensure_rpc_endpoints()?;
        config.ensure_management_wallet_address()?;
        config.ensure_operational_wallet_private_key()?;
        config.ensure_max_rpc_requests_per_second()?;

        let operational_key = config
            .evm_operational_wallet_private_key
            .clone()
            .expect("operational private key ensured");
        let derived_operational_address = derive_evm_address_from_private_key(&operational_key)?;

        let operational_address = match config.evm_operational_wallet_address.as_deref() {
            Some(address) => {
                let parsed = parse_evm_address(address)?;
                if parsed != derived_operational_address {
                    return Err(ConfigError::InvalidConfig(format!(
                        "evm_operational_wallet_address does not match derived address: provided={}, derived={}",
                        address, derived_operational_address
                    )));
                }
                derived_operational_address.to_checksum(None)
            }
            None => derived_operational_address.to_checksum(None),
        };

        Ok(BlockchainConfig {
            blockchain_id: config.blockchain_id,
            evm_operational_wallet_private_key: operational_key,
            evm_operational_wallet_address: operational_address,
            evm_management_wallet_address: config
                .evm_management_wallet_address
                .expect("management address ensured"),
            evm_management_wallet_private_key: config.evm_management_wallet_private_key,
            hub_contract_address: config.hub_contract_address,
            rpc_endpoints: config.rpc_endpoints,
            node_name: config.node_name,
            operator_fee: config.operator_fee,
            substrate_rpc_endpoints: config.substrate_rpc_endpoints,
            max_rpc_requests_per_second: config.max_rpc_requests_per_second,
            tx_confirmations: config.tx_confirmations,
            tx_receipt_timeout_ms: config.tx_receipt_timeout_ms,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BlockchainConfig {
    pub blockchain_id: BlockchainId,
    pub evm_operational_wallet_private_key: String,
    pub evm_operational_wallet_address: String,
    pub evm_management_wallet_address: String,
    pub evm_management_wallet_private_key: Option<String>,
    pub hub_contract_address: String,
    pub rpc_endpoints: Vec<String>,
    pub node_name: String,
    pub operator_fee: Option<u8>,
    pub substrate_rpc_endpoints: Option<Vec<String>>,
    pub max_rpc_requests_per_second: Option<u32>,
    pub tx_confirmations: u64,
    pub tx_receipt_timeout_ms: u64,
}

impl BlockchainConfig {
    pub fn blockchain_id(&self) -> &BlockchainId {
        &self.blockchain_id
    }

    pub fn evm_operational_wallet_private_key(&self) -> &str {
        &self.evm_operational_wallet_private_key
    }

    pub fn evm_operational_wallet_address(&self) -> &str {
        &self.evm_operational_wallet_address
    }

    pub fn evm_management_wallet_address(&self) -> &str {
        &self.evm_management_wallet_address
    }

    pub fn evm_management_wallet_private_key(&self) -> Option<&str> {
        self.evm_management_wallet_private_key.as_deref()
    }

    pub fn rpc_endpoints(&self) -> &Vec<String> {
        &self.rpc_endpoints
    }

    pub fn hub_contract_address(&self) -> &str {
        &self.hub_contract_address
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn operator_fee(&self) -> Option<u8> {
        self.operator_fee
    }

    pub fn substrate_rpc_endpoints(&self) -> Option<&Vec<String>> {
        self.substrate_rpc_endpoints.as_ref()
    }

    pub fn max_rpc_requests_per_second(&self) -> Option<u32> {
        self.max_rpc_requests_per_second
    }

    pub fn tx_confirmations(&self) -> u64 {
        self.tx_confirmations
    }

    pub fn tx_receipt_timeout(&self) -> Option<Duration> {
        if self.tx_receipt_timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(self.tx_receipt_timeout_ms))
        }
    }
}

fn parse_evm_address(value: &str) -> Result<Address, ConfigError> {
    value
        .parse::<Address>()
        .map_err(|e| ConfigError::InvalidConfig(format!("invalid EVM address '{}': {}", value, e)))
}

fn derive_evm_address_from_private_key(private_key: &str) -> Result<Address, ConfigError> {
    let signer: PrivateKeySigner = private_key
        .parse()
        .map_err(|e| ConfigError::InvalidConfig(format!("invalid EVM private key: {}", e)))?;
    Ok(signer.address())
}

/// Stable config keys for supported blockchains.
///
/// These keys are the only accepted TOML entries under `[managers.blockchain]`.
/// The key determines both blockchain implementation kind and expected blockchain_id.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BlockchainConfigKey {
    #[serde(rename = "hardhat1_31337")]
    Hardhat131337,
    #[serde(rename = "hardhat2_31337")]
    Hardhat231337,
    #[serde(rename = "otp_2043")]
    Otp2043,
    #[serde(rename = "otp_20430")]
    Otp20430,
    #[serde(rename = "gnosis_100")]
    Gnosis100,
    #[serde(rename = "gnosis_10200")]
    Gnosis10200,
    #[serde(rename = "base_8453")]
    Base8453,
    #[serde(rename = "base_84532")]
    Base84532,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockchainKind {
    Hardhat,
    Gnosis,
    NeuroWeb,
    Base,
}

impl BlockchainConfigKey {
    fn expected_blockchain_id(&self) -> BlockchainId {
        match self {
            BlockchainConfigKey::Hardhat131337 => "hardhat1:31337".into(),
            BlockchainConfigKey::Hardhat231337 => "hardhat2:31337".into(),
            BlockchainConfigKey::Otp2043 => "otp:2043".into(),
            BlockchainConfigKey::Otp20430 => "otp:20430".into(),
            BlockchainConfigKey::Gnosis100 => "gnosis:100".into(),
            BlockchainConfigKey::Gnosis10200 => "gnosis:10200".into(),
            BlockchainConfigKey::Base8453 => "base:8453".into(),
            BlockchainConfigKey::Base84532 => "base:84532".into(),
        }
    }

    fn expected_kind(&self) -> BlockchainKind {
        match self {
            BlockchainConfigKey::Hardhat131337 | BlockchainConfigKey::Hardhat231337 => {
                BlockchainKind::Hardhat
            }
            BlockchainConfigKey::Otp2043 | BlockchainConfigKey::Otp20430 => {
                BlockchainKind::NeuroWeb
            }
            BlockchainConfigKey::Gnosis100 | BlockchainConfigKey::Gnosis10200 => {
                BlockchainKind::Gnosis
            }
            BlockchainConfigKey::Base8453 | BlockchainConfigKey::Base84532 => BlockchainKind::Base,
        }
    }
}

/// Supported blockchain types for configuration (resolved).
#[derive(Debug, Clone)]
pub enum Blockchain {
    /// Local Hardhat network for development/testing
    Hardhat(BlockchainConfig),
    /// Gnosis Chain (formerly xDai) - mainnet chain_id: 100, testnet (Chiado): 10200
    Gnosis(BlockchainConfig),
    /// NeuroWeb (OT Parachain) - mainnet chain_id: 2043, testnet: 20430
    NeuroWeb(BlockchainConfig),
    /// Base (Coinbase L2) - mainnet chain_id: 8453, testnet (Sepolia): 84532
    Base(BlockchainConfig),
}

impl Blockchain {
    pub fn get_config(&self) -> &BlockchainConfig {
        match self {
            Blockchain::Hardhat(config)
            | Blockchain::Gnosis(config)
            | Blockchain::NeuroWeb(config)
            | Blockchain::Base(config) => config,
        }
    }

    /// Creates the appropriate gas configuration based on blockchain type.
    pub fn gas_config(&self) -> GasConfig {
        match self {
            Blockchain::Hardhat(_) => GasConfig::hardhat(),
            Blockchain::Gnosis(_) => GasConfig::gnosis(),
            Blockchain::NeuroWeb(_) => GasConfig::neuroweb(),
            Blockchain::Base(_) => GasConfig::base(),
        }
    }

    /// Native token decimal places.
    /// NeuroWeb uses 12 decimals (NEURO), others use standard 18 (ETH/xDAI).
    pub fn native_token_decimals(&self) -> u8 {
        match self {
            Blockchain::NeuroWeb(_) => 12,
            _ => 18,
        }
    }

    /// Native token ticker symbol.
    pub fn native_token_ticker(&self) -> &'static str {
        match self {
            Blockchain::Hardhat(_) => "ETH",
            Blockchain::Gnosis(_) => "xDAI",
            Blockchain::NeuroWeb(_) => "NEURO",
            Blockchain::Base(_) => "ETH",
        }
    }

    /// Whether this chain requires EVM account mapping validation.
    ///
    /// On NeuroWeb, EVM addresses must be mapped to Substrate accounts.
    pub fn requires_evm_account_mapping(&self) -> bool {
        matches!(self, Blockchain::NeuroWeb(_))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlockchainManagerConfigRaw(pub BTreeMap<BlockchainConfigKey, BlockchainConfigRaw>);

impl BlockchainManagerConfigRaw {
    pub fn resolve(self) -> Result<BlockchainManagerConfig, ConfigError> {
        let mut resolved = Vec::with_capacity(self.0.len());
        for (key, config) in self.0 {
            if !config.is_enabled() {
                continue;
            }

            let expected_id = key.expected_blockchain_id();
            if config.blockchain_id != expected_id {
                return Err(ConfigError::InvalidConfig(format!(
                    "blockchain config key '{:?}' expects blockchain_id '{}', got '{}'",
                    key, expected_id, config.blockchain_id
                )));
            }

            let resolved_config = config.resolve()?;
            let resolved_chain = match key.expected_kind() {
                BlockchainKind::Hardhat => Blockchain::Hardhat(resolved_config),
                BlockchainKind::Gnosis => Blockchain::Gnosis(resolved_config),
                BlockchainKind::NeuroWeb => Blockchain::NeuroWeb(resolved_config),
                BlockchainKind::Base => Blockchain::Base(resolved_config),
            };
            resolved.push(resolved_chain);
        }
        if resolved.is_empty() {
            return Err(ConfigError::InvalidConfig(
                "no enabled blockchains configured".to_string(),
            ));
        }
        Ok(BlockchainManagerConfig(resolved))
    }
}

#[derive(Debug, Clone)]
pub struct BlockchainManagerConfig(pub Vec<Blockchain>);

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn sample_raw(max_rpc_requests_per_second: Option<u32>) -> BlockchainConfigRaw {
        BlockchainConfigRaw {
            enabled: true,
            blockchain_id: BlockchainId::from("hardhat:31337"),
            evm_operational_wallet_private_key: Some(
                "449bf49be49946f2160d288a56e820adc5808806d558f33a2412783a61aad3d7".to_string(),
            ),
            evm_operational_wallet_address: None,
            evm_management_wallet_address: Some(
                "0x0000000000000000000000000000000000000001".to_string(),
            ),
            evm_management_wallet_private_key: None,
            hub_contract_address: "0x0000000000000000000000000000000000000002".to_string(),
            rpc_endpoints: vec!["http://localhost:8545".to_string()],
            node_name: "test-node".to_string(),
            operator_fee: Some(0),
            substrate_rpc_endpoints: None,
            max_rpc_requests_per_second,
            tx_confirmations: 1,
            tx_receipt_timeout_ms: 60_000,
        }
    }

    #[test]
    fn resolve_rejects_zero_rpc_rate_limit() {
        let config = sample_raw(Some(0));
        let result = config.resolve();
        assert!(matches!(
            result,
            Err(ConfigError::InvalidConfig(ref msg))
                if msg.contains("max_rpc_requests_per_second")
        ));
    }

    #[test]
    fn resolve_accepts_positive_rpc_rate_limit() {
        let config = sample_raw(Some(10));
        let resolved = config.resolve().unwrap();
        assert_eq!(resolved.max_rpc_requests_per_second(), Some(10));
    }

    #[test]
    fn resolve_rejects_missing_rpc_endpoints() {
        let mut config = sample_raw(None);
        config.rpc_endpoints = vec![];

        let result = config.resolve();
        assert!(matches!(
            result,
            Err(ConfigError::InvalidConfig(ref msg))
                if msg.contains("rpc_endpoints must include at least one endpoint")
        ));
    }
}
