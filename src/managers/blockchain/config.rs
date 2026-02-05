use std::time::Duration;

use serde::Deserialize;

use super::chains::evm::GasConfig;
use crate::{config::ConfigError, types::BlockchainId};

/// Configuration for a blockchain network.
///
/// This struct contains all the settings needed to connect to and operate on
/// a specific blockchain network.
///
/// **Secret handling**: Private keys should be provided via configuration
/// (resolved at config load time) or environment variables:
/// - `EVM_OPERATIONAL_WALLET_PRIVATE_KEY` - operational wallet private key (required)
/// - `EVM_MANAGEMENT_WALLET_PRIVATE_KEY` - management wallet private key (optional)
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlockchainConfig {
    /// Unique identifier for this blockchain instance.
    /// Format: "chaintype:chainid" (e.g., "hardhat:31337", "gnosis:100", "neuroweb:2043").
    /// The chain type determines the gas configuration and other chain-specific behavior.
    /// The chain ID is parsed from this field.
    blockchain_id: BlockchainId,

    /// Private key for the operational wallet (used for transactions).
    /// Set via EVM_OPERATIONAL_WALLET_PRIVATE_KEY env var or config file.
    evm_operational_wallet_private_key: Option<String>,

    /// Address for the operational wallet (20-byte EVM address).
    evm_operational_wallet_address: String,

    /// Address for the management wallet (20-byte EVM address, used for admin operations).
    evm_management_wallet_address: String,

    /// Private key for the management wallet (optional, only needed for staking/admin ops).
    /// Set via EVM_MANAGEMENT_WALLET_PRIVATE_KEY env var or config file.
    evm_management_wallet_private_key: Option<String>,

    /// Hub contract address for this network.
    hub_contract_address: String,

    /// RPC endpoints for EVM JSON-RPC calls (supports HTTP and WebSocket).
    /// Multiple endpoints enable fallback if primary fails.
    rpc_endpoints: Vec<String>,

    /// Node name used in profile creation.
    /// This identifies the node on the network.
    node_name: String,

    /// URL for gas price oracle (optional).
    /// Used by Gnosis to fetch current gas prices from Blockscout.
    gas_price_oracle_url: Option<String>,

    /// Operator fee percentage for delegators (0-100).
    /// This is the percentage of rewards kept by the node operator.
    /// Only required on first profile creation.
    operator_fee: Option<u8>,

    /// Substrate RPC endpoints for parachain operations (NeuroWeb only).
    /// Used for EVM account mapping validation.
    /// Supports WebSocket (wss://) and HTTP (https://) endpoints.
    substrate_rpc_endpoints: Option<Vec<String>>,

    /// Maximum RPC requests per second (optional rate limiting).
    /// Set this based on your RPC provider's rate limits.
    /// Common values: 25 (free tier), 50-100 (paid tier), None (unlimited).
    max_rpc_requests_per_second: Option<u32>,

    /// Number of confirmations to wait for when fetching transaction receipts.
    tx_confirmations: u64,

    /// Timeout for waiting on transaction receipts in milliseconds.
    /// Set to 0 to disable the timeout.
    tx_receipt_timeout_ms: u64,
}

impl BlockchainConfig {
    #[cfg(test)]
    pub(crate) fn test_config(chain_type: &str, chain_id: u64) -> Self {
        Self {
            blockchain_id: BlockchainId::from(format!("{}:{}", chain_type, chain_id)),
            evm_operational_wallet_private_key: None,
            evm_operational_wallet_address: String::new(),
            evm_management_wallet_address: String::new(),
            evm_management_wallet_private_key: None,
            hub_contract_address: String::new(),
            rpc_endpoints: vec![],
            node_name: String::new(),
            gas_price_oracle_url: None,
            operator_fee: None,
            substrate_rpc_endpoints: None,
            max_rpc_requests_per_second: None,
            tx_confirmations: 1,
            tx_receipt_timeout_ms: 300_000,
        }
    }

    pub(crate) fn blockchain_id(&self) -> &BlockchainId {
        &self.blockchain_id
    }

    /// Returns the operational wallet private key.
    /// This is guaranteed to be set after config initialization.
    pub(crate) fn evm_operational_wallet_private_key(&self) -> &str {
        self.evm_operational_wallet_private_key
            .as_ref()
            .expect("evm_operational_wallet_private_key should be set during config initialization")
    }

    pub(crate) fn evm_operational_wallet_address(&self) -> &str {
        &self.evm_operational_wallet_address
    }

    pub(crate) fn evm_management_wallet_address(&self) -> &str {
        &self.evm_management_wallet_address
    }

    /// Returns the management wallet private key if available.
    pub(crate) fn evm_management_wallet_private_key(&self) -> Option<&str> {
        self.evm_management_wallet_private_key.as_deref()
    }

    /// Sets the operational wallet private key (called during secret resolution).
    pub(crate) fn set_operational_wallet_private_key(&mut self, key: String) {
        self.evm_operational_wallet_private_key = Some(key);
    }

    /// Sets the management wallet private key (called during secret resolution).
    pub(crate) fn set_management_wallet_private_key(&mut self, key: String) {
        self.evm_management_wallet_private_key = Some(key);
    }

    /// Ensures the operational wallet private key is set.
    pub(crate) fn ensure_operational_wallet_private_key(&self) -> Result<(), ConfigError> {
        if self.evm_operational_wallet_private_key.is_none() {
            return Err(ConfigError::MissingSecret(
                "EVM_OPERATIONAL_WALLET_PRIVATE_KEY env var or evm_operational_wallet_private_key config required".to_string()
            ));
        }
        Ok(())
    }

    pub(crate) fn rpc_endpoints(&self) -> &Vec<String> {
        &self.rpc_endpoints
    }

    pub(crate) fn hub_contract_address(&self) -> &str {
        &self.hub_contract_address
    }

    pub(crate) fn node_name(&self) -> &str {
        &self.node_name
    }

    pub(crate) fn operator_fee(&self) -> Option<u8> {
        self.operator_fee
    }

    pub(crate) fn substrate_rpc_endpoints(&self) -> Option<&Vec<String>> {
        self.substrate_rpc_endpoints.as_ref()
    }

    pub(crate) fn max_rpc_requests_per_second(&self) -> Option<u32> {
        self.max_rpc_requests_per_second
    }

    pub(crate) fn tx_confirmations(&self) -> u64 {
        self.tx_confirmations
    }

    pub(crate) fn tx_receipt_timeout(&self) -> Option<Duration> {
        if self.tx_receipt_timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(self.tx_receipt_timeout_ms))
        }
    }
}

/// Supported blockchain types for configuration.
///
/// The enum variant determines chain-specific behavior (gas config, token decimals, etc.)
/// and is used for TOML config structure (e.g., `[managers.blockchain.Hardhat]`).
/// The `blockchain_id` field is a protocol identifier for network communication.
#[derive(Debug, Deserialize, Clone)]
pub(crate) enum Blockchain {
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
    pub(crate) fn get_config(&self) -> &BlockchainConfig {
        match self {
            Blockchain::Hardhat(config)
            | Blockchain::Gnosis(config)
            | Blockchain::NeuroWeb(config)
            | Blockchain::Base(config) => config,
        }
    }

    pub(crate) fn get_config_mut(&mut self) -> &mut BlockchainConfig {
        match self {
            Blockchain::Hardhat(config)
            | Blockchain::Gnosis(config)
            | Blockchain::NeuroWeb(config)
            | Blockchain::Base(config) => config,
        }
    }

    /// Creates the appropriate gas configuration based on blockchain type.
    pub(crate) fn gas_config(&self) -> GasConfig {
        let oracle_url = self.get_config().gas_price_oracle_url.clone();
        match self {
            Blockchain::Hardhat(_) => GasConfig::hardhat(),
            Blockchain::Gnosis(_) => GasConfig::gnosis(oracle_url),
            Blockchain::NeuroWeb(_) => GasConfig::neuroweb(oracle_url),
            Blockchain::Base(_) => GasConfig::base(oracle_url),
        }
    }

    /// Native token decimal places.
    /// NeuroWeb uses 12 decimals (NEURO), others use standard 18 (ETH/xDAI).
    pub(crate) fn native_token_decimals(&self) -> u8 {
        match self {
            Blockchain::NeuroWeb(_) => 12,
            _ => 18,
        }
    }

    /// Native token ticker symbol.
    pub(crate) fn native_token_ticker(&self) -> &'static str {
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
    pub(crate) fn requires_evm_account_mapping(&self) -> bool {
        matches!(self, Blockchain::NeuroWeb(_))
    }
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct BlockchainManagerConfig(pub Vec<Blockchain>);

impl BlockchainManagerConfig {
    /// Returns mutable references to all blockchain configs for secret resolution.
    pub(crate) fn configs_mut(&mut self) -> impl Iterator<Item = &mut BlockchainConfig> {
        self.0.iter_mut().map(|b| b.get_config_mut())
    }
}
