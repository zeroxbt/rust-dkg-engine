use std::time::Duration;

use alloy::{primitives::Address, signers::local::PrivateKeySigner};
use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlockchainConfigRaw {
    /// Whether this blockchain is enabled in configuration.
    /// Disabled entries are ignored during config resolution.
    #[serde(default)]
    pub(crate) enabled: bool,
    /// Unique identifier for this blockchain instance.
    /// Format: "chaintype:chainid" (e.g., "hardhat:31337", "gnosis:100", "neuroweb:2043").
    /// The chain type determines the gas configuration and other chain-specific behavior.
    /// The chain ID is parsed from this field.
    pub(crate) blockchain_id: BlockchainId,

    /// Private key for the operational wallet (used for transactions).
    /// Set via EVM_OPERATIONAL_WALLET_PRIVATE_KEY env var or config file.
    pub(crate) evm_operational_wallet_private_key: Option<String>,

    /// Address for the operational wallet (20-byte EVM address).
    /// If omitted, it will be derived from the operational private key.
    pub(crate) evm_operational_wallet_address: Option<String>,

    /// Address for the management wallet (20-byte EVM address, used for admin operations).
    /// Must be provided in config for testnet/mainnet; no meaningful default.
    pub(crate) evm_management_wallet_address: Option<String>,

    /// Private key for the management wallet (optional, only needed for staking/admin ops).
    /// Set via EVM_MANAGEMENT_WALLET_PRIVATE_KEY env var or config file.
    pub(crate) evm_management_wallet_private_key: Option<String>,

    /// Hub contract address for this network.
    pub(crate) hub_contract_address: String,

    /// RPC endpoints for EVM JSON-RPC calls (supports HTTP and WebSocket).
    /// Multiple endpoints enable fallback if primary fails.
    pub(crate) rpc_endpoints: Vec<String>,

    /// Node name used in profile creation.
    /// This identifies the node on the network.
    pub(crate) node_name: String,

    /// URL for gas price oracle (optional).
    /// Used by Gnosis to fetch current gas prices from Blockscout.
    pub(crate) gas_price_oracle_url: Option<String>,

    /// Operator fee percentage for delegators (0-100).
    /// This is the percentage of rewards kept by the node operator.
    /// Only required on first profile creation.
    pub(crate) operator_fee: Option<u8>,

    /// Substrate RPC endpoints for parachain operations (NeuroWeb only).
    /// Used for EVM account mapping validation.
    /// Supports WebSocket (wss://) and HTTP (https://) endpoints.
    pub(crate) substrate_rpc_endpoints: Option<Vec<String>>,

    /// Maximum RPC requests per second (optional rate limiting).
    /// Set this based on your RPC provider's rate limits.
    /// Common values: 25 (free tier), 50-100 (paid tier), None (unlimited).
    pub(crate) max_rpc_requests_per_second: Option<u32>,

    /// Number of confirmations to wait for when fetching transaction receipts.
    pub(crate) tx_confirmations: u64,

    /// Timeout for waiting on transaction receipts in milliseconds.
    /// Set to 0 to disable the timeout.
    pub(crate) tx_receipt_timeout_ms: u64,
}

impl BlockchainConfigRaw {
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
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

    /// Ensures the management wallet address is set.
    pub(crate) fn ensure_management_wallet_address(&self) -> Result<(), ConfigError> {
        if self.evm_management_wallet_address.is_none() {
            return Err(ConfigError::MissingSecret(
                "evm_management_wallet_address must be set in blockchain config".to_string(),
            ));
        }
        Ok(())
    }

    /// Ensures at least one RPC endpoint is configured.
    pub(crate) fn ensure_rpc_endpoints(&self) -> Result<(), ConfigError> {
        if self.rpc_endpoints.is_empty() {
            return Err(ConfigError::InvalidConfig(
                "rpc_endpoints must include at least one endpoint".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn resolve(self) -> Result<BlockchainConfig, ConfigError> {
        self.ensure_rpc_endpoints()?;
        self.ensure_management_wallet_address()?;
        self.ensure_operational_wallet_private_key()?;

        let operational_key = self
            .evm_operational_wallet_private_key
            .clone()
            .expect("operational private key ensured");
        let derived_operational_address = derive_evm_address_from_private_key(&operational_key)?;

        let operational_address = match self.evm_operational_wallet_address.as_deref() {
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
            blockchain_id: self.blockchain_id,
            evm_operational_wallet_private_key: operational_key,
            evm_operational_wallet_address: operational_address,
            evm_management_wallet_address: self
                .evm_management_wallet_address
                .expect("management address ensured"),
            evm_management_wallet_private_key: self.evm_management_wallet_private_key,
            hub_contract_address: self.hub_contract_address,
            rpc_endpoints: self.rpc_endpoints,
            node_name: self.node_name,
            gas_price_oracle_url: self.gas_price_oracle_url,
            operator_fee: self.operator_fee,
            substrate_rpc_endpoints: self.substrate_rpc_endpoints,
            max_rpc_requests_per_second: self.max_rpc_requests_per_second,
            tx_confirmations: self.tx_confirmations,
            tx_receipt_timeout_ms: self.tx_receipt_timeout_ms,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BlockchainConfig {
    pub(crate) blockchain_id: BlockchainId,
    pub(crate) evm_operational_wallet_private_key: String,
    pub(crate) evm_operational_wallet_address: String,
    pub(crate) evm_management_wallet_address: String,
    pub(crate) evm_management_wallet_private_key: Option<String>,
    pub(crate) hub_contract_address: String,
    pub(crate) rpc_endpoints: Vec<String>,
    pub(crate) node_name: String,
    pub(crate) gas_price_oracle_url: Option<String>,
    pub(crate) operator_fee: Option<u8>,
    pub(crate) substrate_rpc_endpoints: Option<Vec<String>>,
    pub(crate) max_rpc_requests_per_second: Option<u32>,
    pub(crate) tx_confirmations: u64,
    pub(crate) tx_receipt_timeout_ms: u64,
}

impl BlockchainConfig {
    pub(crate) fn blockchain_id(&self) -> &BlockchainId {
        &self.blockchain_id
    }

    pub(crate) fn evm_operational_wallet_private_key(&self) -> &str {
        &self.evm_operational_wallet_private_key
    }

    pub(crate) fn evm_operational_wallet_address(&self) -> &str {
        &self.evm_operational_wallet_address
    }

    pub(crate) fn evm_management_wallet_address(&self) -> &str {
        &self.evm_management_wallet_address
    }

    pub(crate) fn evm_management_wallet_private_key(&self) -> Option<&str> {
        self.evm_management_wallet_private_key.as_deref()
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

/// Supported blockchain types for configuration (raw, deserialized).
///
/// The enum variant determines chain-specific behavior (gas config, token decimals, etc.)
/// and is used for TOML config structure (e.g., `[managers.blockchain.Hardhat]`).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum BlockchainRaw {
    /// Local Hardhat network for development/testing
    Hardhat(BlockchainConfigRaw),
    /// Gnosis Chain (formerly xDai) - mainnet chain_id: 100, testnet (Chiado): 10200
    Gnosis(BlockchainConfigRaw),
    /// NeuroWeb (OT Parachain) - mainnet chain_id: 2043, testnet: 20430
    NeuroWeb(BlockchainConfigRaw),
    /// Base (Coinbase L2) - mainnet chain_id: 8453, testnet (Sepolia): 84532
    Base(BlockchainConfigRaw),
}

impl BlockchainRaw {
    pub(crate) fn is_enabled(&self) -> bool {
        match self {
            BlockchainRaw::Hardhat(config)
            | BlockchainRaw::Gnosis(config)
            | BlockchainRaw::NeuroWeb(config)
            | BlockchainRaw::Base(config) => config.is_enabled(),
        }
    }

    pub(crate) fn resolve(self) -> Result<Blockchain, ConfigError> {
        match self {
            BlockchainRaw::Hardhat(config) => Ok(Blockchain::Hardhat(config.resolve()?)),
            BlockchainRaw::Gnosis(config) => Ok(Blockchain::Gnosis(config.resolve()?)),
            BlockchainRaw::NeuroWeb(config) => Ok(Blockchain::NeuroWeb(config.resolve()?)),
            BlockchainRaw::Base(config) => Ok(Blockchain::Base(config.resolve()?)),
        }
    }
}

/// Supported blockchain types for configuration (resolved).
#[derive(Debug, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct BlockchainManagerConfigRaw(pub Vec<BlockchainRaw>);

impl BlockchainManagerConfigRaw {
    pub(crate) fn resolve(self) -> Result<BlockchainManagerConfig, ConfigError> {
        use std::collections::HashSet;

        let mut seen_ids: HashSet<BlockchainId> = HashSet::new();
        let mut resolved = Vec::with_capacity(self.0.len());
        for chain in self.0 {
            if chain.is_enabled() {
                let resolved_chain = chain.resolve()?;
                let id = resolved_chain.get_config().blockchain_id().clone();
                if !seen_ids.insert(id.clone()) {
                    return Err(ConfigError::InvalidConfig(format!(
                        "duplicate blockchain_id configured: {}",
                        id
                    )));
                }
                resolved.push(resolved_chain);
            }
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
pub(crate) struct BlockchainManagerConfig(pub Vec<Blockchain>);
