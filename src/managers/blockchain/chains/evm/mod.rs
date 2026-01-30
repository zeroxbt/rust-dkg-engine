use std::str::FromStr;

use alloy::{
    primitives::{Address, B256, Bytes, U256, hex},
    providers::Provider,
    rpc::types::Filter,
};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::managers::blockchain::{
    BlockchainConfig, BlockchainId, GasConfig, RpcRateLimiter, SignatureComponents,
    chains::evm::{
        contracts::{Contracts, Profile, Staking, Token, initialize_contracts},
        provider::{BlockchainProvider, initialize_provider, initialize_provider_with_wallet},
    },
    error::BlockchainError,
    error_utils::handle_contract_call,
    gas::fetch_gas_price_from_oracle,
    substrate::validate_evm_wallets,
};

const MAXIMUM_NUMBERS_OF_BLOCKS_TO_FETCH: u64 = 50;

mod contracts;
mod gas;
mod multicall;
mod provider;
mod rpc;
mod wallets;

pub(crate) use contracts::{
    Hub, KnowledgeCollectionStorage, Multicall3, ParametersStorage, ShardingTableLib,
    ShardingTableLib::NodeInfo,
};
pub(crate) use gas::format_balance;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ContractName {
    Hub,
    ShardingTable,
    ShardingTableStorage,
    Staking,
    Profile,
    ParametersStorage,
    KnowledgeCollectionStorage,
    ParanetsRegistry,
}

impl ContractName {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            ContractName::Hub => "Hub",
            ContractName::ShardingTable => "ShardingTable",
            ContractName::ShardingTableStorage => "ShardingTableStorage",
            ContractName::Staking => "Staking",
            ContractName::Profile => "Profile",
            ContractName::ParametersStorage => "ParametersStorage",
            ContractName::KnowledgeCollectionStorage => "KnowledgeCollectionStorage",
            ContractName::ParanetsRegistry => "ParanetsRegistry",
        }
    }
}

impl FromStr for ContractName {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Hub" => Ok(ContractName::Hub),
            "ShardingTable" => Ok(ContractName::ShardingTable),
            "ShardingTableStorage" => Ok(ContractName::ShardingTableStorage),
            "Staking" => Ok(ContractName::Staking),
            "Profile" => Ok(ContractName::Profile),
            "ParametersStorage" => Ok(ContractName::ParametersStorage),
            "KnowledgeCollectionStorage" => Ok(ContractName::KnowledgeCollectionStorage),
            "ParanetsRegistry" => Ok(ContractName::ParanetsRegistry),
            _ => Err(format!("'{}' is not a valid contract name", s)),
        }
    }
}

pub(crate) struct ContractLog {
    contract_name: ContractName,
    log: alloy::rpc::types::Log,
}

impl ContractLog {
    pub(crate) fn new(contract_name: ContractName, log: alloy::rpc::types::Log) -> Self {
        Self { contract_name, log }
    }

    pub(crate) fn contract_name(&self) -> &ContractName {
        &self.contract_name
    }

    pub(crate) fn log(&self) -> &alloy::rpc::types::Log {
        &self.log
    }
}

pub(crate) struct EvmChain {
    config: BlockchainConfig,
    provider: BlockchainProvider,
    contracts: RwLock<Contracts>,
    identity_id: Option<u128>,
    gas_config: GasConfig,
    native_token_decimals: u8,
    native_token_ticker: &'static str,
    is_development_chain: bool,
    requires_evm_account_mapping: bool,
    rpc_rate_limiter: RpcRateLimiter,
    tx_mutex: Mutex<()>,
}

impl EvmChain {
    pub(crate) async fn new(
        config: BlockchainConfig,
        gas_config: GasConfig,
        native_token_decimals: u8,
        native_token_ticker: &'static str,
        is_development_chain: bool,
        requires_evm_account_mapping: bool,
    ) -> Result<Self, BlockchainError> {
        let provider =
            initialize_provider(&config)
                .await
                .map_err(|e| BlockchainError::ProviderInit {
                    reason: e.to_string(),
                })?;

        let contracts = initialize_contracts(&config, &provider)
            .await
            .map_err(|e| BlockchainError::ContractInit {
                reason: e.to_string(),
            })?;

        tracing::info!(
            "Initialized {} blockchain with gas config: default={} wei, max={} wei, native token: {} ({} decimals)",
            config.blockchain_id(),
            gas_config.default_gas_price,
            gas_config.max_gas_price,
            native_token_ticker,
            native_token_decimals
        );

        // Validate EVM account mappings for chains that require it (NeuroWeb)
        if requires_evm_account_mapping {
            // Get Substrate RPC endpoints - can use same endpoints as EVM or dedicated ones
            let substrate_endpoints = config
                .substrate_rpc_endpoints()
                .cloned()
                .unwrap_or_else(|| config.rpc_endpoints().clone());

            if substrate_endpoints.is_empty() {
                tracing::warn!(
                    "{}: No Substrate RPC endpoints configured for EVM account mapping validation. \
                     Skipping validation - ensure wallets are properly mapped.",
                    config.blockchain_id()
                );
            } else {
                tracing::info!(
                    "{}: Validating EVM account mappings via Substrate RPC...",
                    config.blockchain_id()
                );
                validate_evm_wallets(
                    &substrate_endpoints,
                    config.evm_management_wallet_address(),
                    config.evm_operational_wallet_address(),
                )
                .await?;
            }
        }

        // Initialize RPC rate limiter
        let rpc_rate_limiter = RpcRateLimiter::new(config.max_rpc_requests_per_second());
        if let Some(rps) = config.max_rpc_requests_per_second() {
            tracing::info!(
                "{}: RPC rate limiting enabled at {} requests/second",
                config.blockchain_id(),
                rps
            );
        }

        Ok(Self {
            provider,
            config,
            contracts: RwLock::new(contracts),
            identity_id: None,
            gas_config,
            native_token_decimals,
            native_token_ticker,
            is_development_chain,
            requires_evm_account_mapping,
            rpc_rate_limiter,
            tx_mutex: Mutex::new(()),
        })
    }

    pub(crate) fn blockchain_id(&self) -> &BlockchainId {
        self.config.blockchain_id()
    }

    pub(crate) fn config(&self) -> &BlockchainConfig {
        &self.config
    }

    pub(crate) fn provider(&self) -> &BlockchainProvider {
        &self.provider
    }

    pub(crate) async fn contracts(&self) -> RwLockReadGuard<'_, Contracts> {
        self.contracts.read().await
    }

    pub(crate) async fn contracts_mut(&self) -> RwLockWriteGuard<'_, Contracts> {
        self.contracts.write().await
    }

    pub(crate) fn set_identity_id(&mut self, id: u128) {
        self.identity_id = Some(id);
    }

    pub(crate) fn gas_config(&self) -> &GasConfig {
        &self.gas_config
    }

    /// Returns the number of decimal places for the native token.
    pub(crate) fn native_token_decimals(&self) -> u8 {
        self.native_token_decimals
    }

    /// Returns the native token ticker symbol.
    pub(crate) fn native_token_ticker(&self) -> &'static str {
        self.native_token_ticker
    }

    /// Returns true if this is a development/test chain.
    pub(crate) fn is_development_chain(&self) -> bool {
        self.is_development_chain
    }

    /// Returns true if this chain requires EVM account mapping validation.
    pub(crate) fn requires_evm_account_mapping(&self) -> bool {
        self.requires_evm_account_mapping
    }

    /// Execute an RPC call with rate limiting.
    ///
    /// This method waits for rate limit capacity before executing the provided
    /// async operation. Use this for all RPC calls to respect provider rate limits.
    ///
    /// # Example
    /// ```ignore
    /// let result = self.rpc_call(contract.method().call()).await?;
    /// ```
    pub(crate) async fn rpc_call<T, F>(&self, operation: F) -> T::Output
    where
        F: std::future::IntoFuture<IntoFuture = T>,
        T: std::future::Future,
    {
        self.rpc_rate_limiter.acquire().await;
        operation.into_future().await
    }

    /// Execute a transaction with rate limiting and nonce-safe serialization.
    pub(crate) async fn tx_call<T, F>(&self, operation: F) -> T::Output
    where
        F: std::future::IntoFuture<IntoFuture = T>,
        T: std::future::Future,
    {
        let _guard = self.tx_mutex.lock().await;
        self.rpc_rate_limiter.acquire().await;
        operation.into_future().await
    }
}
