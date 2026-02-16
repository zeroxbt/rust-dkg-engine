use std::str::FromStr;

use alloy::{
    contract::{CallBuilder, CallDecoder, Error as ContractError},
    network::Ethereum,
    providers::PendingTransactionBuilder,
    rpc::types::TransactionReceipt,
};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    BlockchainConfig, BlockchainId, RpcRateLimiter,
    chains::evm::{
        contracts::{Contracts, initialize_contracts},
        provider::{BlockchainProvider, initialize_provider},
    },
    error::BlockchainError,
    error_classification::{
        contract_error_backoff_hint, is_retryable_contract_error, should_bump_gas_price,
    },
    rpc_executor::{RetryPolicy, RetryableError, backoff_delay},
    substrate::validate_evm_wallets,
};
mod contracts;
mod error_decode;
mod gas;
mod multicall;
mod provider;
mod rpc;
mod wallets;
pub use contracts::{
    Hub, KnowledgeCollectionStorage, Multicall3, ParametersStorage, PermissionedNode,
    ShardingTableLib, ShardingTableLib::NodeInfo,
    sharding::sharding_table_storage::ShardingTableLib::Node as ShardingTableNode,
};
use error_decode::decode_contract_error;
pub use gas::{FeeQuote, GasConfig};
pub use provider::initialize_provider_with_wallet;
pub use rpc::random_sampling::{NodeChallenge, ProofPeriodStatus};

const MAXIMUM_NUMBERS_OF_BLOCKS_TO_FETCH: u64 = 50;
const GAS_ESTIMATE_MULTIPLIER: f64 = 1.2;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ContractName {
    Hub,
    ShardingTable,
    ShardingTableStorage,
    Staking,
    DelegatorsInfo,
    Profile,
    ParametersStorage,
    KnowledgeCollectionStorage,
    Paranet,
    ParanetsRegistry,
}

impl ContractName {
    pub fn as_str(&self) -> &str {
        match self {
            ContractName::Hub => "Hub",
            ContractName::ShardingTable => "ShardingTable",
            ContractName::ShardingTableStorage => "ShardingTableStorage",
            ContractName::Staking => "Staking",
            ContractName::DelegatorsInfo => "DelegatorsInfo",
            ContractName::Profile => "Profile",
            ContractName::ParametersStorage => "ParametersStorage",
            ContractName::KnowledgeCollectionStorage => "KnowledgeCollectionStorage",
            ContractName::Paranet => "Paranet",
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
            "DelegatorsInfo" => Ok(ContractName::DelegatorsInfo),
            "Profile" => Ok(ContractName::Profile),
            "ParametersStorage" => Ok(ContractName::ParametersStorage),
            "KnowledgeCollectionStorage" => Ok(ContractName::KnowledgeCollectionStorage),
            "Paranet" => Ok(ContractName::Paranet),
            "ParanetsRegistry" => Ok(ContractName::ParanetsRegistry),
            _ => Err(format!("'{}' is not a valid contract name", s)),
        }
    }
}

pub struct ContractLog {
    contract_name: ContractName,
    log: alloy::rpc::types::Log,
}

impl ContractLog {
    pub fn new(contract_name: ContractName, log: alloy::rpc::types::Log) -> Self {
        Self { contract_name, log }
    }

    pub fn contract_name(&self) -> &ContractName {
        &self.contract_name
    }

    pub fn log(&self) -> &alloy::rpc::types::Log {
        &self.log
    }
}

pub struct EvmChain {
    config: BlockchainConfig,
    provider: RwLock<BlockchainProvider>,
    contracts: RwLock<Contracts>,
    identity_id: Option<u128>,
    gas_config: GasConfig,

    rpc_rate_limiter: RpcRateLimiter,
    tx_mutex: Mutex<()>,
    provider_refresh_mutex: Mutex<()>,
    rpc_retry_policy: RetryPolicy,
    tx_retry_policy: RetryPolicy,
}

impl EvmChain {
    pub async fn new(
        config: BlockchainConfig,
        gas_config: GasConfig,
        native_token_decimals: u8,
        native_token_ticker: &'static str,
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
            provider: RwLock::new(provider),
            config,
            contracts: RwLock::new(contracts),
            identity_id: None,
            gas_config,
            rpc_rate_limiter,
            tx_mutex: Mutex::new(()),
            provider_refresh_mutex: Mutex::new(()),
            rpc_retry_policy: RetryPolicy::rpc_default(),
            tx_retry_policy: RetryPolicy::tx_default(),
        })
    }

    pub fn blockchain_id(&self) -> &BlockchainId {
        self.config.blockchain_id()
    }

    pub fn config(&self) -> &BlockchainConfig {
        &self.config
    }

    pub async fn provider(&self) -> BlockchainProvider {
        self.provider.read().await.clone()
    }

    pub async fn contracts(&self) -> RwLockReadGuard<'_, Contracts> {
        self.contracts.read().await
    }

    pub async fn contracts_mut(&self) -> RwLockWriteGuard<'_, Contracts> {
        self.contracts.write().await
    }

    pub fn set_identity_id(&mut self, id: u128) {
        self.identity_id = Some(id);
    }

    pub fn identity_id(&self) -> Option<u128> {
        self.identity_id
    }

    /// Execute an RPC call with rate limiting.
    ///
    /// This method waits for rate limit capacity before executing the provided
    /// async operation. Use this for all RPC calls to respect provider rate limits.
    ///
    /// # Example
    /// ```ignore
    /// let result = self.rpc_call(|| async { contract.method().call().await }).await?;
    /// ```
    pub async fn rpc_call<T, E, F, O>(&self, mut operation: F) -> Result<T, E>
    where
        E: crate::rpc_executor::RetryableError,
        F: FnMut() -> O,
        O: std::future::IntoFuture<Output = Result<T, E>>,
    {
        let mut attempt = 1;
        loop {
            self.rpc_rate_limiter.acquire().await;
            let result = operation().into_future().await;
            match result {
                Ok(value) => return Ok(value),
                Err(err) => {
                    let retryable = err.is_retryable();
                    if attempt >= self.rpc_retry_policy.max_attempts || !retryable {
                        return Err(err);
                    }

                    if err.should_refresh_provider()
                        && let Err(refresh_err) = self.refresh_provider_and_contracts().await
                    {
                        tracing::error!(
                            blockchain = %self.blockchain_id(),
                            error = %refresh_err,
                            "Failed to refresh provider after backend error"
                        );
                    }

                    let delay = backoff_delay(&self.rpc_retry_policy, attempt, err.backoff_hint());
                    tracing::warn!(
                        attempt,
                        max_attempts = self.rpc_retry_policy.max_attempts,
                        delay_ms = delay.as_millis(),
                        error = %err,
                        "rpc_call failed; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Execute a transaction with rate limiting and nonce-safe serialization.
    pub async fn tx_call<D, F>(
        &self,
        mut build_call: F,
    ) -> Result<PendingTransactionBuilder<Ethereum>, ContractError>
    where
        D: CallDecoder,
        for<'a> F: FnMut(&'a Contracts) -> CallBuilder<&'a BlockchainProvider, D, Ethereum>,
    {
        let _guard = self.tx_mutex.lock().await;

        let mut attempt = 1;
        let mut fee_quote = self.get_fee_quote().await;

        let mut cached_gas_limit: Option<u64> = None;

        loop {
            let contracts = self.contracts().await;
            let mut call = build_call(&contracts).with_cloned_provider();
            call = match &fee_quote {
                FeeQuote::Legacy { gas_price, .. } => call.gas_price(gas_price.to::<u128>()),
                FeeQuote::Eip1559 {
                    max_fee_per_gas,
                    max_priority_fee_per_gas,
                    ..
                } => call
                    .max_fee_per_gas(max_fee_per_gas.to::<u128>())
                    .max_priority_fee_per_gas(max_priority_fee_per_gas.to::<u128>()),
            };
            drop(contracts);

            if cached_gas_limit.is_none() {
                self.rpc_rate_limiter.acquire().await;
                match call.estimate_gas().await {
                    Ok(estimate) => {
                        let estimated = apply_gas_estimate_multiplier(estimate);
                        cached_gas_limit = Some(estimated);
                    }
                    Err(err) => {
                        drop(call);
                        let retryable = is_retryable_contract_error(&err);
                        if attempt >= self.tx_retry_policy.max_attempts || !retryable {
                            return Err(err);
                        }

                        let delay = backoff_delay(
                            &self.tx_retry_policy,
                            attempt,
                            contract_error_backoff_hint(&err),
                        );
                        let decoded_error = decode_contract_error(&err);
                        tracing::warn!(
                            attempt,
                            max_attempts = self.tx_retry_policy.max_attempts,
                            delay_ms = delay.as_millis(),
                            error = %err,
                            decoded_error = ?decoded_error,
                            "Gas estimation failed; retrying"
                        );
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    }
                }
            }

            if let Some(gas_limit) = cached_gas_limit {
                call = call.gas(gas_limit);
            }

            self.rpc_rate_limiter.acquire().await;
            match call.send().await {
                Ok(pending_tx) => return Ok(pending_tx),
                Err(err) => {
                    drop(call);
                    let bump_needed = should_bump_gas_price(&err);
                    let retryable = is_retryable_contract_error(&err) || bump_needed;

                    if attempt >= self.tx_retry_policy.max_attempts || !retryable {
                        return Err(err);
                    }

                    if err.should_refresh_provider()
                        && let Err(refresh_err) = self.refresh_provider_and_contracts().await
                    {
                        tracing::error!(
                            blockchain = %self.blockchain_id(),
                            error = %refresh_err,
                            "Failed to refresh provider after backend error"
                        );
                    }

                    if bump_needed {
                        match fee_quote.bump(&self.gas_config) {
                            Some(bumped) => fee_quote = bumped,
                            None => return Err(err),
                        }
                    }

                    let delay = backoff_delay(
                        &self.tx_retry_policy,
                        attempt,
                        contract_error_backoff_hint(&err),
                    );
                    let decoded_error = decode_contract_error(&err);
                    tracing::warn!(
                        attempt,
                        max_attempts = self.tx_retry_policy.max_attempts,
                        delay_ms = delay.as_millis(),
                        error = %err,
                        decoded_error = ?decoded_error,
                        "Transaction submission failed; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }

    pub async fn tx_call_with<D, F>(
        &self,
        mut build_call: F,
    ) -> Result<PendingTransactionBuilder<Ethereum>, ContractError>
    where
        D: CallDecoder,
        F: FnMut() -> CallBuilder<BlockchainProvider, D, Ethereum>,
    {
        let _guard = self.tx_mutex.lock().await;

        let mut attempt = 1;
        let mut fee_quote = self.get_fee_quote().await;

        let mut cached_gas_limit: Option<u64> = None;

        loop {
            let mut call = build_call();
            call = match &fee_quote {
                FeeQuote::Legacy { gas_price, .. } => call.gas_price(gas_price.to::<u128>()),
                FeeQuote::Eip1559 {
                    max_fee_per_gas,
                    max_priority_fee_per_gas,
                    ..
                } => call
                    .max_fee_per_gas(max_fee_per_gas.to::<u128>())
                    .max_priority_fee_per_gas(max_priority_fee_per_gas.to::<u128>()),
            };

            if cached_gas_limit.is_none() {
                self.rpc_rate_limiter.acquire().await;
                match call.estimate_gas().await {
                    Ok(estimate) => {
                        let estimated = apply_gas_estimate_multiplier(estimate);
                        cached_gas_limit = Some(estimated);
                    }
                    Err(err) => {
                        let retryable = is_retryable_contract_error(&err);
                        if attempt >= self.tx_retry_policy.max_attempts || !retryable {
                            return Err(err);
                        }

                        let delay = backoff_delay(
                            &self.tx_retry_policy,
                            attempt,
                            contract_error_backoff_hint(&err),
                        );
                        let decoded_error = decode_contract_error(&err);
                        tracing::warn!(
                            attempt,
                            max_attempts = self.tx_retry_policy.max_attempts,
                            delay_ms = delay.as_millis(),
                            error = %err,
                            decoded_error = ?decoded_error,
                            "Gas estimation failed; retrying"
                        );
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    }
                }
            }

            if let Some(gas_limit) = cached_gas_limit {
                call = call.gas(gas_limit);
            }

            self.rpc_rate_limiter.acquire().await;
            match call.send().await {
                Ok(pending_tx) => return Ok(pending_tx),
                Err(err) => {
                    let bump_needed = should_bump_gas_price(&err);
                    let retryable = is_retryable_contract_error(&err) || bump_needed;

                    if attempt >= self.tx_retry_policy.max_attempts || !retryable {
                        return Err(err);
                    }

                    if err.should_refresh_provider()
                        && let Err(refresh_err) = self.refresh_provider_and_contracts().await
                    {
                        tracing::error!(
                            blockchain = %self.blockchain_id(),
                            error = %refresh_err,
                            "Failed to refresh provider after backend error"
                        );
                    }

                    if bump_needed {
                        match fee_quote.bump(&self.gas_config) {
                            Some(bumped) => fee_quote = bumped,
                            None => return Err(err),
                        }
                    }

                    let delay = backoff_delay(
                        &self.tx_retry_policy,
                        attempt,
                        contract_error_backoff_hint(&err),
                    );
                    let decoded_error = decode_contract_error(&err);
                    tracing::warn!(
                        attempt,
                        max_attempts = self.tx_retry_policy.max_attempts,
                        delay_ms = delay.as_millis(),
                        error = %err,
                        decoded_error = ?decoded_error,
                        "Transaction submission failed; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }
}

impl EvmChain {
    async fn refresh_provider_and_contracts(&self) -> Result<(), BlockchainError> {
        let _guard = self.provider_refresh_mutex.lock().await;

        let provider =
            initialize_provider(&self.config)
                .await
                .map_err(|e| BlockchainError::ProviderInit {
                    reason: e.to_string(),
                })?;

        let contracts = initialize_contracts(&self.config, &provider)
            .await
            .map_err(|e| BlockchainError::ContractInit {
                reason: e.to_string(),
            })?;

        *self.provider.write().await = provider;
        *self.contracts.write().await = contracts;

        tracing::info!(
            "{}: Refreshed provider and contract instances",
            self.blockchain_id()
        );

        Ok(())
    }

    /// Await the receipt for a pending transaction with configured confirmations/timeout.
    pub async fn handle_contract_call(
        &self,
        result: Result<PendingTransactionBuilder<alloy::network::Ethereum>, ContractError>,
    ) -> Result<TransactionReceipt, BlockchainError> {
        match result {
            Ok(pending_tx) => {
                let pending_tx = pending_tx
                    .with_required_confirmations(self.config.tx_confirmations())
                    .with_timeout(self.config.tx_receipt_timeout());

                match pending_tx.get_receipt().await {
                    Ok(receipt) => Ok(receipt),
                    Err(err) => {
                        tracing::error!("Failed to retrieve transaction receipt: {:?}", err);
                        Err(BlockchainError::ReceiptFailed {
                            reason: err.to_string(),
                        })
                    }
                }
            }
            Err(err) => {
                let decoded_error = decode_contract_error(&err);
                tracing::error!(
                    decoded_error = ?decoded_error,
                    "Contract call failed: {:?}",
                    err
                );
                Err(BlockchainError::Contract(err))
            }
        }
    }
}

fn apply_gas_estimate_multiplier(estimate: u64) -> u64 {
    if estimate == 0 {
        return 0;
    }

    let scaled = (estimate as f64 * GAS_ESTIMATE_MULTIPLIER).ceil();
    if !scaled.is_finite() || scaled <= 0.0 {
        return estimate;
    }

    let scaled = scaled.min(u64::MAX as f64) as u64;
    scaled.max(estimate)
}
