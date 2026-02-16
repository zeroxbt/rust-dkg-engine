//! Substrate/Parachain utilities for NeuroWeb integration.
//!
//! This module provides functionality for interacting with Substrate-based chains,
//! specifically for validating EVM account mappings on NeuroWeb parachain.

use std::io;

use jsonrpsee::{
    core::{client::ClientT, traits::ToRpcParams},
    http_client::{HttpClient, HttpClientBuilder},
};
use subxt::{
    OnlineClient, PolkadotConfig,
    backend::rpc::{RawRpcFuture, RawRpcSubscription, RawValue, RpcClient, RpcClientT},
    dynamic::Value,
    metadata::Metadata,
};
use url::Url;

use crate::error::BlockchainError;

/// Check if an EVM wallet address has a valid account mapping on the parachain.
///
/// On NeuroWeb, EVM addresses (20-byte Ethereum-style) must be mapped to Substrate
/// accounts (32-byte SS58) before they can be used. This function queries the
/// `evmAccounts.accounts` storage to verify the mapping exists.
///
/// # Arguments
/// * `rpc_endpoints` - List of Substrate RPC endpoints (WebSocket or HTTP)
/// * `evm_address` - The EVM wallet address to check (with or without 0x prefix)
///
/// # Returns
/// * `Ok(true)` - The address has a valid mapping
/// * `Ok(false)` - The address does not have a mapping
/// * `Err(_)` - Failed to connect or query the chain
async fn check_evm_account_mapping(
    rpc_endpoints: &[String],
    evm_address: &str,
) -> Result<bool, BlockchainError> {
    // Parse the EVM address (20 bytes)
    let address_hex = evm_address.strip_prefix("0x").unwrap_or(evm_address);
    let address_bytes: [u8; 20] = hex::decode(address_hex)
        .map_err(|e| BlockchainError::Custom(format!("Invalid EVM address: {}", e)))?
        .try_into()
        .map_err(|_| BlockchainError::Custom("EVM address must be 20 bytes".to_string()))?;

    // Try connecting to each endpoint until one succeeds
    let mut last_error = None;

    for endpoint in rpc_endpoints {
        tracing::debug!("Trying Substrate RPC endpoint: {}", endpoint);

        let rpc_client = match build_rpc_client(endpoint).await {
            Ok(client) => client,
            Err(e) => {
                tracing::warn!("Failed to connect to Substrate RPC {}: {}", endpoint, e);
                last_error = Some(e);
                continue;
            }
        };

        let result: Result<bool, String> = async {
            let client = OnlineClient::<PolkadotConfig>::from_rpc_client(rpc_client)
                .await
                .map_err(|e| e.to_string())?;

            let block_ref = client
                .backend()
                .latest_finalized_block_ref()
                .await
                .map_err(|e| format!("Failed to get latest block: {}", e))?;

            let metadata = client.metadata();
            let (pallet_name, storage_name) = resolve_evm_accounts_storage_names(&metadata)
                .map_err(|e| format!("Failed to resolve evmAccounts storage: {}", e))?;

            let storage_query = subxt::dynamic::storage(
                &pallet_name,
                &storage_name,
                vec![Value::from_bytes(address_bytes)],
            );

            let storage_result = client
                .storage()
                .at(block_ref.hash())
                .fetch(&storage_query)
                .await
                .map_err(|e| format!("Failed to query storage: {}", e))?;

            Ok(storage_result.is_some())
        }
        .await;

        match result {
            Ok(mapped) => {
                tracing::info!("Connected to Substrate RPC: {}", endpoint);
                return Ok(mapped);
            }
            Err(e) => {
                tracing::warn!("Failed to connect to Substrate RPC {}: {}", endpoint, e);
                last_error = Some(e);
            }
        }
    }

    Err(BlockchainError::Custom(format!(
        "Failed to connect to any Substrate RPC endpoint: {}",
        last_error.unwrap_or_else(|| "no endpoints provided".to_string())
    )))
}

fn resolve_evm_accounts_storage_names(metadata: &Metadata) -> Result<(String, String), String> {
    let pallet = metadata
        .pallets()
        .find(|pallet| pallet.name().eq_ignore_ascii_case("evmaccounts"))
        .ok_or_else(|| "pallet 'EvmAccounts' not found in metadata".to_string())?;

    let storage = pallet
        .storage()
        .ok_or_else(|| format!("pallet '{}' does not expose storage entries", pallet.name()))?;

    let entry = storage
        .entries()
        .iter()
        .find(|entry| entry.name().eq_ignore_ascii_case("accounts"))
        .ok_or_else(|| {
            format!(
                "storage entry 'Accounts' not found in pallet '{}'",
                pallet.name()
            )
        })?;

    Ok((pallet.name().to_string(), entry.name().to_string()))
}

async fn build_rpc_client(endpoint: &str) -> Result<RpcClient, String> {
    let url = Url::parse(endpoint).map_err(|e| format!("Invalid URL: {}", e))?;

    match url.scheme() {
        "wss" => RpcClient::from_url(endpoint)
            .await
            .map_err(|e| e.to_string()),
        "ws" => RpcClient::from_insecure_url(endpoint)
            .await
            .map_err(|e| e.to_string()),
        "https" | "http" => {
            let client = HttpClientBuilder::default()
                .build(url)
                .map_err(|e| e.to_string())?;
            Ok(RpcClient::new(HttpRpcClient::new(client)))
        }
        _ => Err(format!(
            "Unsupported Substrate RPC URL scheme (expected ws/wss or http/https): {}",
            endpoint
        )),
    }
}

struct HttpRpcClient {
    client: HttpClient,
}

impl HttpRpcClient {
    fn new(client: HttpClient) -> Self {
        Self { client }
    }
}

struct Params(Option<Box<RawValue>>);

impl ToRpcParams for Params {
    fn to_rpc_params(self) -> Result<Option<Box<RawValue>>, serde_json::Error> {
        Ok(self.0)
    }
}

impl RpcClientT for HttpRpcClient {
    fn request_raw<'a>(
        &'a self,
        method: &'a str,
        params: Option<Box<RawValue>>,
    ) -> RawRpcFuture<'a, Box<RawValue>> {
        Box::pin(async move {
            let res = ClientT::request(&self.client, method, Params(params))
                .await
                .map_err(subxt::ext::subxt_rpcs::Error::from)?;
            Ok(res)
        })
    }

    fn subscribe_raw<'a>(
        &'a self,
        _sub: &'a str,
        _params: Option<Box<RawValue>>,
        _unsub: &'a str,
    ) -> RawRpcFuture<'a, RawRpcSubscription> {
        Box::pin(async move {
            Err(subxt::ext::subxt_rpcs::Error::Client(Box::new(
                io::Error::other("HTTP Substrate RPC does not support subscriptions; use ws/wss"),
            )))
        })
    }
}

/// Validate EVM wallet mappings for NeuroWeb parachain.
///
/// This checks that all configured EVM wallets have valid Substrate account mappings.
/// Both management and operational wallet mappings are **required**.
pub async fn validate_evm_wallets(
    rpc_endpoints: &[String],
    management_wallet: &str,
    operational_wallet: &str,
) -> Result<(), BlockchainError> {
    // Check management wallet - REQUIRED
    let management_mapped = check_evm_account_mapping(rpc_endpoints, management_wallet).await?;
    if !management_mapped {
        return Err(BlockchainError::EvmAccountMappingRequired {
            wallet_type: "management".to_string(),
            wallet_address: management_wallet.to_string(),
        });
    }
    tracing::info!(
        "Management wallet {} has valid EVM account mapping",
        management_wallet
    );

    // Check operational wallet - REQUIRED
    let operational_mapped = check_evm_account_mapping(rpc_endpoints, operational_wallet).await?;
    if !operational_mapped {
        return Err(BlockchainError::EvmAccountMappingRequired {
            wallet_type: "operational".to_string(),
            wallet_address: operational_wallet.to_string(),
        });
    }
    tracing::info!(
        "Operational wallet {} has valid EVM account mapping",
        operational_wallet
    );

    Ok(())
}

// Re-export hex for internal use
mod hex {
    pub fn decode(s: &str) -> Result<Vec<u8>, String> {
        alloy::primitives::hex::decode(s).map_err(|e| e.to_string())
    }
}
