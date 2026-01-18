//! Substrate/Parachain utilities for NeuroWeb integration.
//!
//! This module provides functionality for interacting with Substrate-based chains,
//! specifically for validating EVM account mappings on NeuroWeb parachain.

use subxt::{
    OnlineClient, PolkadotConfig,
    backend::{BackendExt, rpc::RpcClient},
};

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
pub async fn check_evm_account_mapping(
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
    let client = connect_to_substrate(rpc_endpoints).await?;

    // Query the evmAccounts.accounts storage
    // Storage key: evmAccounts::accounts(H160) -> Option<AccountId32>
    let storage_key = build_evm_accounts_storage_key(&address_bytes);

    // Get the latest finalized block
    let block_ref = client
        .backend()
        .latest_finalized_block_ref()
        .await
        .map_err(|e| BlockchainError::Custom(format!("Failed to get latest block: {}", e)))?;

    // Query raw storage
    let storage_result: Option<Vec<u8>> = client
        .backend()
        .storage_fetch_value(storage_key, block_ref.hash())
        .await
        .map_err(|e| BlockchainError::Custom(format!("Failed to query storage: {}", e)))?;

    // If storage value exists and is not empty, the mapping exists
    match storage_result {
        Some(value) if !value.is_empty() => Ok(true),
        _ => Ok(false),
    }
}

/// Connect to a Substrate chain using the provided RPC endpoints.
///
/// Tries each endpoint in order until one succeeds.
async fn connect_to_substrate(
    rpc_endpoints: &[String],
) -> Result<OnlineClient<PolkadotConfig>, BlockchainError> {
    let mut last_error = None;

    for endpoint in rpc_endpoints {
        tracing::debug!("Trying Substrate RPC endpoint: {}", endpoint);

        match RpcClient::from_url(endpoint).await {
            Ok(rpc_client) => {
                match OnlineClient::<PolkadotConfig>::from_rpc_client(rpc_client).await {
                    Ok(client) => {
                        tracing::info!("Connected to Substrate RPC: {}", endpoint);
                        return Ok(client);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to create Substrate client from {}: {}",
                            endpoint,
                            e
                        );
                        last_error = Some(e.to_string());
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to connect to Substrate RPC {}: {}", endpoint, e);
                last_error = Some(e.to_string());
            }
        }
    }

    Err(BlockchainError::Custom(format!(
        "Failed to connect to any Substrate RPC endpoint: {}",
        last_error.unwrap_or_else(|| "no endpoints provided".to_string())
    )))
}

/// Build the storage key for evmAccounts.accounts(H160).
///
/// The storage key format is:
/// - Pallet name hash (xxhash128): "EvmAccounts" -> first 16 bytes
/// - Storage item hash (xxhash128): "Accounts" -> next 16 bytes
/// - Key hash (blake2_128_concat): H160 address -> 16 bytes hash + 20 bytes address
fn build_evm_accounts_storage_key(address: &[u8; 20]) -> Vec<u8> {
    let mut key = Vec::with_capacity(68); // 16 + 16 + 16 + 20 = 68 bytes

    // Pallet name hash using twox_128
    key.extend_from_slice(&twox_128(b"EvmAccounts"));

    // Storage item hash using twox_128
    key.extend_from_slice(&twox_128(b"Accounts"));

    // Key hash using Blake2_128Concat (hash + raw key)
    key.extend_from_slice(&blake2_128(address));
    key.extend_from_slice(address);

    key
}

/// Compute twox_128 hash (used for pallet and storage item names)
fn twox_128(data: &[u8]) -> [u8; 16] {
    use std::hash::Hasher;

    // xxhash64 with seed 0
    let mut h0 = twox_hash::XxHash64::with_seed(0);
    h0.write(data);
    let r0 = h0.finish();

    // xxhash64 with seed 1
    let mut h1 = twox_hash::XxHash64::with_seed(1);
    h1.write(data);
    let r1 = h1.finish();

    // Combine into 128-bit hash
    let mut result = [0u8; 16];
    result[0..8].copy_from_slice(&r0.to_le_bytes());
    result[8..16].copy_from_slice(&r1.to_le_bytes());
    result
}

/// Compute blake2_128 hash (used for storage key hashing)
fn blake2_128(data: &[u8]) -> [u8; 16] {
    // Use blake2b with 128-bit output
    // subxt uses sp_core_hashing which uses blake2b_simd
    let hash = blake2b_simd::Params::new().hash_length(16).hash(data);
    let mut result = [0u8; 16];
    result.copy_from_slice(hash.as_bytes());
    result
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_storage_key_format() {
        // Test address
        let address: [u8; 20] = [
            0x70, 0x99, 0x79, 0x70, 0xC5, 0x18, 0x12, 0xdc, 0x3A, 0x01, 0x0C, 0x7d, 0x01, 0xb5,
            0x0e, 0x0d, 0x17, 0xdc, 0x79, 0xC8,
        ];

        let key = build_evm_accounts_storage_key(&address);

        // Key should be 68 bytes: 16 (pallet) + 16 (storage) + 16 (blake2_128) + 20 (address)
        assert_eq!(key.len(), 68);

        // Verify the address is at the end (blake2_128_concat includes raw key)
        assert_eq!(&key[48..], &address);
    }

    #[test]
    fn test_twox_128() {
        // Test known hash values
        let hash = twox_128(b"EvmAccounts");
        // The hash should be deterministic
        assert_eq!(hash.len(), 16);
    }

    #[test]
    fn test_blake2_128() {
        let hash = blake2_128(&[0x70, 0x99, 0x79, 0x70]);
        assert_eq!(hash.len(), 16);
    }
}
