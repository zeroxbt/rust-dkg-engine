use std::{num::NonZeroUsize, sync::Arc};

use alloy::{
    network::{Ethereum, EthereumWallet},
    providers::{DynProvider, Provider, ProviderBuilder, WsConnect},
    rpc::client::RpcClient,
    transports::{
        BoxTransport, IntoBoxTransport,
        http::{Http, reqwest::Url},
        layers::FallbackLayer,
    },
};
use tower::ServiceBuilder;

use super::wallets::wallet_from_private_key;
use crate::managers::blockchain::{BlockchainConfig, error::BlockchainError};

/// Use Arc<DynProvider> for thread-safe sharing.
pub(crate) type BlockchainProvider = Arc<DynProvider<Ethereum>>;

/// Creates a provider with the given wallet and RPC endpoints.
/// Supports both HTTP and WebSocket endpoints with automatic failover.
pub(crate) async fn initialize_provider_with_wallet(
    rpc_endpoints: &[String],
    wallet: EthereumWallet,
) -> Result<BlockchainProvider, BlockchainError> {
    // Collect all valid transports (HTTP and WebSocket)
    let mut transports: Vec<BoxTransport> = Vec::new();
    let mut valid_endpoints = Vec::new();

    for endpoint in rpc_endpoints {
        if endpoint.starts_with("ws://") || endpoint.starts_with("wss://") {
            // WebSocket endpoint - connect and box the transport
            let ws_connect = WsConnect::new(endpoint);
            match RpcClient::connect_pubsub(ws_connect).await {
                Ok(client) => {
                    transports.push(client.transport().clone().into_box_transport());
                    valid_endpoints.push(endpoint.clone());
                    tracing::debug!("WebSocket RPC endpoint added: {}", endpoint);
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to WebSocket RPC '{}': {}", endpoint, e);
                }
            }
        } else {
            // HTTP endpoint
            match endpoint.parse::<Url>() {
                Ok(url) => {
                    transports.push(Http::new(url).into_box_transport());
                    valid_endpoints.push(endpoint.clone());
                    tracing::debug!("HTTP RPC endpoint added: {}", endpoint);
                }
                Err(e) => {
                    tracing::warn!("Invalid RPC URL '{}': {}", endpoint, e);
                }
            }
        }
    }

    if transports.is_empty() {
        return Err(BlockchainError::RpcConnectionFailed {
            attempts: rpc_endpoints.len(),
        });
    }

    // Configure fallback layer:
    // - Queries 1 transport at a time (pure failover, no parallel requests)
    // - Automatically ranks by latency + success rate
    // - Falls back to next transport only on failure
    let fallback_layer = FallbackLayer::default().with_active_transport_count(NonZeroUsize::MIN);

    // Build the fallback transport
    let transport = ServiceBuilder::new()
        .layer(fallback_layer)
        .service(transports);

    // Create RPC client with fallback transport
    let client = RpcClient::builder().transport(transport, false);

    // Build provider with wallet
    let provider = ProviderBuilder::new().wallet(wallet).connect_client(client);

    // Verify connectivity
    match provider.get_block_number().await {
        Ok(block) => {
            tracing::info!(
                "Blockchain provider initialized with {} RPC endpoints (block: {}): {:?}",
                valid_endpoints.len(),
                block,
                valid_endpoints
            );
            Ok(Arc::new(provider.erased()))
        }
        Err(e) => {
            tracing::error!("All RPC endpoints failed connectivity check: {}", e);
            Err(BlockchainError::RpcConnectionFailed {
                attempts: valid_endpoints.len(),
            })
        }
    }
}

/// Creates a provider using the operational wallet from config.
pub(crate) async fn initialize_provider(
    config: &BlockchainConfig,
) -> Result<BlockchainProvider, BlockchainError> {
    let wallet = wallet_from_private_key(config.evm_operational_wallet_private_key())?;

    initialize_provider_with_wallet(&config.rpc_endpoints, wallet).await
}
