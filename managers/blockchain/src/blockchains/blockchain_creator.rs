// Generated bindings include methods with many parameters; allow for this module.
#[allow(clippy::too_many_arguments)]
pub mod knowledge_collection_storage {
    use alloy::sol;

    sol!(
        #[sol(rpc)]
        KnowledgeCollectionStorage,
        "../../abi/KnowledgeCollectionStorage.json"
    );
}

// Wrap each ABI in its own module to avoid duplicate ShardingTableLib definitions.
pub mod sharding_table {
    use alloy::sol;

    sol!(
        #[sol(rpc)]
        ShardingTable,
        "../../abi/ShardingTable.json"
    );
}

pub mod sharding_table_storage {
    use alloy::sol;

    sol!(
        #[sol(rpc)]
        ShardingTableStorage,
        "../../abi/ShardingTableStorage.json"
    );
}
sol!(
    #[sol(rpc)]
    Hub,
    "../../abi/Hub.json"
);

sol!(
    #[sol(rpc)]
    Staking,
    "../../abi/Staking.json"
);

sol!(
    #[sol(rpc)]
    IdentityStorage,
    "../../abi/IdentityStorage.json"
);

sol!(
    #[sol(rpc)]
    ParametersStorage,
    "../../abi/ParametersStorage.json"
);

sol!(
    #[sol(rpc)]
    Profile,
    "../../abi/Profile.json"
);

sol!(
    #[sol(rpc)]
    Token,
    "../../abi/Token.json"
);

#[allow(clippy::too_many_arguments)]
pub mod paranets_registry {
    use alloy::sol;

    sol!(
        #[sol(rpc)]
        ParanetsRegistry,
        "../../abi/ParanetsRegistry.json"
    );
}

use std::{num::NonZeroUsize, sync::Arc};

use alloy::{
    network::{Ethereum, EthereumWallet},
    primitives::Address,
    providers::{DynProvider, Provider, ProviderBuilder, WsConnect},
    rpc::client::RpcClient,
    signers::local::PrivateKeySigner,
    sol,
    transports::{
        BoxTransport, IntoBoxTransport,
        http::{Http, reqwest::Url},
        layers::FallbackLayer,
    },
};
pub use knowledge_collection_storage::KnowledgeCollectionStorage;
pub use paranets_registry::ParanetsRegistry;
pub use sharding_table::ShardingTable;
pub use sharding_table_storage::ShardingTableStorage;
use tower::ServiceBuilder;

use crate::{BlockchainConfig, ContractName, error::BlockchainError};

// Use Arc<DynProvider> for thread-safe sharing
pub type BlockchainProvider = Arc<DynProvider<Ethereum>>;

pub struct Contracts {
    hub: Hub::HubInstance<BlockchainProvider>,
    knowledge_collection_storage:
        Option<KnowledgeCollectionStorage::KnowledgeCollectionStorageInstance<BlockchainProvider>>,
    staking: Staking::StakingInstance<BlockchainProvider>,
    identity_storage: IdentityStorage::IdentityStorageInstance<BlockchainProvider>,
    parameters_storage: ParametersStorage::ParametersStorageInstance<BlockchainProvider>,
    profile: Profile::ProfileInstance<BlockchainProvider>,
    sharding_table: ShardingTable::ShardingTableInstance<BlockchainProvider>,
    sharding_table_storage: ShardingTableStorage::ShardingTableStorageInstance<BlockchainProvider>,
    token: Token::TokenInstance<BlockchainProvider>,
    paranets_registry: Option<ParanetsRegistry::ParanetsRegistryInstance<BlockchainProvider>>,
}

impl Contracts {
    pub fn identity_storage(
        &self,
    ) -> &IdentityStorage::IdentityStorageInstance<BlockchainProvider> {
        &self.identity_storage
    }

    pub fn profile(&self) -> &Profile::ProfileInstance<BlockchainProvider> {
        &self.profile
    }

    pub fn sharding_table(&self) -> &ShardingTable::ShardingTableInstance<BlockchainProvider> {
        &self.sharding_table
    }

    pub fn sharding_table_storage(
        &self,
    ) -> &ShardingTableStorage::ShardingTableStorageInstance<BlockchainProvider> {
        &self.sharding_table_storage
    }

    pub fn staking(&self) -> &Staking::StakingInstance<BlockchainProvider> {
        &self.staking
    }

    pub fn token(&self) -> &Token::TokenInstance<BlockchainProvider> {
        &self.token
    }

    pub fn parameters_storage(
        &self,
    ) -> &ParametersStorage::ParametersStorageInstance<BlockchainProvider> {
        &self.parameters_storage
    }

    pub fn knowledge_collection_storage(
        &self,
    ) -> Result<
        &KnowledgeCollectionStorage::KnowledgeCollectionStorageInstance<BlockchainProvider>,
        BlockchainError,
    > {
        self.knowledge_collection_storage.as_ref().ok_or_else(|| {
            BlockchainError::Custom(
                "KnowledgeCollectionStorage contract is not initialized".to_string(),
            )
        })
    }

    pub fn paranets_registry(
        &self,
    ) -> Result<&ParanetsRegistry::ParanetsRegistryInstance<BlockchainProvider>, BlockchainError>
    {
        self.paranets_registry.as_ref().ok_or_else(|| {
            BlockchainError::Custom("ParanetsRegistry contract is not initialized".to_string())
        })
    }

    pub fn get_address(&self, contract_name: &ContractName) -> Result<Address, BlockchainError> {
        match contract_name {
            ContractName::Hub => Ok(*self.hub.address()),
            ContractName::ShardingTable => Ok(*self.sharding_table.address()),
            ContractName::ShardingTableStorage => Ok(*self.sharding_table_storage.address()),
            ContractName::Staking => Ok(*self.staking.address()),
            ContractName::Profile => Ok(*self.profile.address()),
            ContractName::ParametersStorage => Ok(*self.parameters_storage.address()),
            ContractName::KnowledgeCollectionStorage => self
                .knowledge_collection_storage
                .as_ref()
                .map(|storage| *storage.address())
                .ok_or_else(|| {
                    BlockchainError::Custom(
                        "KnowledgeCollectionStorage contract is not initialized".to_string(),
                    )
                }),
            ContractName::ParanetsRegistry => self
                .paranets_registry
                .as_ref()
                .map(|registry| *registry.address())
                .ok_or_else(|| {
                    BlockchainError::Custom(
                        "ParanetsRegistry contract is not initialized".to_string(),
                    )
                }),
        }
    }

    pub async fn replace_contract(
        &mut self,
        provider: &BlockchainProvider,
        contract_name: ContractName,
        contract_address: Address,
    ) -> Result<(), BlockchainError> {
        match contract_name {
            ContractName::Profile => {
                self.profile = Profile::new(contract_address, provider.clone())
            }
            ContractName::ShardingTable => {
                self.sharding_table = ShardingTable::new(contract_address, provider.clone())
            }
            ContractName::ShardingTableStorage => {
                self.sharding_table_storage =
                    ShardingTableStorage::new(contract_address, provider.clone())
            }
            ContractName::Hub => {
                self.hub = Hub::new(contract_address, provider.clone());
            }
            ContractName::Staking => {
                self.staking = Staking::new(contract_address, provider.clone());
            }
            ContractName::ParametersStorage => {
                self.parameters_storage =
                    ParametersStorage::new(contract_address, provider.clone());
            }
            ContractName::KnowledgeCollectionStorage => {
                self.knowledge_collection_storage = Some(KnowledgeCollectionStorage::new(
                    contract_address,
                    provider.clone(),
                ));
            }
            ContractName::ParanetsRegistry => {
                self.paranets_registry =
                    Some(ParanetsRegistry::new(contract_address, provider.clone()));
            }
        };

        Ok(())
    }
}

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
    let signer: PrivateKeySigner =
        config
            .evm_operational_wallet_private_key
            .parse()
            .map_err(|e| BlockchainError::InvalidPrivateKey {
                key_length: config.evm_operational_wallet_private_key.len(),
                source: e,
            })?;
    let wallet = EthereumWallet::from(signer);

    initialize_provider_with_wallet(&config.rpc_endpoints, wallet).await
}

pub(crate) async fn initialize_contracts(
    config: &BlockchainConfig,
    provider: &BlockchainProvider,
) -> Result<Contracts, BlockchainError> {
    let address: Address =
        config
            .hub_contract_address
            .parse()
            .map_err(|_| BlockchainError::InvalidAddress {
                address: config.hub_contract_address.clone(),
            })?;
    let hub = Hub::new(address, provider.clone());

    let asset_storages_addresses = hub.getAllAssetStorages().call().await?;

    let knowledge_collection_storage_addr =
        asset_storages_addresses.iter().rev().find_map(|contract| {
            match contract.name.parse::<ContractName>() {
                Ok(ContractName::KnowledgeCollectionStorage) => Some(contract.addr),
                _ => None,
            }
        });
    let knowledge_collection_storage = knowledge_collection_storage_addr
        .map(|addr| KnowledgeCollectionStorage::new(addr, provider.clone()));

    Ok(Contracts {
        hub: hub.clone(),
        knowledge_collection_storage,
        staking: Staking::new(
            Address::from(
                hub.getContractAddress("Staking".to_string())
                    .call()
                    .await?
                    .0,
            ),
            provider.clone(),
        ),
        identity_storage: IdentityStorage::new(
            Address::from(
                hub.getContractAddress("IdentityStorage".to_string())
                    .call()
                    .await?
                    .0,
            ),
            provider.clone(),
        ),
        parameters_storage: ParametersStorage::new(
            Address::from(
                hub.getContractAddress("ParametersStorage".to_string())
                    .call()
                    .await?
                    .0,
            ),
            provider.clone(),
        ),
        profile: Profile::new(
            Address::from(
                hub.getContractAddress("Profile".to_string())
                    .call()
                    .await?
                    .0,
            ),
            provider.clone(),
        ),
        sharding_table: ShardingTable::new(
            Address::from(
                hub.getContractAddress("ShardingTable".to_string())
                    .call()
                    .await?
                    .0,
            ),
            provider.clone(),
        ),
        sharding_table_storage: ShardingTableStorage::new(
            Address::from(
                hub.getContractAddress("ShardingTableStorage".to_string())
                    .call()
                    .await?
                    .0,
            ),
            provider.clone(),
        ),
        token: Token::new(
            Address::from(hub.getContractAddress("Token".to_string()).call().await?.0),
            provider.clone(),
        ),
        paranets_registry: Some(ParanetsRegistry::new(
            Address::from(
                hub.getContractAddress("ParanetsRegistry".to_string())
                    .call()
                    .await?
                    .0,
            ),
            provider.clone(),
        )),
    })
}
