// Generated bindings include methods with many parameters; allow for this module.
#[allow(clippy::too_many_arguments)]
pub(crate) mod knowledge_collection_storage {
    use alloy::sol;

    sol!(
        #[sol(rpc)]
        KnowledgeCollectionStorage,
        "abi/KnowledgeCollectionStorage.json"
    );
}

// Wrap each ABI in its own module to avoid duplicate ShardingTableLib definitions.
pub(crate) mod sharding_table {
    use alloy::sol;

    sol!(
        #[sol(rpc)]
        ShardingTable,
        "abi/ShardingTable.json"
    );
}

pub(crate) mod sharding_table_storage {
    use alloy::sol;

    sol!(
        #[sol(rpc)]
        ShardingTableStorage,
        "abi/ShardingTableStorage.json"
    );
}
sol!(
    #[sol(rpc)]
    Hub,
    "abi/Hub.json"
);

sol!(
    #[sol(rpc)]
    Staking,
    "abi/Staking.json"
);

sol!(
    #[sol(rpc)]
    IdentityStorage,
    "abi/IdentityStorage.json"
);

sol!(
    #[sol(rpc)]
    ParametersStorage,
    "abi/ParametersStorage.json"
);

sol!(
    #[sol(rpc)]
    Profile,
    "abi/Profile.json"
);

sol!(
    #[sol(rpc)]
    Token,
    "abi/Token.json"
);

sol!(
    #[sol(rpc)]
    Chronos,
    "abi/Chronos.json"
);

#[allow(clippy::too_many_arguments)]
pub(crate) mod paranets_registry {
    use alloy::sol;

    sol!(
        #[sol(rpc)]
        ParanetsRegistry,
        "abi/ParanetsRegistry.json"
    );
}

use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

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
pub(crate) use knowledge_collection_storage::KnowledgeCollectionStorage;
pub(crate) use paranets_registry::ParanetsRegistry;
pub(crate) use sharding_table::ShardingTable;
pub(crate) use sharding_table_storage::ShardingTableStorage;
use tower::ServiceBuilder;

use crate::managers::blockchain::{BlockchainConfig, ContractName, error::BlockchainError};

// Use Arc<DynProvider> for thread-safe sharing
pub(crate) type BlockchainProvider = Arc<DynProvider<Ethereum>>;

pub(crate) struct Contracts {
    hub: Hub::HubInstance<BlockchainProvider>,
    knowledge_collection_storages: HashMap<
        Address,
        KnowledgeCollectionStorage::KnowledgeCollectionStorageInstance<BlockchainProvider>,
    >,
    staking: Staking::StakingInstance<BlockchainProvider>,
    identity_storage: IdentityStorage::IdentityStorageInstance<BlockchainProvider>,
    parameters_storage: ParametersStorage::ParametersStorageInstance<BlockchainProvider>,
    profile: Profile::ProfileInstance<BlockchainProvider>,
    sharding_table: ShardingTable::ShardingTableInstance<BlockchainProvider>,
    sharding_table_storage: ShardingTableStorage::ShardingTableStorageInstance<BlockchainProvider>,
    token: Token::TokenInstance<BlockchainProvider>,
    chronos: Chronos::ChronosInstance<BlockchainProvider>,
    paranets_registry: Option<ParanetsRegistry::ParanetsRegistryInstance<BlockchainProvider>>,
}

impl Contracts {
    pub(crate) fn identity_storage(
        &self,
    ) -> &IdentityStorage::IdentityStorageInstance<BlockchainProvider> {
        &self.identity_storage
    }

    pub(crate) fn profile(&self) -> &Profile::ProfileInstance<BlockchainProvider> {
        &self.profile
    }

    pub(crate) fn sharding_table(
        &self,
    ) -> &ShardingTable::ShardingTableInstance<BlockchainProvider> {
        &self.sharding_table
    }

    pub(crate) fn sharding_table_storage(
        &self,
    ) -> &ShardingTableStorage::ShardingTableStorageInstance<BlockchainProvider> {
        &self.sharding_table_storage
    }

    pub(crate) fn staking(&self) -> &Staking::StakingInstance<BlockchainProvider> {
        &self.staking
    }

    pub(crate) fn token(&self) -> &Token::TokenInstance<BlockchainProvider> {
        &self.token
    }

    pub(crate) fn parameters_storage(
        &self,
    ) -> &ParametersStorage::ParametersStorageInstance<BlockchainProvider> {
        &self.parameters_storage
    }

    /// Get a KnowledgeCollectionStorage contract by its address.
    pub(crate) fn knowledge_collection_storage_by_address(
        &self,
        address: &Address,
    ) -> Option<&KnowledgeCollectionStorage::KnowledgeCollectionStorageInstance<BlockchainProvider>>
    {
        self.knowledge_collection_storages.get(address)
    }

    pub(crate) fn chronos(&self) -> &Chronos::ChronosInstance<BlockchainProvider> {
        &self.chronos
    }

    pub(crate) fn paranets_registry(
        &self,
    ) -> Result<&ParanetsRegistry::ParanetsRegistryInstance<BlockchainProvider>, BlockchainError>
    {
        self.paranets_registry.as_ref().ok_or_else(|| {
            BlockchainError::Custom("ParanetsRegistry contract is not initialized".to_string())
        })
    }

    pub(crate) fn get_address(
        &self,
        contract_name: &ContractName,
    ) -> Result<Address, BlockchainError> {
        match contract_name {
            ContractName::Hub => Ok(*self.hub.address()),
            ContractName::ShardingTable => Ok(*self.sharding_table.address()),
            ContractName::ShardingTableStorage => Ok(*self.sharding_table_storage.address()),
            ContractName::Staking => Ok(*self.staking.address()),
            ContractName::Profile => Ok(*self.profile.address()),
            ContractName::ParametersStorage => Ok(*self.parameters_storage.address()),
            ContractName::KnowledgeCollectionStorage => self
                .knowledge_collection_storages
                .keys()
                .next()
                .copied()
                .ok_or_else(|| {
                    BlockchainError::Custom(
                        "No KnowledgeCollectionStorage contracts initialized".to_string(),
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

    /// Get all addresses for a contract type.
    /// For KnowledgeCollectionStorage, returns all registered addresses.
    /// For other contracts, returns a single address.
    pub(crate) fn get_all_addresses(&self, contract_name: &ContractName) -> Vec<Address> {
        match contract_name {
            ContractName::KnowledgeCollectionStorage => {
                self.knowledge_collection_storages.keys().copied().collect()
            }
            _ => self.get_address(contract_name).into_iter().collect(),
        }
    }

    pub(crate) async fn replace_contract(
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
                // ADD contract instead of replacing (supports multiple storage contracts)
                if let std::collections::hash_map::Entry::Vacant(e) =
                    self.knowledge_collection_storages.entry(contract_address)
                {
                    e.insert(KnowledgeCollectionStorage::new(
                        contract_address,
                        provider.clone(),
                    ));
                    tracing::info!(
                        "Added KnowledgeCollectionStorage at {:?} (total: {})",
                        contract_address,
                        self.knowledge_collection_storages.len()
                    );
                }
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

    // Collect ALL KnowledgeCollectionStorage contracts into HashMap
    let mut knowledge_collection_storages = HashMap::new();
    for contract in &asset_storages_addresses {
        if let Ok(ContractName::KnowledgeCollectionStorage) = contract.name.parse::<ContractName>()
        {
            knowledge_collection_storages.insert(
                contract.addr,
                KnowledgeCollectionStorage::new(contract.addr, provider.clone()),
            );
            tracing::info!(
                "Initialized KnowledgeCollectionStorage at {:?}",
                contract.addr
            );
        }
    }

    if knowledge_collection_storages.is_empty() {
        tracing::warn!("No KnowledgeCollectionStorage contracts found from Hub");
    } else {
        tracing::info!(
            "Initialized {} KnowledgeCollectionStorage contract(s)",
            knowledge_collection_storages.len()
        );
    }

    Ok(Contracts {
        hub: hub.clone(),
        knowledge_collection_storages,
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
        chronos: Chronos::new(
            Address::from(
                hub.getContractAddress("Chronos".to_string())
                    .call()
                    .await?
                    .0,
            ),
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
