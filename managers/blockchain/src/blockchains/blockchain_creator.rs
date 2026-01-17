use std::sync::Arc;

use alloy::{
    network::{Ethereum, EthereumWallet},
    primitives::Address,
    providers::{DynProvider, Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol,
};

use crate::{BlockchainConfig, ContractName, error::BlockchainError};

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
    KnowledgeCollectionStorage,
    "../../abi/KnowledgeCollectionStorage.json"
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
    sharding_table: sharding_table::ShardingTable::ShardingTableInstance<BlockchainProvider>,
    sharding_table_storage:
        sharding_table_storage::ShardingTableStorage::ShardingTableStorageInstance<
            BlockchainProvider,
        >,
    token: Token::TokenInstance<BlockchainProvider>,
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

    pub fn sharding_table(
        &self,
    ) -> &sharding_table::ShardingTable::ShardingTableInstance<BlockchainProvider> {
        &self.sharding_table
    }

    pub fn sharding_table_storage(
        &self,
    ) -> &sharding_table_storage::ShardingTableStorage::ShardingTableStorageInstance<
        BlockchainProvider,
    > {
        &self.sharding_table_storage
    }

    pub fn staking(&self) -> &Staking::StakingInstance<BlockchainProvider> {
        &self.staking
    }

    pub fn token(&self) -> &Token::TokenInstance<BlockchainProvider> {
        &self.token
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
                self.sharding_table =
                    sharding_table::ShardingTable::new(contract_address, provider.clone())
            }
            ContractName::ShardingTableStorage => {
                self.sharding_table_storage = sharding_table_storage::ShardingTableStorage::new(
                    contract_address,
                    provider.clone(),
                )
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
        };

        Ok(())
    }
}

// Native async trait (Rust 1.75+)
pub(crate) trait BlockchainCreator {
    async fn new(config: BlockchainConfig) -> Self;

    async fn initialize_provider(
        config: &BlockchainConfig,
    ) -> Result<BlockchainProvider, BlockchainError> {
        let mut tries = 0;
        let mut rpc_number = 0;

        let signer: PrivateKeySigner =
            config
                .evm_operational_wallet_private_key
                .parse()
                .map_err(|e| BlockchainError::InvalidPrivateKey {
                    key_length: config.evm_operational_wallet_private_key.len(),
                    source: e,
                })?;
        let wallet = EthereumWallet::from(signer);

        while tries < config.rpc_endpoints.len() {
            let endpoint = &config.rpc_endpoints[rpc_number];

            if endpoint.starts_with("ws") {
                return Err(BlockchainError::Custom(
                    "websocket RPCs not supported yet".to_string(),
                ));
            }

            let endpoint_url =
                endpoint
                    .parse()
                    .map_err(|e| BlockchainError::HttpProviderCreation {
                        endpoint: endpoint.clone(),
                        source: Box::new(e),
                    })?;

            let provider = Arc::new(
                ProviderBuilder::new()
                    .wallet(wallet.clone())
                    .connect_http(endpoint_url)
                    .erased(),
            );

            match provider.get_block_number().await {
                Ok(_) => {
                    tracing::info!("Blockchain provider initialized with rpc: {}", endpoint);
                    return Ok(provider);
                }
                Err(_) => {
                    tracing::warn!("Unable to connect to blockchain rpc: {}", endpoint);
                    tries += 1;
                    rpc_number = (rpc_number + 1) % config.rpc_endpoints.len();
                }
            }
        }

        Err(BlockchainError::RpcConnectionFailed {
            attempts: config.rpc_endpoints.len(),
        })
    }

    async fn initialize_contracts(
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

        let knowledge_collection_storage_addr = asset_storages_addresses.iter().rev().find_map(
            |contract| match contract.name.parse::<ContractName>() {
                Ok(ContractName::KnowledgeCollectionStorage) => Some(contract.addr),
                _ => None,
            },
        );
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
            sharding_table: sharding_table::ShardingTable::new(
                Address::from(
                    hub.getContractAddress("ShardingTable".to_string())
                        .call()
                        .await?
                        .0,
                ),
                provider.clone(),
            ),
            sharding_table_storage: sharding_table_storage::ShardingTableStorage::new(
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
        })
    }
}
