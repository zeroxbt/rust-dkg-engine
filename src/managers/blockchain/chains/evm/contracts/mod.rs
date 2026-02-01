pub(crate) mod chronos;
pub(crate) mod hub;
pub(crate) mod identity_storage;
pub(crate) mod kc_storage;
pub(crate) mod multicall;
pub(crate) mod params;
pub(crate) mod paranet;
pub(crate) mod profile;
pub(crate) mod random_sampling;
pub(crate) mod random_sampling_storage;
pub(crate) mod sharding;
pub(crate) mod staking;
pub(crate) mod token;

use std::collections::HashMap;

use alloy::primitives::Address;
pub(crate) use chronos::Chronos;
pub(crate) use hub::Hub;
pub(crate) use identity_storage::IdentityStorage;
pub(crate) use kc_storage::KnowledgeCollectionStorage;
pub(crate) use multicall::Multicall3;
pub(crate) use params::ParametersStorage;
pub(crate) use paranet::{ParanetsRegistry, PermissionedNode};
pub(crate) use profile::Profile;
pub(crate) use random_sampling::RandomSampling;
pub(crate) use random_sampling_storage::RandomSamplingStorage;
pub(crate) use sharding::{ShardingTable, ShardingTableLib, ShardingTableStorage};
pub(crate) use staking::Staking;
pub(crate) use token::Token;

use super::provider::BlockchainProvider;
use crate::managers::blockchain::{BlockchainConfig, ContractName, error::BlockchainError};

/// Multicall3 contract address - standard address on most EVM chains.
/// See: https://www.multicall3.com/
const MULTICALL3_ADDRESS: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";
/// NeuroWeb testnet (otp:20430) Multicall3 address.
const NEUROWEB_TESTNET_MULTICALL3_ADDRESS: &str = "0x13e970420f3534A9C25309ebF81b22f8294D6162";
/// NeuroWeb mainnet (otp:2043) Multicall3 address.
const NEUROWEB_MAINNET_MULTICALL3_ADDRESS: &str = "0xDd0192E1C876ec50F08F4D20f121283cd89985a5";

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
    multicall3: Multicall3::Multicall3Instance<BlockchainProvider>,
    random_sampling: RandomSampling::RandomSamplingInstance<BlockchainProvider>,
    random_sampling_storage:
        RandomSamplingStorage::RandomSamplingStorageInstance<BlockchainProvider>,
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

    pub(crate) fn multicall3(&self) -> &Multicall3::Multicall3Instance<BlockchainProvider> {
        &self.multicall3
    }

    pub(crate) fn random_sampling(
        &self,
    ) -> &RandomSampling::RandomSamplingInstance<BlockchainProvider> {
        &self.random_sampling
    }

    pub(crate) fn random_sampling_storage(
        &self,
    ) -> &RandomSamplingStorage::RandomSamplingStorageInstance<BlockchainProvider> {
        &self.random_sampling_storage
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
        multicall3: Multicall3::new(
            {
                let address = match config.blockchain_id().chain_id() {
                    Some(20430) => NEUROWEB_TESTNET_MULTICALL3_ADDRESS,
                    Some(2043) => NEUROWEB_MAINNET_MULTICALL3_ADDRESS,
                    _ => MULTICALL3_ADDRESS,
                };
                address
                    .parse()
                    .map_err(|_| BlockchainError::InvalidAddress {
                        address: address.to_string(),
                    })?
            },
            provider.clone(),
        ),
        random_sampling: RandomSampling::new(
            Address::from(
                hub.getContractAddress("RandomSampling".to_string())
                    .call()
                    .await?
                    .0,
            ),
            provider.clone(),
        ),
        random_sampling_storage: RandomSamplingStorage::new(
            Address::from(
                hub.getContractAddress("RandomSamplingStorage".to_string())
                    .call()
                    .await?
                    .0,
            ),
            provider.clone(),
        ),
    })
}
