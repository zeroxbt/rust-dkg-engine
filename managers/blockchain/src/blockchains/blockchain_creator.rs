use std::{collections::HashMap, str::FromStr, sync::Arc};

use ethers::{
    abi::Address,
    contract::{Contract as EthersContract, abigen},
    middleware::{Middleware, MiddlewareBuilder, NonceManagerMiddleware, SignerMiddleware},
    providers::{Http, Provider},
    signers::{LocalWallet, Signer},
};

use crate::{BlockchainConfig, ContractName, error::BlockchainError};

abigen!(Hub, "../../abi/Hub.json");
abigen!(Staking, "../../abi/Staking.json");
abigen!(IdentityStorage, "../../abi/IdentityStorage.json");
abigen!(ParametersStorage, "../../abi/ParametersStorage.json");
abigen!(
    KnowledgeCollectionStorage,
    "../../abi/KnowledgeCollectionStorage.json"
);
abigen!(Profile, "../../abi/Profile.json");
abigen!(ShardingTable, "../../abi/ShardingTable.json");
// abigen!(
// ServiceAgreementStorageProxy,
// "../../abi/ServiceAgreementStorageProxy.json"
// );
// abigen!(
// UnfinalizedStateStorage,
// "../../abi/UnfinalizedStateStorage.json"
// );

pub type BlockchainProvider = NonceManagerMiddleware<SignerMiddleware<Provider<Http>, LocalWallet>>;

pub struct Contracts {
    hub: Hub<BlockchainProvider>,
    knowledge_collection_storages: HashMap<Address, KnowledgeCollectionStorage<BlockchainProvider>>,
    staking: Staking<BlockchainProvider>,
    identity_storage: IdentityStorage<BlockchainProvider>,
    parameters_storage: ParametersStorage<BlockchainProvider>,
    profile: Profile<BlockchainProvider>,
    sharding_table: ShardingTable<BlockchainProvider>,
}

impl Contracts {
    pub fn identity_storage(&self) -> &IdentityStorage<BlockchainProvider> {
        &self.identity_storage
    }

    pub fn profile(&self) -> &Profile<BlockchainProvider> {
        &self.profile
    }

    pub fn get(
        &self,
        contract_name: &ContractName,
    ) -> Result<&EthersContract<BlockchainProvider>, BlockchainError> {
        match contract_name {
            ContractName::Hub => Ok(&self.hub),
            ContractName::ShardingTable => Ok(&self.sharding_table),
            ContractName::Staking => Ok(&self.staking),
            ContractName::Profile => Ok(&self.profile),
            ContractName::ParametersStorage => Ok(&self.parameters_storage),
            ContractName::KnowledgeCollectionStorage => Err(BlockchainError::Custom(
                "KnowledgeCollectionStorage contracts must be accessed via get_knowledge_collection_storage"
                    .to_string(),
            )),
            // Legacy contracts not currently in use
            ContractName::CommitManagerV1U1
            | ContractName::ServiceAgreementV1
            | ContractName::ContentAssetStorage => Err(BlockchainError::Custom(format!(
                "Contract {} is not currently initialized",
                contract_name.as_str()
            ))),
        }
    }

    pub fn get_knowledge_collection_storage(
        &self,
        address: &Address,
    ) -> Option<&KnowledgeCollectionStorage<BlockchainProvider>> {
        self.knowledge_collection_storages.get(address)
    }

    pub fn get_knowledge_collection_storage_addresses(&self) -> Vec<Address> {
        self.knowledge_collection_storages.keys().cloned().collect()
    }

    pub async fn replace_contract(
        &mut self,
        provider: &Arc<BlockchainProvider>,
        contract_name: ContractName,
        contract_address: Address,
    ) -> Result<(), BlockchainError> {
        match contract_name {
            ContractName::Profile => {
                self.profile = Profile::new(contract_address, Arc::clone(provider))
            }
            ContractName::ShardingTable => {
                self.sharding_table = ShardingTable::new(contract_address, Arc::clone(provider))
            }
            ContractName::Hub => {
                self.hub = Hub::new(contract_address, Arc::clone(provider));
            }
            ContractName::Staking => {
                self.staking = Staking::new(contract_address, Arc::clone(provider));
            }
            ContractName::ParametersStorage => {
                self.parameters_storage =
                    ParametersStorage::new(contract_address, Arc::clone(provider));
            }
            ContractName::KnowledgeCollectionStorage => {
                self.knowledge_collection_storages.insert(
                    contract_address,
                    KnowledgeCollectionStorage::new(contract_address, Arc::clone(provider)),
                );
            }
            // Legacy contracts - log warning but don't fail
            ContractName::CommitManagerV1U1
            | ContractName::ServiceAgreementV1
            | ContractName::ContentAssetStorage => {
                tracing::warn!(
                    "Contract {} is not currently supported for re-initialization",
                    contract_name.as_str()
                );
            }
        };

        Ok(())
    }
}

// Native async trait (Rust 1.75+)
pub(crate) trait BlockchainCreator {
    async fn new(config: BlockchainConfig) -> Self;

    async fn initialize_ethers_provider(
        config: &BlockchainConfig,
    ) -> Result<Arc<BlockchainProvider>, BlockchainError> {
        let mut tries = 0;
        let mut rpc_number = 0;

        let signer = config
            .evm_operational_wallet_private_key
            .parse::<LocalWallet>()
            .map_err(|e| BlockchainError::InvalidPrivateKey {
                key_length: config.evm_operational_wallet_private_key.len(),
                source: e,
            })?
            .with_chain_id(config.chain_id);
        let signer_address = signer.address();

        while tries < config.rpc_endpoints.len() {
            let cloned_signer = signer.clone();
            let endpoint = &config.rpc_endpoints[rpc_number];

            let current_provider = if endpoint.starts_with("ws") {
                return Err(BlockchainError::Custom(
                    "websocket RPCs not supported yet".to_string(),
                ));
            } else {
                let http = Http::from_str(endpoint).map_err(|e| {
                    BlockchainError::HttpProviderCreation {
                        endpoint: endpoint.clone(),
                        source: Box::new(e),
                    }
                })?;
                Arc::new(
                    Provider::new(http)
                        .with_signer(cloned_signer)
                        .nonce_manager(signer_address),
                )
            };

            if current_provider.get_block_number().await.is_ok() {
                tracing::info!("Blockchain provider initialized with rpc: {}", endpoint);

                return Ok(current_provider);
            } else {
                tracing::warn!("Unable to connect to blockchain rpc: {}", endpoint);
                tries += 1;
                rpc_number = (rpc_number + 1) % config.rpc_endpoints.len();
            }
        }

        Err(BlockchainError::RpcConnectionFailed {
            attempts: config.rpc_endpoints.len(),
        })
    }

    async fn initialize_contracts(
        config: &BlockchainConfig,
        provider: &Arc<BlockchainProvider>,
    ) -> Result<Contracts, BlockchainError> {
        let address = config
            .hub_contract_address
            .parse::<Address>()
            .map_err(|_| BlockchainError::InvalidAddress {
                address: config.hub_contract_address.clone(),
            })?;
        let hub = Hub::new(address, provider.clone());

        let asset_storages_addresses = hub.get_all_asset_storages().call().await?;

        let knowledge_collection_storages: HashMap<
            Address,
            KnowledgeCollectionStorage<BlockchainProvider>,
        > = asset_storages_addresses
            .iter()
            .filter_map(|contract| match contract.name.parse::<ContractName>() {
                Ok(ContractName::KnowledgeCollectionStorage) => Some((
                    contract.addr,
                    KnowledgeCollectionStorage::new(contract.addr, Arc::clone(provider)),
                )),
                _ => None,
            })
            .collect();

        Ok(Contracts {
            hub: hub.clone(),
            knowledge_collection_storages,
            staking: Staking::new(
                hub.get_contract_address("Staking".to_string())
                    .call()
                    .await?,
                Arc::clone(provider),
            ),
            identity_storage: IdentityStorage::new(
                hub.get_contract_address("IdentityStorage".to_string())
                    .call()
                    .await?,
                Arc::clone(provider),
            ),
            parameters_storage: ParametersStorage::new(
                hub.get_contract_address("ParametersStorage".to_string())
                    .call()
                    .await?,
                Arc::clone(provider),
            ),
            profile: Profile::new(
                hub.get_contract_address("Profile".to_string())
                    .call()
                    .await?,
                Arc::clone(provider),
            ),
            sharding_table: ShardingTable::new(
                hub.get_contract_address("ShardingTable".to_string())
                    .call()
                    .await?,
                Arc::clone(provider),
            ),
        })
    }
}
