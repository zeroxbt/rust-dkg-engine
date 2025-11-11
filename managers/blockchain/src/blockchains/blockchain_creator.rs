use crate::{BlockchainConfig, ContractName};
use async_trait::async_trait;
use ethers::{
    abi::Address,
    contract::abigen,
    contract::Contract,
    middleware::{Middleware, MiddlewareBuilder, NonceManagerMiddleware, SignerMiddleware},
    providers::{Http, Provider},
    signers::{LocalWallet, Signer},
};
use std::sync::Arc;
use std::{collections::HashMap, str::FromStr};

use super::abstract_blockchain::BlockchainError;

abigen!(Hub, "../../abi/Hub.json");
abigen!(ContentAssetStorage, "../../abi/ContentAssetStorage.json");
// abigen!(AssertionStorage, "../../abi/AssertionStorage.json");
abigen!(Staking, "../../abi/Staking.json");
// abigen!(StakingStorage, "../../abi/StakingStorage.json");
// abigen!(Token, "../../abi/Token.json");
// abigen!(HashingProxy, "../../abi/HashingProxy.json");
abigen!(IdentityStorage, "../../abi/IdentityStorage.json");
// abigen!(Log2PLDSF, "../../abi/Log2PLDSF.json");
// abigen!(ParametersStorage, "../../abi/ParametersStorage.json");
abigen!(Profile, "../../abi/Profile.json");
// abigen!(ProfileStorage, "../../abi/ProfileStorage.json");
// abigen!(ScoringProxy, "../../abi/ScoringProxy.json");
abigen!(ServiceAgreementV1, "../../abi/ServiceAgreementV1.json");
// abigen!(CommitManagerV1, "../../abi/CommitManagerV1.json");
abigen!(CommitManagerV1U1, "../../abi/CommitManagerV1U1.json");
// abigen!(ProofManagerV1, "../../abi/ProofManagerV1.json");
// abigen!(ProofManagerV1U1, "../../abi/ProofManagerV1U1.json");
abigen!(ShardingTable, "../../abi/ShardingTable.json");
/* abigen!(ShardingTableStorage, "../../abi/ShardingTableStorage.json");
abigen!(
    ServiceAgreementStorageProxy,
    "../../abi/ServiceAgreementStorageProxy.json"
);
abigen!(
    UnfinalizedStateStorage,
    "../../abi/UnfinalizedStateStorage.json"
); */

pub type BlockchainProvider = NonceManagerMiddleware<SignerMiddleware<Provider<Http>, LocalWallet>>;

pub struct Contracts {
    hub: Hub<BlockchainProvider>,
    content_asset_storages: HashMap<Address, ContentAssetStorage<BlockchainProvider>>,
    //  assertion_storage: AssertionStorage<BlockchainProvider>,
    staking: Staking<BlockchainProvider>,
    //  staking_storage: StakingStorage<BlockchainProvider>,
    //  token: Token<BlockchainProvider>,
    //  hashing_proxy: HashingProxy<BlockchainProvider>,
    identity_storage: IdentityStorage<BlockchainProvider>,
    //  log2_pldsf: Log2PLDSF<BlockchainProvider>,
    //  parameters_storage: ParametersStorage<BlockchainProvider>,
    profile: Profile<BlockchainProvider>,
    //  profile_storage: ProfileStorage<BlockchainProvider>,
    //  scoring_proxy: ScoringProxy<BlockchainProvider>,
    service_agreement_v1: ServiceAgreementV1<BlockchainProvider>,
    // commit_manager_v1: CommitManagerV1<BlockchainProvider>,
    commit_manager_v1_u1: CommitManagerV1U1<BlockchainProvider>,
    //  proof_manager_v1: ProofManagerV1<BlockchainProvider>,
    //  proof_manager_v1_u1: ProofManagerV1U1<BlockchainProvider>,
    sharding_table: ShardingTable<BlockchainProvider>,
    // sharding_table_storage: ShardingTableStorage<BlockchainProvider>,
    // service_agreement_storage_proxy: ServiceAgreementStorageProxy<BlockchainProvider>,
    // unfinalized_state_storage: UnfinalizedStateStorage<BlockchainProvider>,
}

impl Contracts {
    pub fn identity_storage(&self) -> &IdentityStorage<BlockchainProvider> {
        &self.identity_storage
    }

    pub fn profile(&self) -> &Profile<BlockchainProvider> {
        &self.profile
    }

    pub fn get(&self, contract_name: &ContractName) -> &Contract<BlockchainProvider> {
        match contract_name {
            ContractName::Hub => &self.hub,
            ContractName::ShardingTable => &self.sharding_table,
            ContractName::Staking => &self.staking,
            ContractName::CommitManagerV1U1 => &self.commit_manager_v1_u1,
            ContractName::Profile => &self.profile,
            ContractName::ServiceAgreementV1 => &self.service_agreement_v1,
            _ => panic!("Unexpected contract"),
        }
    }

    pub fn get_content_asset_storage(
        &self,
        address: &Address,
    ) -> Option<&ContentAssetStorage<BlockchainProvider>> {
        self.content_asset_storages.get(address)
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
            ContractName::CommitManagerV1U1 => {
                self.commit_manager_v1_u1 =
                    CommitManagerV1U1::new(contract_address, Arc::clone(provider));
            }
            ContractName::ServiceAgreementV1 => {
                self.service_agreement_v1 =
                    ServiceAgreementV1::new(contract_address, Arc::clone(provider));
            }
            ContractName::ContentAssetStorage => {
                self.content_asset_storages.insert(
                    contract_address,
                    ContentAssetStorage::new(contract_address, Arc::clone(provider)),
                );
            }
        };

        Ok(())
    }
}

#[async_trait]
pub trait BlockchainCreator {
    async fn new(config: BlockchainConfig) -> Self;

    async fn initialize_ethers_provider(
        config: &BlockchainConfig,
    ) -> Result<Arc<BlockchainProvider>, Box<dyn std::error::Error>> {
        let mut tries = 0;
        let mut rpc_number = 0;

        let signer = config
            .evm_operational_wallet_private_key
            .parse::<LocalWallet>()
            .unwrap()
            .with_chain_id(config.chain_id);
        let signer_address = signer.address();

        while tries < config.rpc_endpoints.len() {
            let cloned_signer = signer.clone();
            let endpoint = &config.rpc_endpoints[rpc_number];

            let current_provider = if endpoint.starts_with("ws") {
                panic!("websocket RPCs not supported yet");
            } else {
                let http = Http::from_str(endpoint)?;
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

        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "RPC initialization failed",
        )))
    }

    async fn initialize_contracts(
        config: &BlockchainConfig,
        provider: &Arc<BlockchainProvider>,
    ) -> Contracts {
        let address = config.hub_contract_address.parse::<Address>().unwrap();
        let hub = Hub::new(address, provider.clone());

        let asset_storages_addresses = hub.get_all_asset_storages().call().await.unwrap();

        let content_asset_storages: HashMap<Address, ContentAssetStorage<BlockchainProvider>> =
            asset_storages_addresses
                .iter()
                .filter_map(|contract| match contract.name.parse::<ContractName>() {
                    Ok(ContractName::ContentAssetStorage) => Some((
                        contract.addr,
                        ContentAssetStorage::new(contract.addr, Arc::clone(provider)),
                    )),
                    _ => None,
                })
                .collect();

        Contracts {
            hub: hub.clone(),
            content_asset_storages,
            /*
            assertion_storage: AssertionStorage::new(
                hub.get_contract_address("AssertionStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ), */
            staking: Staking::new(
                hub.get_contract_address("Staking".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            /* staking_storage: StakingStorage::new(
                hub.get_contract_address("StakingStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            token: Token::new(
                hub.get_contract_address("Token".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            hashing_proxy: HashingProxy::new(
                hub.get_contract_address("HashingProxy".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ), */
            identity_storage: IdentityStorage::new(
                hub.get_contract_address("IdentityStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            /*  log2_pldsf: Log2PLDSF::new(
                hub.get_contract_address("Log2PLDSF".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            parameters_storage: ParametersStorage::new(
                hub.get_contract_address("ParametersStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ), */
            profile: Profile::new(
                hub.get_contract_address("Profile".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            /*  profile_storage: ProfileStorage::new(
                hub.get_contract_address("ProfileStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            scoring_proxy: ScoringProxy::new(
                hub.get_contract_address("ScoringProxy".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ), */
            service_agreement_v1: ServiceAgreementV1::new(
                hub.get_contract_address("ServiceAgreementV1".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            /*  commit_manager_v1: CommitManagerV1::new(
                hub.get_contract_address("CommitManagerV1".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ), */
            commit_manager_v1_u1: CommitManagerV1U1::new(
                hub.get_contract_address("CommitManagerV1U1".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            /*   proof_manager_v1: ProofManagerV1::new(
                hub.get_contract_address("ProofManagerV1".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            proof_manager_v1_u1: ProofManagerV1U1::new(
                hub.get_contract_address("ProofManagerV1U1".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),*/
            sharding_table: ShardingTable::new(
                hub.get_contract_address("ShardingTable".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            /*    sharding_table_storage: ShardingTableStorage::new(
                hub.get_contract_address("ShardingTableStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            service_agreement_storage_proxy: ServiceAgreementStorageProxy::new(
                hub.get_contract_address("ServiceAgreementStorageProxy".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ),
            unfinalized_state_storage: UnfinalizedStateStorage::new(
                hub.get_contract_address("UnfinalizedStateStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(provider),
            ), */
        }
    }
}
