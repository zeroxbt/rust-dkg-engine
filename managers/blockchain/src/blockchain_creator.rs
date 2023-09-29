use crate::BlockchainConfig;
use async_trait::async_trait;
use ethers::{
    abi::Address,
    contract::abigen,
    middleware::{Middleware, MiddlewareBuilder, NonceManagerMiddleware, SignerMiddleware},
    providers::{Http, Provider},
    signers::{LocalWallet, Signer},
};
use std::str::FromStr;
use std::sync::Arc;

abigen!(Hub, "../../abi/Hub.json");
abigen!(ContentAssetStorage, "../../abi/ContentAssetStorage.json");
abigen!(AssertionStorage, "../../abi/AssertionStorage.json");
abigen!(Staking, "../../abi/Staking.json");
abigen!(StakingStorage, "../../abi/StakingStorage.json");
abigen!(Token, "../../abi/Token.json");
abigen!(HashingProxy, "../../abi/HashingProxy.json");
abigen!(IdentityStorage, "../../abi/IdentityStorage.json");
abigen!(Log2PLDSF, "../../abi/Log2PLDSF.json");
abigen!(ParametersStorage, "../../abi/ParametersStorage.json");
abigen!(Profile, "../../abi/Profile.json");
abigen!(ProfileStorage, "../../abi/ProfileStorage.json");
abigen!(ScoringProxy, "../../abi/ScoringProxy.json");
abigen!(ServiceAgreementV1, "../../abi/ServiceAgreementV1.json");
abigen!(CommitManagerV1, "../../abi/CommitManagerV1.json");
abigen!(CommitManagerV1U1, "../../abi/CommitManagerV1U1.json");
abigen!(ProofManagerV1, "../../abi/ProofManagerV1.json");
abigen!(ProofManagerV1U1, "../../abi/ProofManagerV1U1.json");
abigen!(ShardingTable, "../../abi/ShardingTable.json");
abigen!(ShardingTableStorage, "../../abi/ShardingTableStorage.json");
abigen!(
    ServiceAgreementStorageProxy,
    "../../abi/ServiceAgreementStorageProxy.json"
);
abigen!(
    UnfinalizedStateStorage,
    "../../abi/UnfinalizedStateStorage.json"
);

pub type BlockchainProvider = NonceManagerMiddleware<SignerMiddleware<Provider<Http>, LocalWallet>>;

pub struct Contracts {
    hub: Hub<BlockchainProvider>,
    content_asset_storage: ContentAssetStorage<BlockchainProvider>,
    assertion_storage: AssertionStorage<BlockchainProvider>,
    staking: Staking<BlockchainProvider>,
    staking_storage: StakingStorage<BlockchainProvider>,
    token: Token<BlockchainProvider>,
    hashing_proxy: HashingProxy<BlockchainProvider>,
    identity_storage: IdentityStorage<BlockchainProvider>,
    log2_pldsf: Log2PLDSF<BlockchainProvider>,
    parameters_storage: ParametersStorage<BlockchainProvider>,
    profile: Profile<BlockchainProvider>,
    profile_storage: ProfileStorage<BlockchainProvider>,
    scoring_proxy: ScoringProxy<BlockchainProvider>,
    service_agreement_v1: ServiceAgreementV1<BlockchainProvider>,
    commit_manager_v1: CommitManagerV1<BlockchainProvider>,
    commit_manager_v1_u1: CommitManagerV1U1<BlockchainProvider>,
    proof_manager_v1: ProofManagerV1<BlockchainProvider>,
    proof_manager_v1_u1: ProofManagerV1U1<BlockchainProvider>,
    sharding_table: ShardingTable<BlockchainProvider>,
    sharding_table_storage: ShardingTableStorage<BlockchainProvider>,
    service_agreement_storage_proxy: ServiceAgreementStorageProxy<BlockchainProvider>,
    unfinalized_state_storage: UnfinalizedStateStorage<BlockchainProvider>,
}

impl Contracts {
    pub fn hub(&self) -> &Hub<BlockchainProvider> {
        &self.hub
    }

    pub fn content_asset_storage(&self) -> &ContentAssetStorage<BlockchainProvider> {
        &self.content_asset_storage
    }

    pub fn assertion_storage(&self) -> &AssertionStorage<BlockchainProvider> {
        &self.assertion_storage
    }

    pub fn staking(&self) -> &Staking<BlockchainProvider> {
        &self.staking
    }

    pub fn staking_storage(&self) -> &StakingStorage<BlockchainProvider> {
        &self.staking_storage
    }

    pub fn token(&self) -> &Token<BlockchainProvider> {
        &self.token
    }

    pub fn hashing_proxy(&self) -> &HashingProxy<BlockchainProvider> {
        &self.hashing_proxy
    }

    pub fn identity_storage(&self) -> &IdentityStorage<BlockchainProvider> {
        &self.identity_storage
    }

    pub fn log2_pldsf(&self) -> &Log2PLDSF<BlockchainProvider> {
        &self.log2_pldsf
    }

    pub fn parameters_storage(&self) -> &ParametersStorage<BlockchainProvider> {
        &self.parameters_storage
    }

    pub fn profile(&self) -> &Profile<BlockchainProvider> {
        &self.profile
    }

    pub fn profile_storage(&self) -> &ProfileStorage<BlockchainProvider> {
        &self.profile_storage
    }

    pub fn scoring_proxy(&self) -> &ScoringProxy<BlockchainProvider> {
        &self.scoring_proxy
    }

    pub fn service_agreement_v1(&self) -> &ServiceAgreementV1<BlockchainProvider> {
        &self.service_agreement_v1
    }

    pub fn commit_manager_v1(&self) -> &CommitManagerV1<BlockchainProvider> {
        &self.commit_manager_v1
    }

    pub fn commit_manager_v1_u1(&self) -> &CommitManagerV1U1<BlockchainProvider> {
        &self.commit_manager_v1_u1
    }

    pub fn proof_manager_v1(&self) -> &ProofManagerV1<BlockchainProvider> {
        &self.proof_manager_v1
    }

    pub fn proof_manager_v1_u1(&self) -> &ProofManagerV1U1<BlockchainProvider> {
        &self.proof_manager_v1_u1
    }

    pub fn sharding_table(&self) -> &ShardingTable<BlockchainProvider> {
        &self.sharding_table
    }

    pub fn sharding_table_storage(&self) -> &ShardingTableStorage<BlockchainProvider> {
        &self.sharding_table_storage
    }

    pub fn service_agreement_storage_proxy(
        &self,
    ) -> &ServiceAgreementStorageProxy<BlockchainProvider> {
        &self.service_agreement_storage_proxy
    }

    pub fn unfinalized_state_storage(&self) -> &UnfinalizedStateStorage<BlockchainProvider> {
        &self.unfinalized_state_storage
    }
}

#[async_trait]
pub trait BlockchainCreator {
    async fn new(config: BlockchainConfig) -> Self;

    async fn create_ethers_provider(
        config: &BlockchainConfig,
    ) -> Result<Arc<BlockchainProvider>, Box<dyn std::error::Error>> {
        let mut tries = 0;
        let mut rpc_number = 0;

        let signer = config
            .evm_operational_wallet_private_key
            .parse::<LocalWallet>()
            .unwrap();
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
        provider: Arc<BlockchainProvider>,
    ) -> Contracts {
        let address = config.hub_contract_address.parse::<Address>().unwrap();

        let hub = Hub::new(address, provider.clone());

        Contracts {
            hub: hub.clone(),
            content_asset_storage: ContentAssetStorage::new(
                hub.get_asset_storage_address("ContentAssetStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            assertion_storage: AssertionStorage::new(
                hub.get_contract_address("AssertionStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            staking: Staking::new(
                hub.get_contract_address("Staking".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            staking_storage: StakingStorage::new(
                hub.get_contract_address("StakingStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            token: Token::new(
                hub.get_contract_address("Token".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            hashing_proxy: HashingProxy::new(
                hub.get_contract_address("HashingProxy".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            identity_storage: IdentityStorage::new(
                hub.get_contract_address("IdentityStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            log2_pldsf: Log2PLDSF::new(
                hub.get_contract_address("Log2PLDSF".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            parameters_storage: ParametersStorage::new(
                hub.get_contract_address("ParametersStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            profile: Profile::new(
                hub.get_contract_address("Profile".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            profile_storage: ProfileStorage::new(
                hub.get_contract_address("ProfileStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            scoring_proxy: ScoringProxy::new(
                hub.get_contract_address("ScoringProxy".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            service_agreement_v1: ServiceAgreementV1::new(
                hub.get_contract_address("ServiceAgreementV1".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            commit_manager_v1: CommitManagerV1::new(
                hub.get_contract_address("CommitManagerV1".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            commit_manager_v1_u1: CommitManagerV1U1::new(
                hub.get_contract_address("CommitManagerV1U1".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            proof_manager_v1: ProofManagerV1::new(
                hub.get_contract_address("ProofManagerV1".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            proof_manager_v1_u1: ProofManagerV1U1::new(
                hub.get_contract_address("ProofManagerV1U1".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            sharding_table: ShardingTable::new(
                hub.get_contract_address("ShardingTable".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            sharding_table_storage: ShardingTableStorage::new(
                hub.get_contract_address("ShardingTableStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            service_agreement_storage_proxy: ServiceAgreementStorageProxy::new(
                hub.get_contract_address("ServiceAgreementStorageProxy".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
            unfinalized_state_storage: UnfinalizedStateStorage::new(
                hub.get_contract_address("UnfinalizedStateStorage".to_string())
                    .call()
                    .await
                    .unwrap(),
                Arc::clone(&provider),
            ),
        }
    }
}
