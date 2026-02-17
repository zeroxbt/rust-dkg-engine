use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_key_value_store::PublishTmpDatasetStore;
use dkg_network::{BatchGetAck, FinalityAck, GetAck, NetworkManager, StoreAck};
use dkg_repository::RepositoryManager;

use crate::{
    commands::scheduler::CommandScheduler,
    operations::{GetOperation, PublishStoreOperation},
    services::{
        AssertionValidationService, OperationStatusService, PeerAddressStore, PeerService,
        TripleStoreService,
    },
    state::ResponseChannels,
};

#[derive(Clone)]
pub(crate) struct DialPeersDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) peer_service: Arc<PeerService>,
}

#[derive(Clone)]
pub(crate) struct SavePeerAddressesDeps {
    pub(crate) peer_service: Arc<PeerService>,
    pub(crate) peer_address_store: Arc<PeerAddressStore>,
}

#[derive(Clone)]
pub(crate) struct ClaimRewardsDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
}

#[derive(Clone)]
pub(crate) struct CleanupDeps {
    pub(crate) repository_manager: Arc<RepositoryManager>,
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    pub(crate) publish_operation_results: Arc<OperationStatusService<PublishStoreOperation>>,
    pub(crate) get_operation_results: Arc<OperationStatusService<GetOperation>>,
    pub(crate) store_response_channels: Arc<ResponseChannels<StoreAck>>,
    pub(crate) get_response_channels: Arc<ResponseChannels<GetAck>>,
    pub(crate) finality_response_channels: Arc<ResponseChannels<FinalityAck>>,
    pub(crate) batch_get_response_channels: Arc<ResponseChannels<BatchGetAck>>,
}

#[derive(Clone)]
pub(crate) struct ShardingTableCheckDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) peer_service: Arc<PeerService>,
}

#[derive(Clone)]
pub(crate) struct BlockchainEventListenerDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) repository_manager: Arc<RepositoryManager>,
    pub(crate) command_scheduler: CommandScheduler,
}

#[derive(Clone)]
pub(crate) struct ProvingDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) repository_manager: Arc<RepositoryManager>,
    pub(crate) triple_store_service: Arc<TripleStoreService>,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) assertion_validation_service: Arc<AssertionValidationService>,
    pub(crate) peer_service: Arc<PeerService>,
}
