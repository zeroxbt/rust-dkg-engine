use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_key_value_store::{PeerAddressStore, PublishTmpDatasetStore};
use dkg_network::{BatchGetAck, FinalityAck, GetAck, NetworkManager, StoreAck};
use dkg_repository::{
    BlockchainRepository, FinalityStatusRepository, KcSyncRepository, OperationRepository,
    ParanetKcSyncRepository, ProofChallengeRepository,
};

use crate::{
    application::{
        AssertionRetrieval, AssertionValidation, GetAssertionUseCase, OperationTracking,
        ShardPeerSelection, TripleStoreAssertions,
    },
    commands::scheduler::CommandScheduler,
    node_state::PeerRegistry,
    node_state::ResponseChannels,
    operations::{GetOperation, PublishStoreOperation},
};

#[derive(Clone)]
pub(crate) struct DialPeersDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
}

#[derive(Clone)]
pub(crate) struct SavePeerAddressesDeps {
    pub(crate) peer_registry: Arc<PeerRegistry>,
    pub(crate) peer_address_store: Arc<PeerAddressStore>,
}

#[derive(Clone)]
pub(crate) struct ClaimRewardsDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
}

#[derive(Clone)]
pub(crate) struct CleanupDeps {
    pub(crate) operation_repository: OperationRepository,
    pub(crate) finality_status_repository: FinalityStatusRepository,
    pub(crate) proof_challenge_repository: ProofChallengeRepository,
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    pub(crate) publish_operation_tracking: Arc<OperationTracking<PublishStoreOperation>>,
    pub(crate) get_operation_tracking: Arc<OperationTracking<GetOperation>>,
    pub(crate) store_response_channels: Arc<ResponseChannels<StoreAck>>,
    pub(crate) get_response_channels: Arc<ResponseChannels<GetAck>>,
    pub(crate) finality_response_channels: Arc<ResponseChannels<FinalityAck>>,
    pub(crate) batch_get_response_channels: Arc<ResponseChannels<BatchGetAck>>,
}

#[derive(Clone)]
pub(crate) struct ShardingTableCheckDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
}

#[derive(Clone)]
pub(crate) struct BlockchainEventListenerDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) blockchain_repository: BlockchainRepository,
    pub(crate) command_scheduler: CommandScheduler,
}

#[derive(Clone)]
pub(crate) struct ProvingDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) proof_challenge_repository: ProofChallengeRepository,
    pub(crate) assertion_retrieval: Arc<AssertionRetrieval>,
    pub(crate) shard_peer_selection: Arc<ShardPeerSelection>,
}

#[derive(Clone)]
pub(crate) struct SyncDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) kc_sync_repository: KcSyncRepository,
    pub(crate) triple_store_assertions: Arc<TripleStoreAssertions>,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) assertion_validation: Arc<AssertionValidation>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
}

#[derive(Clone)]
pub(crate) struct ParanetSyncDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) paranet_kc_sync_repository: ParanetKcSyncRepository,
    pub(crate) triple_store_assertions: Arc<TripleStoreAssertions>,
    pub(crate) get_assertion_use_case: Arc<GetAssertionUseCase>,
}

#[derive(Clone)]
pub(crate) struct PeriodicTasksDeps {
    pub(crate) dial_peers: DialPeersDeps,
    pub(crate) cleanup: CleanupDeps,
    pub(crate) save_peer_addresses: SavePeerAddressesDeps,
    pub(crate) sharding_table_check: ShardingTableCheckDeps,
    pub(crate) blockchain_event_listener: BlockchainEventListenerDeps,
    pub(crate) claim_rewards: ClaimRewardsDeps,
    pub(crate) proving: ProvingDeps,
    pub(crate) sync: SyncDeps,
    pub(crate) paranet_sync: ParanetSyncDeps,
}
