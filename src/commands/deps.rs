use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_key_value_store::PublishTmpDatasetStore;
use dkg_network::{BatchGetAck, FinalityAck, GetAck, NetworkManager, StoreAck};
use dkg_repository::{FinalityStatusRepository, TriplesInsertCountRepository};

use crate::{
    application::{GetAssertionUseCase, OperationTracking, TripleStoreAssertions},
    operations::{GetOperation, PublishStoreOperation},
    node_state::PeerRegistry,
    node_state::ResponseChannels,
};

#[derive(Clone)]
pub(crate) struct SendPublishStoreRequestsDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) publish_store_operation_tracking: Arc<OperationTracking<PublishStoreOperation>>,
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
}

#[derive(Clone)]
pub(crate) struct HandlePublishStoreRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
    pub(crate) store_response_channels: Arc<ResponseChannels<StoreAck>>,
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
}

#[derive(Clone)]
pub(crate) struct SendPublishFinalityRequestDeps {
    pub(crate) finality_status_repository: FinalityStatusRepository,
    pub(crate) triples_insert_count_repository: TriplesInsertCountRepository,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    pub(crate) triple_store_assertions: Arc<TripleStoreAssertions>,
}

#[derive(Clone)]
pub(crate) struct HandlePublishFinalityRequestDeps {
    pub(crate) finality_status_repository: FinalityStatusRepository,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) finality_response_channels: Arc<ResponseChannels<FinalityAck>>,
}

#[derive(Clone)]
pub(crate) struct SendGetRequestsDeps {
    pub(crate) get_operation_tracking: Arc<OperationTracking<GetOperation>>,
    pub(crate) get_assertion_use_case: Arc<GetAssertionUseCase>,
}

#[derive(Clone)]
pub(crate) struct HandleGetRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) triple_store_assertions: Arc<TripleStoreAssertions>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
    pub(crate) get_response_channels: Arc<ResponseChannels<GetAck>>,
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
}

#[derive(Clone)]
pub(crate) struct HandleBatchGetRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) triple_store_assertions: Arc<TripleStoreAssertions>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
    pub(crate) batch_get_response_channels: Arc<ResponseChannels<BatchGetAck>>,
}
