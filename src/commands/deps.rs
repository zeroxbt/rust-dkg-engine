use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_key_value_store::PublishTmpDatasetStore;
use dkg_network::NetworkManager;
use dkg_repository::{
    FinalityStatusRepository, KcProjectionRepository, OperationRepository,
    TriplesInsertCountRepository,
};

use crate::{
    application::{
        GetAssertionUseCase, KcMaterializationService, OperationTracking, TripleStoreAssertions,
    },
    operations::{GetOperation, PublishStoreOperation},
    peer_registry::PeerRegistry,
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
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
}

#[derive(Clone)]
pub(crate) struct SendPublishFinalityRequestDeps {
    pub(crate) finality_status_repository: FinalityStatusRepository,
    pub(crate) operation_repository: OperationRepository,
    pub(crate) triples_insert_count_repository: TriplesInsertCountRepository,
    pub(crate) kc_projection_repository: KcProjectionRepository,
    pub(crate) kc_materialization_service: Arc<KcMaterializationService>,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
}

#[derive(Clone)]
pub(crate) struct HandlePublishFinalityRequestDeps {
    pub(crate) finality_status_repository: FinalityStatusRepository,
    pub(crate) network_manager: Arc<NetworkManager>,
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
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
}

#[derive(Clone)]
pub(crate) struct HandleBatchGetRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) triple_store_assertions: Arc<TripleStoreAssertions>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
}
