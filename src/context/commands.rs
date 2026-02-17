use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_key_value_store::PublishTmpDatasetStore;
use dkg_network::{BatchGetAck, FinalityAck, GetAck, NetworkManager, StoreAck};
use dkg_repository::RepositoryManager;

use crate::{
    operations::{GetOperation, PublishStoreOperation},
    services::{
        GetFetchService, OperationStatusService, PeerService, TripleStoreService,
        operation_status::OperationStatusService as GenericOperationService,
    },
    state::ResponseChannels,
};

#[derive(Clone)]
pub(crate) struct SendPublishStoreRequestsDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) peer_service: Arc<PeerService>,
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) publish_store_operation_status_service:
        Arc<GenericOperationService<PublishStoreOperation>>,
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
}

#[derive(Clone)]
pub(crate) struct HandlePublishStoreRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) peer_service: Arc<PeerService>,
    pub(crate) store_response_channels: Arc<ResponseChannels<StoreAck>>,
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
}

#[derive(Clone)]
pub(crate) struct SendPublishFinalityRequestDeps {
    pub(crate) repository_manager: Arc<RepositoryManager>,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) peer_service: Arc<PeerService>,
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    pub(crate) triple_store_service: Arc<TripleStoreService>,
}

#[derive(Clone)]
pub(crate) struct HandlePublishFinalityRequestDeps {
    pub(crate) repository_manager: Arc<RepositoryManager>,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) finality_response_channels: Arc<ResponseChannels<FinalityAck>>,
}

#[derive(Clone)]
pub(crate) struct SendGetRequestsDeps {
    pub(crate) get_operation_status_service: Arc<OperationStatusService<GetOperation>>,
    pub(crate) get_fetch_service: Arc<GetFetchService>,
}

#[derive(Clone)]
pub(crate) struct HandleGetRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) triple_store_service: Arc<TripleStoreService>,
    pub(crate) peer_service: Arc<PeerService>,
    pub(crate) get_response_channels: Arc<ResponseChannels<GetAck>>,
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
}

#[derive(Clone)]
pub(crate) struct HandleBatchGetRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) triple_store_service: Arc<TripleStoreService>,
    pub(crate) peer_service: Arc<PeerService>,
    pub(crate) batch_get_response_channels: Arc<ResponseChannels<BatchGetAck>>,
}
