use std::sync::Arc;

use network::{PeerId, message::RequestMessage};
use repository::RepositoryManager;

use super::{
    operation_service::OperationResponseTracker, sharding_table_service::ShardingTableService,
};
use crate::{
    network::NetworkHandle,
    services::file_service::FileService,
    types::{
        protocol::StoreRequestData,
        traits::service::{NetworkOperationProtocol, OperationLifecycle},
    },
};

pub struct PublishService {
    network_handle: Arc<NetworkHandle>,
    repository_manager: Arc<RepositoryManager>,
    response_tracker: OperationResponseTracker<StoreRequestData>,
    sharding_table_service: Arc<ShardingTableService>,
    file_service: Arc<FileService>,
}

impl PublishService {
    pub fn new(
        network_handle: Arc<NetworkHandle>,
        repository_manager: Arc<RepositoryManager>,
        sharding_table_service: Arc<ShardingTableService>,
        file_service: Arc<FileService>,
    ) -> Self {
        Self {
            network_handle,
            repository_manager,
            response_tracker: OperationResponseTracker::new(),
            sharding_table_service,
            file_service,
        }
    }
}

impl NetworkOperationProtocol for PublishService {
    type OperationRequestMessageData = StoreRequestData;
    const BATCH_SIZE: usize = 20;
    const MIN_ACK_RESPONSES: usize = 8;

    fn sharding_table_service(&self) -> &Arc<ShardingTableService> {
        &self.sharding_table_service
    }

    fn network_handle(&self) -> &Arc<NetworkHandle> {
        &self.network_handle
    }

    fn response_tracker(&self) -> &OperationResponseTracker<Self::OperationRequestMessageData> {
        &self.response_tracker
    }

    async fn send_request(
        &self,
        peer: PeerId,
        message: RequestMessage<Self::OperationRequestMessageData>,
    ) {
        let _ = self.network_handle.send_store_request(peer, message).await;
    }
}

impl OperationLifecycle for PublishService {
    type ResultData = ();
    const OPERATION_NAME: &'static str = "publish";

    fn repository_manager(&self) -> &Arc<RepositoryManager> {
        &self.repository_manager
    }

    fn file_service(&self) -> &Arc<FileService> {
        &self.file_service
    }
}
