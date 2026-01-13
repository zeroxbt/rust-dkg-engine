use std::sync::Arc;

use network::{NetworkManager, PeerId, message::RequestMessage};
use repository::RepositoryManager;

use super::{
    operation_response_tracker::OperationResponseTracker,
    sharding_table_service::ShardingTableService,
};
use crate::{
    network::{NetworkProtocols, ProtocolRequest},
    services::file_service::FileService,
    types::{
        protocol::StoreRequestData,
        traits::service::{NetworkOperationProtocol, OperationLifecycle},
    },
};

pub struct PublishService {
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    repository_manager: Arc<RepositoryManager>,
    response_tracker: OperationResponseTracker<StoreRequestData>,
    sharding_table_service: Arc<ShardingTableService>,
    file_service: Arc<FileService>,
}

impl PublishService {
    pub fn new(
        network_manager: Arc<NetworkManager<NetworkProtocols>>,
        repository_manager: Arc<RepositoryManager>,
        sharding_table_service: Arc<ShardingTableService>,
        file_service: Arc<FileService>,
    ) -> Self {
        Self {
            network_manager,
            repository_manager,
            response_tracker: OperationResponseTracker::new(),
            sharding_table_service,
            file_service,
        }
    }
}

impl NetworkOperationProtocol for PublishService {
    type OperationRequestMessageData = StoreRequestData;
    const BATCH_SIZE: u8 = 20;
    const MIN_ACK_RESPONSES: u8 = 8;

    fn sharding_table_service(&self) -> &Arc<ShardingTableService> {
        &self.sharding_table_service
    }

    fn network_manager(&self) -> &Arc<NetworkManager<NetworkProtocols>> {
        &self.network_manager
    }

    fn response_tracker(&self) -> &OperationResponseTracker<Self::OperationRequestMessageData> {
        &self.response_tracker
    }

    async fn send_request(
        &self,
        peer: PeerId,
        message: RequestMessage<Self::OperationRequestMessageData>,
    ) {
        let _ = self
            .network_manager
            .send_protocol_request(ProtocolRequest::store(peer, message))
            .await;
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
