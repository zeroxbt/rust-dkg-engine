use std::sync::Arc;

use network::{
    PeerId,
    action::NetworkAction,
    message::{RequestMessage, StoreRequestData},
};
use repository::RepositoryManager;
use tokio::sync::mpsc::Sender;

use super::{
    operation_service::OperationResponseTracker, sharding_table_service::ShardingTableService,
};
use crate::{
    services::file_service::FileService,
    types::traits::service::{NetworkOperationProtocol, OperationLifecycle},
};

pub struct PublishService {
    network_action_tx: Sender<NetworkAction>,
    repository_manager: Arc<RepositoryManager>,
    response_tracker: OperationResponseTracker<StoreRequestData>,
    sharding_table_service: Arc<ShardingTableService>,
    file_service: Arc<FileService>,
}

impl PublishService {
    pub fn new(
        network_action_tx: Sender<NetworkAction>,
        repository_manager: Arc<RepositoryManager>,
        sharding_table_service: Arc<ShardingTableService>,
        file_service: Arc<FileService>,
    ) -> Self {
        Self {
            network_action_tx,
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
    fn network_action_tx(&self) -> &Sender<NetworkAction> {
        &self.network_action_tx
    }
    fn response_tracker(&self) -> &OperationResponseTracker<Self::OperationRequestMessageData> {
        &self.response_tracker
    }
    fn create_network_action(
        &self,
        peer: PeerId,
        message: RequestMessage<Self::OperationRequestMessageData>,
    ) -> NetworkAction {
        NetworkAction::StoreRequest { peer, message }
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
