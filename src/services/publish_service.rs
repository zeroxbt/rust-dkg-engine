use network::{
    action::NetworkAction,
    message::{RequestMessage, StoreRequestData},
    PeerId,
};
use repository::RepositoryManager;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use super::{
    operation_service::{OperationResponseTracker, OperationService},
    sharding_table_service::ShardingTableService,
};

pub struct PublishService {
    network_action_tx: Sender<NetworkAction>,
    repository_manager: Arc<RepositoryManager>,
    response_tracker: OperationResponseTracker<StoreRequestData>,
    sharding_table_service: Arc<ShardingTableService>,
}

impl OperationService for PublishService {
    type OperationRequestMessageData = StoreRequestData;
    const BATCH_SIZE: usize = 20;
    const MIN_ACK_RESPONSES: usize = 8;

    fn new(
        network_action_tx: Sender<NetworkAction>,
        repository_manager: Arc<RepositoryManager>,
        sharding_table_service: Arc<ShardingTableService>,
    ) -> Self {
        Self {
            network_action_tx,
            repository_manager,
            response_tracker: OperationResponseTracker::<Self::OperationRequestMessageData>::new(),
            sharding_table_service,
        }
    }
    fn repository_manager(&self) -> &Arc<RepositoryManager> {
        &self.repository_manager
    }
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
