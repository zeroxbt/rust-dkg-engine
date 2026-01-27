pub(crate) mod file_service;
pub(crate) mod get_validation_service;
pub(crate) mod operation;
pub(crate) mod peer_discovery_tracker;
pub(crate) mod pending_storage_service;
pub(crate) mod response_channels;
pub(crate) mod triple_store_service;

use std::sync::Arc;

use crate::{
    config::AppPaths,
    managers::{NetworkManager, RepositoryManager, key_value_store::KeyValueStoreManager},
    operations::{BatchGetOperation, GetOperation, PublishOperation},
};

pub(crate) use get_validation_service::GetValidationService;
pub(crate) use operation::OperationService;
pub(crate) use peer_discovery_tracker::PeerDiscoveryTracker;
pub(crate) use pending_storage_service::PendingStorageService;
pub(crate) use response_channels::ResponseChannels;
pub(crate) use triple_store_service::TripleStoreService;

/// Container for all initialized services.
pub(crate) struct Services {
    pub publish_operation: Arc<OperationService<PublishOperation>>,
    pub get_operation: Arc<OperationService<GetOperation>>,
    pub batch_get_operation: Arc<OperationService<BatchGetOperation>>,
    pub pending_storage: Arc<PendingStorageService>,
}

/// Initialize all services.
pub(crate) fn initialize(
    paths: &AppPaths,
    repository_manager: &Arc<RepositoryManager>,
    network_manager: &Arc<NetworkManager>,
) -> Services {
    // Create shared key-value store manager using centralized path
    let kv_store_manager = KeyValueStoreManager::connect(paths.key_value_store.clone())
        .expect("Failed to connect to key-value store manager");

    let pending_storage = Arc::new(
        PendingStorageService::new(&kv_store_manager)
            .expect("Failed to create pending storage service"),
    );

    let publish_operation = Arc::new(
        OperationService::<PublishOperation>::new(
            Arc::clone(repository_manager),
            Arc::clone(network_manager),
            &kv_store_manager,
        )
        .expect("Failed to create publish operation service"),
    );

    let get_operation = Arc::new(
        OperationService::<GetOperation>::new(
            Arc::clone(repository_manager),
            Arc::clone(network_manager),
            &kv_store_manager,
        )
        .expect("Failed to create get operation service"),
    );

    let batch_get_operation = Arc::new(
        OperationService::<BatchGetOperation>::new(
            Arc::clone(repository_manager),
            Arc::clone(network_manager),
            &kv_store_manager,
        )
        .expect("Failed to create batch get operation service"),
    );

    Services {
        publish_operation,
        get_operation,
        batch_get_operation,
        pending_storage,
    }
}
