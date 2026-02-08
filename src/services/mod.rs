pub(crate) mod file_service;
pub(crate) mod get_validation_service;
pub(crate) mod operation_status;
pub(crate) mod peer;
pub(crate) mod response_channels;
pub(crate) mod triple_store_service;

use std::sync::Arc;

pub(crate) use get_validation_service::GetValidationService;
pub(crate) use operation_status::OperationStatusService;
pub(crate) use peer::{PeerAddressStore, PeerService};
pub(crate) use response_channels::ResponseChannels;
pub(crate) use triple_store_service::TripleStoreService;

use crate::{
    managers::{
        Managers,
        network::messages::{BatchGetAck, FinalityAck, GetAck, StoreAck},
    },
    operations::{GetOperationResult, PublishStoreOperationResult, protocols},
};

/// Response channels for all protocol types.
pub(crate) struct ResponseChannelsSet {
    pub store: Arc<ResponseChannels<StoreAck>>,
    pub get: Arc<ResponseChannels<GetAck>>,
    pub finality: Arc<ResponseChannels<FinalityAck>>,
    pub batch_get: Arc<ResponseChannels<BatchGetAck>>,
}

impl ResponseChannelsSet {
    fn new() -> Self {
        Self {
            store: Arc::new(ResponseChannels::new()),
            get: Arc::new(ResponseChannels::new()),
            finality: Arc::new(ResponseChannels::new()),
            batch_get: Arc::new(ResponseChannels::new()),
        }
    }
}

/// Container for all initialized services.
pub(crate) struct Services {
    // Operation status services (publish = store phase only, not finality)
    pub publish_store_operation: Arc<OperationStatusService<PublishStoreOperationResult>>,
    pub get_operation: Arc<OperationStatusService<GetOperationResult>>,

    // Storage services
    pub triple_store: Arc<TripleStoreService>,

    // Validation services
    pub get_validation: Arc<GetValidationService>,

    // Infrastructure services
    pub peer_service: Arc<PeerService>,
    pub peer_address_store: Arc<PeerAddressStore>,

    // Response channels for all protocols
    pub response_channels: ResponseChannelsSet,
}

/// Initialize all services.
///
/// Services depend only on Managers, establishing a clear dependency hierarchy:
/// - Managers: lowest level, self-contained infrastructure
/// - Services: business logic layer, depends only on Managers
/// - Controllers/Commands: highest level, depends on both via Context
pub(crate) fn initialize(managers: &Managers) -> Services {
    // Operation status services
    let publish_store_operation = Arc::new(
        OperationStatusService::<PublishStoreOperationResult>::new(
            Arc::clone(&managers.repository),
            &managers.key_value_store,
            protocols::publish_store::NAME,
        )
        .expect("Failed to create publish store operation service"),
    );

    let get_operation = Arc::new(
        OperationStatusService::<GetOperationResult>::new(
            Arc::clone(&managers.repository),
            &managers.key_value_store,
            protocols::get::NAME,
        )
        .expect("Failed to create get operation service"),
    );

    // Storage services
    let triple_store = Arc::new(TripleStoreService::new(Arc::clone(&managers.triple_store)));

    // Validation services
    let get_validation = Arc::new(GetValidationService::new(Arc::clone(&managers.blockchain)));

    // Infrastructure services
    let peer_service = Arc::new(PeerService::new());
    let peer_address_store = Arc::new(
        managers
            .key_value_store
            .peer_address_store()
            .expect("Failed to create peer address store"),
    );

    // Response channels
    let response_channels = ResponseChannelsSet::new();

    Services {
        publish_store_operation,
        get_operation,
        triple_store,
        get_validation,
        peer_service,
        peer_address_store,
        response_channels,
    }
}
