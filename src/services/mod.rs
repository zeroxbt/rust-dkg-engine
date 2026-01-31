pub(crate) mod file_service;
pub(crate) mod get_validation_service;
pub(crate) mod operation;
pub(crate) mod peer_discovery_tracker;
pub(crate) mod peer_performance_tracker;
pub(crate) mod peer_rate_limiter;
pub(crate) mod pending_storage_service;
pub(crate) mod response_channels;
pub(crate) mod triple_store_service;

use std::sync::Arc;

pub(crate) use get_validation_service::GetValidationService;
pub(crate) use operation::OperationStatusService;
pub(crate) use peer_discovery_tracker::PeerDiscoveryTracker;
pub(crate) use peer_performance_tracker::PeerPerformanceTracker;
pub(crate) use peer_rate_limiter::{PeerRateLimiter, PeerRateLimiterConfig};
pub(crate) use pending_storage_service::PendingStorageService;
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
    pub pending_storage: Arc<PendingStorageService>,
    pub triple_store: Arc<TripleStoreService>,

    // Validation services
    pub get_validation: Arc<GetValidationService>,

    // Infrastructure services
    pub peer_discovery_tracker: Arc<PeerDiscoveryTracker>,
    pub peer_performance_tracker: Arc<PeerPerformanceTracker>,
    pub peer_rate_limiter: Arc<PeerRateLimiter>,

    // Response channels for all protocols
    pub response_channels: ResponseChannelsSet,
}

/// Initialize all services.
///
/// Services depend only on Managers, establishing a clear dependency hierarchy:
/// - Managers: lowest level, self-contained infrastructure
/// - Services: business logic layer, depends only on Managers
/// - Controllers/Commands: highest level, depends on both via Context
pub(crate) fn initialize(
    managers: &Managers,
    rate_limiter_config: PeerRateLimiterConfig,
) -> Services {
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
    let pending_storage = Arc::new(
        PendingStorageService::new(&managers.key_value_store)
            .expect("Failed to create pending storage service"),
    );

    let triple_store = Arc::new(TripleStoreService::new(Arc::clone(&managers.triple_store)));

    // Validation services
    let get_validation = Arc::new(GetValidationService::new(Arc::clone(&managers.blockchain)));

    // Infrastructure services
    let peer_discovery_tracker = Arc::new(PeerDiscoveryTracker::new());
    let peer_performance_tracker = Arc::new(PeerPerformanceTracker::new());
    let peer_rate_limiter = Arc::new(PeerRateLimiter::new(rate_limiter_config));

    // Response channels
    let response_channels = ResponseChannelsSet::new();

    Services {
        publish_store_operation,
        get_operation,
        pending_storage,
        triple_store,
        get_validation,
        peer_discovery_tracker,
        peer_performance_tracker,
        peer_rate_limiter,
        response_channels,
    }
}
