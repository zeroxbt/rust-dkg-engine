pub(crate) mod assertion_validation;
pub(crate) mod get_fetch;
pub(crate) mod operation_status;
pub(crate) mod peer;
pub(crate) mod triple_store;

use std::sync::Arc;

pub(crate) use assertion_validation::AssertionValidationService;
use dkg_network::{BatchGetAck, FinalityAck, GetAck, StoreAck};
pub(crate) use get_fetch::{
    GET_NETWORK_CONCURRENT_PEERS, GetFetchRequest, GetFetchService, GetFetchSource,
};
pub(crate) use operation_status::OperationStatusService;
pub(crate) use peer::PeerService;
pub(crate) use triple_store::TripleStoreService;

use crate::{
    managers::Managers,
    operations::{GetOperation, PublishStoreOperation},
    state::ResponseChannels,
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
    pub publish_store_operation: Arc<OperationStatusService<PublishStoreOperation>>,
    pub get_operation: Arc<OperationStatusService<GetOperation>>,

    // Storage services
    pub triple_store: Arc<TripleStoreService>,

    // Validation services
    pub assertion_validation: Arc<AssertionValidationService>,
    pub get_fetch: Arc<GetFetchService>,

    // Infrastructure services
    pub peer_service: Arc<PeerService>,

    // Response channels for all protocols
    pub response_channels: ResponseChannelsSet,
}

/// Initialize all services.
///
/// Services depend only on Managers, establishing a clear dependency hierarchy:
/// - Managers: lowest level, self-contained infrastructure
/// - Services: business logic layer, depends only on Managers
/// - Controllers/Commands: highest level, depends on explicit deps structs
pub(crate) fn initialize(managers: &Managers) -> Services {
    // Operation status services
    let operation_repository = managers.repository.operation_repository();
    let publish_store_operation = Arc::new(OperationStatusService::<PublishStoreOperation>::new(
        operation_repository.clone(),
        &managers.key_value_store,
    ));

    let get_operation = Arc::new(OperationStatusService::<GetOperation>::new(
        operation_repository,
        &managers.key_value_store,
    ));

    // Storage services
    let triple_store = Arc::new(TripleStoreService::new(Arc::clone(&managers.triple_store)));

    // Validation services
    let assertion_validation = Arc::new(AssertionValidationService::new(Arc::clone(
        &managers.blockchain,
    )));

    // Infrastructure services
    let peer_service = Arc::new(PeerService::new());

    let get_fetch = Arc::new(GetFetchService::new(
        Arc::clone(&managers.blockchain),
        Arc::clone(&triple_store),
        Arc::clone(&managers.network),
        Arc::clone(&assertion_validation),
        Arc::clone(&peer_service),
    ));

    // Response channels
    let response_channels = ResponseChannelsSet::new();

    Services {
        publish_store_operation,
        get_operation,
        triple_store,
        assertion_validation,
        get_fetch,
        peer_service,
        response_channels,
    }
}
