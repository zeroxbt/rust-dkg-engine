pub(crate) mod get;
pub(crate) mod paranet;
pub(crate) mod publish;
pub(crate) mod shared;
pub(crate) mod signatures;

/// Maximum number of UALs accepted in a single batch GET request.
/// Enforced server-side (receiver truncates) and respected client-side (sync caps outgoing batches).
pub(crate) const UAL_MAX_LIMIT: usize = 1000;

pub(crate) use get::{
    AssertionRetrieval, AssertionSource, FetchRequest, GetAssertionInput, GetAssertionUseCase,
    GetOperationWorkflow, NETWORK_CONCURRENT_PEERS, ServeBatchGetInput, ServeBatchGetOutcome,
    ServeBatchGetWorkflow, ServeGetInput, ServeGetOutcome, ServeGetWorkflow,
    TokenRangeResolutionPolicy,
};
pub(crate) use paranet::parse_paranet_ual_with_id;
pub(crate) use publish::{
    ExecutePublishStoreInput, ExecutePublishStoreWorkflow, ProcessPublishFinalityEventInput,
    ProcessPublishFinalityEventWorkflow, ServePublishFinalityInput, ServePublishFinalityOutcome,
    ServePublishFinalityWorkflow, ServePublishStoreInput, ServePublishStoreOutcome,
    ServePublishStoreWorkflow,
};
pub(crate) use shared::{
    AssertionValidation, OperationTracking, TripleStoreAssertions, group_and_sort_public_triples,
};
