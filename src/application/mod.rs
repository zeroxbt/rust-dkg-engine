pub(crate) mod assertion_validation;

/// Maximum number of UALs accepted in a single batch GET request.
/// Enforced server-side (receiver truncates) and respected client-side (sync caps outgoing batches).
pub(crate) const UAL_MAX_LIMIT: usize = 1000;
pub(crate) mod get_assertion;
pub(crate) mod operation_tracking;
pub(crate) mod triple_store_assertions;

pub(crate) use assertion_validation::{AssertionValidation, group_and_sort_public_triples};
pub(crate) use get_assertion::{
    AssertionSource, GetAssertionInput, GetAssertionUseCase, TokenRangeResolutionPolicy,
    fetch_assertion_from_local,
};
pub(crate) use get_assertion::config::GET_NETWORK_CONCURRENT_PEERS;
pub(crate) use operation_tracking::OperationTracking;
pub(crate) use triple_store_assertions::TripleStoreAssertions;
