pub(crate) mod assertion_validation;
pub(crate) mod state_metadata;

/// Maximum number of UALs accepted in a single batch GET request.
/// Enforced server-side (receiver truncates) and respected client-side (sync caps outgoing batches).
pub(crate) const UAL_MAX_LIMIT: usize = 1000;
pub(crate) mod assertions;
pub(crate) mod get_assertion;
pub(crate) mod kc_chain_metadata_sync;
pub(crate) mod operation_tracking;
pub(crate) mod paranet;
pub(crate) mod signature;

pub(crate) use assertion_validation::{AssertionValidation, group_and_sort_public_triples};
pub(crate) use assertions::TripleStoreAssertions;
pub(crate) use get_assertion::{
    AssertionSource, GetAssertionInput, GetAssertionUseCase, TokenRangeResolutionPolicy,
    fetch_assertion_from_local,
};
pub(crate) use operation_tracking::OperationTracking;
