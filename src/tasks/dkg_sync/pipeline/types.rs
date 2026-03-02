//! Shared types for the sync pipeline stages.

use dkg_blockchain::Address;
use dkg_domain::{Assertion, KnowledgeCollectionMetadata, TokenIds};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct QueueKcKey {
    pub contract_addr_str: String,
    pub kc_id: u64,
}

impl QueueKcKey {
    pub(crate) fn new(contract_addr_str: String, kc_id: u64) -> Self {
        Self {
            contract_addr_str,
            kc_id,
        }
    }
}

/// Dispatcher work item flowing into the filter stage.
#[derive(Clone)]
pub(crate) struct QueueKcWorkItem {
    pub key: QueueKcKey,
    pub contract_address: Address,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueWriteAction {
    Remove,
    Retry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProjectionWriteAction {
    MarkPresent,
    MarkPending { last_error: Option<&'static str> },
}

/// Terminal queue action emitted by filter/fetch/insert stages.
#[derive(Clone)]
pub(crate) struct QueueOutcome {
    pub key: QueueKcKey,
    pub queue_action: QueueWriteAction,
    pub projection_action: ProjectionWriteAction,
}

impl QueueOutcome {
    pub(crate) fn remove_synced(key: QueueKcKey) -> Self {
        Self {
            key,
            queue_action: QueueWriteAction::Remove,
            projection_action: ProjectionWriteAction::MarkPresent,
        }
    }

    pub(crate) fn remove_already_synced(key: QueueKcKey) -> Self {
        Self {
            key,
            queue_action: QueueWriteAction::Remove,
            projection_action: ProjectionWriteAction::MarkPresent,
        }
    }

    pub(crate) fn retry_with_pending_error(key: QueueKcKey, reason: &'static str) -> Self {
        Self {
            key,
            queue_action: QueueWriteAction::Retry,
            projection_action: ProjectionWriteAction::MarkPending {
                last_error: Some(reason),
            },
        }
    }
}

/// KC that needs to be fetched from the network (output of filter stage).
#[derive(Clone)]
pub(crate) struct KcToSync {
    pub key: QueueKcKey,
    pub ual: String,
    pub token_ids: TokenIds,
    pub merkle_root: Option<String>,
    pub metadata: KnowledgeCollectionMetadata,
}

/// KC fetched from network (output of fetch stage, input to insert stage).
#[derive(Clone)]
pub(crate) struct FetchedKc {
    pub key: QueueKcKey,
    pub ual: String,
    pub assertion: Assertion,
    pub metadata: Option<KnowledgeCollectionMetadata>,
    pub estimated_assets: u64,
}
