//! Shared types for the sync pipeline stages.

use dkg_blockchain::Address;
use dkg_domain::{Assertion, KnowledgeCollectionMetadata, TokenIds};

/// Dispatcher work item flowing into the filter stage.
#[derive(Clone)]
pub(crate) struct QueueKcWorkItem {
    pub contract_address: Address,
    pub contract_addr_str: String,
    pub kc_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueOutcomeKind {
    Remove,
    Retry,
}

/// Terminal queue action emitted by filter/fetch/insert stages.
#[derive(Clone)]
pub(crate) struct QueueOutcome {
    pub contract_addr_str: String,
    pub kc_id: u64,
    pub kind: QueueOutcomeKind,
}

impl QueueOutcome {
    pub(crate) fn remove(contract_addr_str: String, kc_id: u64) -> Self {
        Self {
            contract_addr_str,
            kc_id,
            kind: QueueOutcomeKind::Remove,
        }
    }

    pub(crate) fn retry(contract_addr_str: String, kc_id: u64) -> Self {
        Self {
            contract_addr_str,
            kc_id,
            kind: QueueOutcomeKind::Retry,
        }
    }
}

/// KC that needs to be fetched from the network (output of filter stage).
#[derive(Clone)]
pub(crate) struct KcToSync {
    pub contract_addr_str: String,
    pub kc_id: u64,
    pub ual: String,
    pub token_ids: TokenIds,
    pub merkle_root: Option<String>,
    pub metadata: KnowledgeCollectionMetadata,
}

/// KC fetched from network (output of fetch stage, input to insert stage).
#[derive(Clone)]
pub(crate) struct FetchedKc {
    pub contract_addr_str: String,
    pub kc_id: u64,
    pub ual: String,
    pub assertion: Assertion,
    pub metadata: Option<KnowledgeCollectionMetadata>,
    pub estimated_assets: u64,
}
