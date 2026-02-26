//! Shared types for the sync pipeline stages.
use dkg_domain::{Assertion, KnowledgeCollectionMetadata, TokenIds};

/// KC that needs to be fetched from the network (output of filter stage)
#[derive(Clone)]
pub(crate) struct KcToSync {
    pub kc_id: u64,
    pub ual: String,
    pub token_ids: TokenIds,
    pub merkle_root: Option<String>,
    pub metadata: KnowledgeCollectionMetadata,
}

/// KC fetched from network (output of fetch stage, input to insert stage)
#[derive(Clone)]
pub(crate) struct FetchedKc {
    pub kc_id: u64,
    pub ual: String,
    pub assertion: Assertion,
    pub metadata: Option<KnowledgeCollectionMetadata>,
}

/// Stats collected by filter task
pub(crate) struct FilterStats {
    pub already_synced: Vec<u64>,
    pub expired: Vec<u64>,
    pub waiting_for_metadata: Vec<u64>,
    pub waiting_for_state: Vec<u64>,
}

/// Stats collected by fetch task
pub(crate) struct FetchStats {
    pub failures: Vec<u64>,
}

/// Stats collected by insert task
pub(crate) struct InsertStats {
    pub synced: Vec<u64>,
    pub failed: Vec<u64>,
}

/// Result of syncing a single contract
pub(crate) struct ContractSyncResult {
    pub state_hydrated: u64,
    pub pending: usize,
    pub synced: u64,
    pub failed: u64,
}
