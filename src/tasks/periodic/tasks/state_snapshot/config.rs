use dkg_blockchain::BlockchainId;

#[derive(Debug, Clone)]
pub(crate) struct StateSnapshotConfig {
    pub interval_secs: u64,
    pub blockchain_ids: Vec<BlockchainId>,
    pub max_retry_attempts: u32,
}
