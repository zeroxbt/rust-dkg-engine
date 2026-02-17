use crate::context::{
    BlockchainEventListenerDeps, ClaimRewardsDeps, CleanupDeps, DialPeersDeps, ParanetSyncDeps,
    ProvingDeps, SavePeerAddressesDeps, ShardingTableCheckDeps, SyncDeps,
};

#[derive(Clone)]
pub(crate) struct PeriodicDeps {
    pub(crate) dial_peers: DialPeersDeps,
    pub(crate) cleanup: CleanupDeps,
    pub(crate) save_peer_addresses: SavePeerAddressesDeps,
    pub(crate) sharding_table_check: ShardingTableCheckDeps,
    pub(crate) blockchain_event_listener: BlockchainEventListenerDeps,
    pub(crate) claim_rewards: ClaimRewardsDeps,
    pub(crate) proving: ProvingDeps,
    pub(crate) sync: SyncDeps,
    pub(crate) paranet_sync: ParanetSyncDeps,
}
