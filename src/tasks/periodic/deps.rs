use super::tasks::{
    blockchain_admin_events::BlockchainAdminEventsDeps, claim_rewards::ClaimRewardsDeps,
    cleanup::CleanupDeps, dial_peers::DialPeersDeps, kc_reconciliation::KcReconciliationDeps,
    paranet_sync::ParanetSyncDeps, proving::ProvingDeps,
    save_peer_addresses::SavePeerAddressesDeps, sharding_table_check::ShardingTableCheckDeps,
    state_snapshot::StateSnapshotDeps,
};
use crate::tasks::dkg_sync::DkgSyncDeps;

#[derive(Clone)]
pub(crate) struct PeriodicTasksDeps {
    pub(crate) dial_peers: DialPeersDeps,
    pub(crate) cleanup: CleanupDeps,
    pub(crate) save_peer_addresses: SavePeerAddressesDeps,
    pub(crate) sharding_table_check: ShardingTableCheckDeps,
    pub(crate) blockchain_admin_events: BlockchainAdminEventsDeps,
    pub(crate) claim_rewards: ClaimRewardsDeps,
    pub(crate) proving: ProvingDeps,
    pub(crate) dkg_sync: DkgSyncDeps,
    pub(crate) state_snapshot: StateSnapshotDeps,
    pub(crate) paranet_sync: ParanetSyncDeps,
    pub(crate) kc_reconciliation: KcReconciliationDeps,
}
