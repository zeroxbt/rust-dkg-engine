mod runner;
mod shutdown;

use std::sync::Arc;

use dkg_blockchain::BlockchainId;

use crate::{commands::scheduler::CommandScheduler, node_state::PeerRegistry, tasks::periodic};

pub(crate) struct RuntimeDeps {
    pub(crate) command_scheduler: CommandScheduler,
    pub(crate) network_manager: Arc<dkg_network::NetworkManager>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
    pub(crate) periodic_tasks_deps: Arc<periodic::PeriodicTasksDeps>,
    pub(crate) blockchain_ids: Vec<BlockchainId>,
}
pub(crate) use runner::run;
