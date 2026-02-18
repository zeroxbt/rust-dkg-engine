mod runner;
mod shutdown;

use std::sync::Arc;

use dkg_blockchain::BlockchainId;

use crate::{commands::scheduler::CommandScheduler, periodic_tasks, runtime_state::PeerDirectory};

pub(crate) struct RuntimeDeps {
    pub(crate) command_scheduler: CommandScheduler,
    pub(crate) network_manager: Arc<dkg_network::NetworkManager>,
    pub(crate) peer_directory: Arc<PeerDirectory>,
    pub(crate) periodic_tasks_deps: Arc<periodic_tasks::PeriodicTasksDeps>,
    pub(crate) blockchain_ids: Vec<BlockchainId>,
}
pub(crate) use runner::run;
