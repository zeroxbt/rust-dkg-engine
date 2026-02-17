use std::{future::Future, sync::Arc};

use dkg_blockchain::BlockchainId;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::deps::PeriodicDeps;

pub(crate) trait GlobalPeriodicTask: Send + 'static {
    type Config: Send + 'static;

    fn from_deps(deps: Arc<PeriodicDeps>, config: Self::Config) -> Self;

    fn run_task(self, shutdown: CancellationToken) -> impl Future<Output = ()> + Send;
}

pub(crate) trait BlockchainPeriodicTask: Send + 'static {
    fn from_deps(deps: Arc<PeriodicDeps>) -> Self;

    fn run_task(
        self,
        blockchain_id: &BlockchainId,
        shutdown: CancellationToken,
    ) -> impl Future<Output = ()> + Send;
}

pub(crate) fn spawn_global_task<T: GlobalPeriodicTask>(
    set: &mut JoinSet<()>,
    deps: &Arc<PeriodicDeps>,
    shutdown: &CancellationToken,
    config: T::Config,
) {
    let deps = Arc::clone(deps);
    let shutdown = shutdown.clone();
    set.spawn(async move {
        T::from_deps(deps, config).run_task(shutdown).await;
    });
}

pub(crate) fn spawn_blockchain_task<T: BlockchainPeriodicTask>(
    set: &mut JoinSet<()>,
    deps: &Arc<PeriodicDeps>,
    shutdown: &CancellationToken,
    blockchain_id: &BlockchainId,
) {
    let deps = Arc::clone(deps);
    let shutdown = shutdown.clone();
    let blockchain_id = blockchain_id.clone();
    set.spawn(async move {
        T::from_deps(deps).run_task(&blockchain_id, shutdown).await;
    });
}
