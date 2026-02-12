use std::{future::Future, sync::Arc};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{context::Context, managers::blockchain::BlockchainId};

pub(crate) trait GlobalPeriodicTask: Send + 'static {
    type Config: Send + 'static;

    fn from_context(context: Arc<Context>, config: Self::Config) -> Self;

    fn run_task(self, shutdown: CancellationToken) -> impl Future<Output = ()> + Send;
}

pub(crate) trait BlockchainPeriodicTask: Send + 'static {
    fn from_context(context: Arc<Context>) -> Self;

    fn run_task(
        self,
        blockchain_id: &BlockchainId,
        shutdown: CancellationToken,
    ) -> impl Future<Output = ()> + Send;
}

pub(crate) fn spawn_global_task<T: GlobalPeriodicTask>(
    set: &mut JoinSet<()>,
    context: &Arc<Context>,
    shutdown: &CancellationToken,
    config: T::Config,
) {
    let ctx = Arc::clone(context);
    let shutdown = shutdown.clone();
    set.spawn(async move {
        T::from_context(ctx, config).run_task(shutdown).await;
    });
}

pub(crate) fn spawn_blockchain_task<T: BlockchainPeriodicTask>(
    set: &mut JoinSet<()>,
    context: &Arc<Context>,
    shutdown: &CancellationToken,
    blockchain_id: &BlockchainId,
) {
    let ctx = Arc::clone(context);
    let shutdown = shutdown.clone();
    let blockchain_id = blockchain_id.clone();
    set.spawn(async move {
        T::from_context(ctx)
            .run_task(&blockchain_id, shutdown)
            .await;
    });
}
