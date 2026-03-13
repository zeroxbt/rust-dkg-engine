use std::{future::Future, sync::Arc};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::deps::PeriodicTasksDeps;

pub(crate) trait PeriodicTask: Send + 'static {
    type Config: Send + 'static;
    type Context: Send + 'static;

    fn from_deps(deps: Arc<PeriodicTasksDeps>, config: Self::Config) -> Self;

    fn run_task(
        self,
        context: Self::Context,
        shutdown: CancellationToken,
    ) -> impl Future<Output = ()> + Send;
}

pub(crate) fn spawn_task<T: PeriodicTask>(
    set: &mut JoinSet<()>,
    deps: &Arc<PeriodicTasksDeps>,
    shutdown: &CancellationToken,
    config: T::Config,
    context: T::Context,
) {
    let deps = Arc::clone(deps);
    let shutdown = shutdown.clone();
    set.spawn(async move {
        T::from_deps(deps, config).run_task(context, shutdown).await;
    });
}
