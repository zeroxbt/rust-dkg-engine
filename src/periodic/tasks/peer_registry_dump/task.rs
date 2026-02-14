use std::{sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;

use crate::{context::Context, services::PeerService};

/// If set (and > 0), periodically dumps the entire peer registry to logs.
/// Disabled by default to avoid log spam in production.
const PEER_REGISTRY_DUMP_INTERVAL_SECS_ENV: &str = "DKG_PEER_REGISTRY_DUMP_INTERVAL_SECS";

pub(crate) struct PeerRegistryDumpTask {
    peer_service: Arc<PeerService>,
}

impl PeerRegistryDumpTask {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            peer_service: Arc::clone(context.peer_service()),
        }
    }

    pub(crate) async fn run(self, shutdown: CancellationToken) {
        loop {
            let dump = self.peer_service.dump_peer_registry();
            tracing::info!(
                peer_registry_len = dump.len(),
                peer_registry = ?dump,
                "Peer registry dump"
            );

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                _ = shutdown.cancelled() => {
                    tracing::info!(task = "peer_registry_dump", "Periodic task shutting down");
                    break;
                }
            }
        }
    }
}
