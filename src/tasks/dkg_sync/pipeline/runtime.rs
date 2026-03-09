//! Long-lived data pipeline for sync discovery.

use dkg_blockchain::BlockchainId;
use tokio::{sync::mpsc, task::JoinHandle};

use super::{
    stages::{fetch::run_fetch_stage, filter::run_filter_stage, insert::run_insert_stage},
    types::{FetchedKc, KcToSync, QueueKcWorkItem, QueueOutcome},
};
use crate::tasks::dkg_sync::{DkgSyncConfig, DkgSyncDeps};

pub(crate) struct DkgSyncPipeline {
    config: DkgSyncConfig,
    deps: DkgSyncDeps,
}

pub(crate) struct DkgSyncPipelineRuntime {
    pub(crate) input_tx: mpsc::Sender<Vec<QueueKcWorkItem>>,
    filter_handle: JoinHandle<()>,
    fetch_handle: JoinHandle<()>,
    insert_handle: JoinHandle<()>,
}

impl DkgSyncPipelineRuntime {
    pub(crate) async fn shutdown(self) {
        drop(self.input_tx);

        for (name, handle) in [
            ("filter", self.filter_handle),
            ("fetch", self.fetch_handle),
            ("insert", self.insert_handle),
        ] {
            if let Err(error) = handle.await {
                tracing::error!(stage = name, error = ?error, "Sync pipeline stage panicked");
            }
        }
    }
}

impl DkgSyncPipeline {
    pub(crate) fn new(deps: DkgSyncDeps, config: DkgSyncConfig) -> Self {
        Self { config, deps }
    }

    pub(crate) fn start(
        &self,
        blockchain_id: &BlockchainId,
        queue_outcome_tx: mpsc::Sender<Vec<QueueOutcome>>,
    ) -> DkgSyncPipelineRuntime {
        let queue_processor = &self.config.queue_processor;
        let stage_channel_message_buffer = queue_processor.stage_channel_message_buffer.max(1);
        let filter_max_kc_per_chunk = queue_processor.filter_max_kc_per_chunk.max(1);
        let fetch_max_kc_per_batch = queue_processor.fetch_max_kc_per_batch.max(1);
        let fetch_batch_concurrency = queue_processor.fetch_batch_concurrency.max(1);
        let fetch_peer_fanout_concurrency = queue_processor.fetch_peer_fanout_concurrency.max(1);
        // Allow `Some(0)` as config shorthand for "no cap" (same as None).
        let max_peer_attempts_per_batch = queue_processor
            .max_peer_attempts_per_batch
            .filter(|value| *value > 0);
        let insert_kc_concurrency = queue_processor.insert_kc_concurrency.max(1);
        let fetch_max_ka_per_batch = queue_processor.fetch_max_ka_per_batch.max(1);

        let (input_tx, input_rx) =
            mpsc::channel::<Vec<QueueKcWorkItem>>(stage_channel_message_buffer);
        let (filter_tx, filter_rx) = mpsc::channel::<Vec<KcToSync>>(stage_channel_message_buffer);
        let (fetch_tx, fetch_rx) = mpsc::channel::<Vec<FetchedKc>>(stage_channel_message_buffer);

        let filter_handle = {
            let blockchain_id = blockchain_id.clone();
            let kc_chain_metadata_repository = self.deps.kc_chain_metadata_repository.clone();
            let triple_store_assertions = self.deps.triple_store_assertions.clone();
            let outcome_tx = queue_outcome_tx.clone();
            tokio::spawn(async move {
                run_filter_stage(
                    input_rx,
                    filter_max_kc_per_chunk,
                    blockchain_id,
                    kc_chain_metadata_repository,
                    triple_store_assertions,
                    filter_tx,
                    outcome_tx,
                )
                .await;
            })
        };

        let fetch_handle = {
            let blockchain_id = blockchain_id.clone();
            let network_manager = self.deps.network_manager.clone();
            let assertion_validation = self.deps.assertion_validation.clone();
            let peer_registry = self.deps.peer_registry.clone();
            let outcome_tx = queue_outcome_tx.clone();
            tokio::spawn(async move {
                run_fetch_stage(
                    filter_rx,
                    fetch_max_kc_per_batch,
                    fetch_batch_concurrency,
                    fetch_peer_fanout_concurrency,
                    max_peer_attempts_per_batch,
                    fetch_max_ka_per_batch,
                    blockchain_id,
                    network_manager,
                    assertion_validation,
                    peer_registry,
                    fetch_tx,
                    outcome_tx,
                )
                .await;
            })
        };

        let insert_handle = {
            let blockchain_id = blockchain_id.clone();
            let kc_materialization_service = self.deps.kc_materialization_service.clone();
            tokio::spawn(async move {
                run_insert_stage(
                    fetch_rx,
                    blockchain_id,
                    insert_kc_concurrency,
                    kc_materialization_service,
                    queue_outcome_tx,
                )
                .await;
            })
        };

        DkgSyncPipelineRuntime {
            input_tx,
            filter_handle,
            fetch_handle,
            insert_handle,
        }
    }
}
