use std::{sync::Arc, time::Duration};

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::blockchain::{BlockchainId, BlockchainManager, ContractName},
};

/// Interval between sync cycles (30 seconds)
const SYNC_PERIOD: Duration = Duration::from_secs(30);

/// Maximum number of KCs to process per sync cycle per contract
#[allow(dead_code)]
const SYNC_BATCH_SIZE: usize = 1000;

/// Maximum retry attempts before marking a KC as permanently failed
#[allow(dead_code)]
const MAX_RETRY_ATTEMPTS: u32 = 2;

pub(crate) struct SyncCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
}

impl SyncCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct SyncCommandData {
    pub blockchain_id: BlockchainId,
}

impl SyncCommandData {
    pub(crate) fn new(blockchain_id: BlockchainId) -> Self {
        Self { blockchain_id }
    }
}

impl CommandHandler<SyncCommandData> for SyncCommandHandler {
    async fn execute(&self, data: &SyncCommandData) -> CommandExecutionResult {
        tracing::info!(
            blockchain_id = %data.blockchain_id,
            "[DKG SYNC] Starting sync cycle"
        );

        // Get all KC storage contract addresses for this blockchain
        let contract_addresses = match self
            .blockchain_manager
            .get_all_contract_addresses(
                &data.blockchain_id,
                &ContractName::KnowledgeCollectionStorage,
            )
            .await
        {
            Ok(addresses) => addresses,
            Err(e) => {
                tracing::error!(
                    blockchain_id = %data.blockchain_id,
                    error = %e,
                    "[DKG SYNC] Failed to get KC storage contract addresses"
                );
                return CommandExecutionResult::Repeat { delay: SYNC_PERIOD };
            }
        };

        tracing::debug!(
            blockchain_id = %data.blockchain_id,
            contract_count = contract_addresses.len(),
            "[DKG SYNC] Found KC storage contracts"
        );

        // TODO 2: For each contract (using join_all for parallelism):
        //   a. Get the latest KC ID from chain
        //   b. Compare with our last synced KC ID (from DB)
        //   c. Insert any new KC IDs as "missed" in DB

        // TODO 3: Fetch a batch of missed KCs from DB for this blockchain
        //         (limit SYNC_BATCH_SIZE, retry_count < MAX_RETRY_ATTEMPTS)

        // TODO 4: For each missed KC, check if we already have it locally in triple store

        // TODO 5: For KCs not found locally, batch GET from network

        // TODO 6: Store fetched KCs in triple store

        // TODO 7: Update DB: mark successful KCs as synced, increment retry_count for failed ones

        // TODO 8: Update last synced KC ID in DB for each contract

        tracing::info!(
            blockchain_id = %data.blockchain_id,
            "[DKG SYNC] Sync cycle completed"
        );

        CommandExecutionResult::Repeat { delay: SYNC_PERIOD }
    }
}
