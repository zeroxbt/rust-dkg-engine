use std::{collections::HashMap, sync::Arc};

use blockchain::{
    AssetStorageChangedFilter, BlockchainManager, BlockchainName, ContractChangedFilter,
    ContractName, EventLog, EventName, KnowledgeCollectionCreatedFilter, NewAssetStorageFilter,
    NewContractFilter, ParameterChangedFilter, utils::to_hex_string,
};
use repository::RepositoryManager;

use crate::context::Context;

const EVENT_FETCH_INTERVAL_MAINNET_MS: u64 = 10_000;
const EVENT_FETCH_INTERVAL_DEV_MS: u64 = 4_000;

/// Maximum number of blocks we can sync historically.
/// If the node has been offline for longer than this (in blocks), we skip missed events.
/// This is roughly 1 hour worth of blocks assuming ~12 second block time.
/// In dev/test environments, this is set to u64::MAX (effectively unlimited).
const MAX_BLOCKS_TO_SYNC_MAINNET: u64 = 300; // ~1 hour at 12s blocks
const MAX_BLOCKS_TO_SYNC_DEV: u64 = u64::MAX; // unlimited for dev

/// Contracts and events to monitor (aligned with JS implementation)
/// MONITORED_CONTRACT_EVENTS = {
///     Hub: ['NewContract', 'ContractChanged', 'NewAssetStorage', 'AssetStorageChanged'],
///     ParametersStorage: ['ParameterChanged'],
///     KnowledgeCollectionStorage: ['KnowledgeCollectionCreated'],
/// }
fn get_monitored_contract_events() -> HashMap<ContractName, Vec<EventName>> {
    let mut map = HashMap::new();
    map.insert(
        ContractName::Hub,
        vec![
            EventName::NewContract,
            EventName::ContractChanged,
            EventName::NewAssetStorage,
            EventName::AssetStorageChanged,
        ],
    );
    map.insert(
        ContractName::ParametersStorage,
        vec![EventName::ParameterChanged],
    );
    map.insert(
        ContractName::KnowledgeCollectionStorage,
        vec![EventName::KnowledgeCollectionCreated],
    );
    map
}

pub struct BlockchainEventController {
    blockchain_manager: Arc<BlockchainManager>,
    repository_manager: Arc<RepositoryManager>,
    /// Polling interval in milliseconds
    poll_interval_ms: u64,
    /// Maximum number of blocks to sync historically (beyond this, events are skipped)
    max_blocks_to_sync: u64,
}

impl BlockchainEventController {
    pub fn new(context: Arc<Context>) -> Self {
        let is_dev_env = context.config().is_dev_env;

        let poll_interval_ms = if is_dev_env {
            EVENT_FETCH_INTERVAL_DEV_MS
        } else {
            EVENT_FETCH_INTERVAL_MAINNET_MS
        };

        let max_blocks_to_sync = if is_dev_env {
            MAX_BLOCKS_TO_SYNC_DEV
        } else {
            MAX_BLOCKS_TO_SYNC_MAINNET
        };

        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            repository_manager: Arc::clone(context.repository_manager()),
            poll_interval_ms,
            max_blocks_to_sync,
        }
    }

    /// Main event loop - polls for blockchain events at regular intervals
    pub async fn listen_and_handle_events(&self) {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_millis(self.poll_interval_ms));

        let max_blocks_display = if self.max_blocks_to_sync == u64::MAX {
            "unlimited".to_string()
        } else {
            self.max_blocks_to_sync.to_string()
        };

        tracing::info!(
            "Starting blockchain event listener (poll_interval: {}ms, max_blocks_to_sync: {})",
            self.poll_interval_ms,
            max_blocks_display
        );

        loop {
            interval.tick().await;

            // Process each blockchain sequentially (as in JS EventListenerCommand)
            for blockchain in self.blockchain_manager.get_blockchain_names() {
                if let Err(e) = self.fetch_and_handle_blockchain_events(blockchain).await {
                    tracing::error!(
                        "Error processing blockchain events for {}: {:?}",
                        blockchain,
                        e
                    );
                }
            }
        }
    }

    /// Fetch and handle events for a single blockchain
    /// Corresponds to BlockchainEventListenerCommand.fetchAndHandleBlockchainEvents in JS
    async fn fetch_and_handle_blockchain_events(
        &self,
        blockchain: &BlockchainName,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get current block (JS uses: currentBlock - 2 for finality safety)
        let current_block = self
            .blockchain_manager
            .get_block_number(blockchain)
            .await?
            .saturating_sub(2);

        // Fetch events from all monitored contracts
        let mut all_events = Vec::new();
        let monitored = get_monitored_contract_events();

        for (contract_name, events_to_filter) in &monitored {
            let last_checked_block = self
                .repository_manager
                .blockchain_repository()
                .get_last_checked_block(blockchain.as_str(), contract_name.as_str())
                .await?;

            let from_block = last_checked_block + 1;

            // Skip if we're already up to date
            if from_block > current_block {
                continue;
            }

            // Check for extended downtime - if we missed too many blocks, skip them
            let blocks_behind = current_block.saturating_sub(last_checked_block);
            if blocks_behind > self.max_blocks_to_sync {
                tracing::warn!(
                    "Extended downtime detected for {} on {}: {} blocks behind (max: {}). Skipping missed events.",
                    contract_name.as_str(),
                    blockchain,
                    blocks_behind,
                    self.max_blocks_to_sync
                );

                // Update last checked block to current and skip fetching
                self.repository_manager
                    .blockchain_repository()
                    .update_last_checked_block(
                        blockchain.as_str(),
                        contract_name.as_str(),
                        current_block,
                        chrono::Utc::now(),
                    )
                    .await?;
                continue;
            }

            // Fetch events for this contract
            let events = self
                .blockchain_manager
                .get_event_logs(
                    blockchain,
                    contract_name,
                    events_to_filter,
                    from_block,
                    current_block,
                )
                .await?;

            all_events.extend(events);

            // Update last checked block for this contract
            self.repository_manager
                .blockchain_repository()
                .update_last_checked_block(
                    blockchain.as_str(),
                    contract_name.as_str(),
                    current_block,
                    chrono::Utc::now(),
                )
                .await?;
        }

        if all_events.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            "Fetched {} events for blockchain {}",
            all_events.len(),
            blockchain
        );

        // Sort events by (blockNumber, transactionIndex, logIndex) as in JS
        all_events.sort_by(|a, b| {
            let a_log = a.log();
            let b_log = b.log();

            let a_block = a_log.block_number.unwrap_or_default().as_u64();
            let b_block = b_log.block_number.unwrap_or_default().as_u64();
            if a_block != b_block {
                return a_block.cmp(&b_block);
            }

            let a_tx_index = a_log.transaction_index.unwrap_or_default().as_u64();
            let b_tx_index = b_log.transaction_index.unwrap_or_default().as_u64();
            if a_tx_index != b_tx_index {
                return a_tx_index.cmp(&b_tx_index);
            }

            let a_log_index = a_log.log_index.unwrap_or_default().as_u64();
            let b_log_index = b_log.log_index.unwrap_or_default().as_u64();
            a_log_index.cmp(&b_log_index)
        });

        // Process all events sequentially (all dependent in current JS implementation)
        for event in all_events {
            self.process_event(blockchain, event).await;
        }

        Ok(())
    }

    /// Process a single event - dispatch to appropriate handler
    async fn process_event(&self, blockchain: &BlockchainName, event: EventLog) {
        let block_number = event.log().block_number.unwrap_or_default().as_u64();
        tracing::trace!(
            "Processing event {:?} in block {}",
            event.event_name(),
            block_number
        );

        match event.event_name() {
            EventName::NewContract => {
                self.handle_new_contract_event(blockchain, event).await;
            }
            EventName::ContractChanged => {
                self.handle_contract_changed_event(blockchain, event).await;
            }
            EventName::NewAssetStorage => {
                self.handle_new_asset_storage_event(blockchain, event).await;
            }
            EventName::AssetStorageChanged => {
                self.handle_asset_storage_changed_event(blockchain, event)
                    .await;
            }
            EventName::ParameterChanged => {
                self.handle_parameter_changed_event(blockchain, event).await;
            }
            EventName::KnowledgeCollectionCreated => {
                self.handle_knowledge_collection_created_event(blockchain, event)
                    .await;
            }
        }
    }

    // === Event Handlers (aligned with JS) ===

    async fn handle_parameter_changed_event(&self, blockchain: &BlockchainName, event: EventLog) {
        let filter = blockchain::utils::decode_event_log::<ParameterChangedFilter>(event);
        tracing::debug!(
            "ParameterChanged on {}: {} = {}",
            blockchain,
            filter.parameter_name,
            filter.parameter_value
        );
        // TODO: Update contract call cache with new parameter values
        // In JS: blockchainModuleManager.setContractCallCache(blockchain,
        // CONTRACTS.PARAMETERS_STORAGE, parameterName, parameterValue)
    }

    async fn handle_new_contract_event(&self, blockchain: &BlockchainName, event: EventLog) {
        let filter = blockchain::utils::decode_event_log::<NewContractFilter>(event);
        tracing::info!(
            "NewContract on {}: {} at {:?}",
            blockchain,
            filter.contract_name,
            filter.new_contract_address
        );

        if let Err(e) = self
            .blockchain_manager
            .re_initialize_contract(
                blockchain,
                filter.contract_name.clone(),
                filter.new_contract_address,
            )
            .await
        {
            tracing::error!(
                "Failed to re-initialize contract {} on {}: {:?}",
                filter.contract_name,
                blockchain,
                e
            );
        }
    }

    async fn handle_contract_changed_event(&self, blockchain: &BlockchainName, event: EventLog) {
        let filter = blockchain::utils::decode_event_log::<ContractChangedFilter>(event);
        tracing::info!(
            "ContractChanged on {}: {} at {:?}",
            blockchain,
            filter.contract_name,
            filter.new_contract_address
        );

        if let Err(e) = self
            .blockchain_manager
            .re_initialize_contract(
                blockchain,
                filter.contract_name.clone(),
                filter.new_contract_address,
            )
            .await
        {
            tracing::error!(
                "Failed to re-initialize contract {} on {}: {:?}",
                filter.contract_name,
                blockchain,
                e
            );
        }
    }

    async fn handle_new_asset_storage_event(&self, blockchain: &BlockchainName, event: EventLog) {
        let filter = blockchain::utils::decode_event_log::<NewAssetStorageFilter>(event);
        tracing::info!(
            "NewAssetStorage on {}: {} at {:?}",
            blockchain,
            filter.contract_name,
            filter.new_contract_address
        );

        if let Err(e) = self
            .blockchain_manager
            .re_initialize_contract(
                blockchain,
                filter.contract_name.clone(),
                filter.new_contract_address,
            )
            .await
        {
            tracing::error!(
                "Failed to re-initialize asset storage {} on {}: {:?}",
                filter.contract_name,
                blockchain,
                e
            );
        }
    }

    async fn handle_asset_storage_changed_event(
        &self,
        blockchain: &BlockchainName,
        event: EventLog,
    ) {
        let filter = blockchain::utils::decode_event_log::<AssetStorageChangedFilter>(event);
        tracing::info!(
            "AssetStorageChanged on {}: {} at {:?}",
            blockchain,
            filter.contract_name,
            filter.new_contract_address
        );

        if let Err(e) = self
            .blockchain_manager
            .re_initialize_contract(
                blockchain,
                filter.contract_name.clone(),
                filter.new_contract_address,
            )
            .await
        {
            tracing::error!(
                "Failed to re-initialize asset storage {} on {}: {:?}",
                filter.contract_name,
                blockchain,
                e
            );
        }
    }

    async fn handle_knowledge_collection_created_event(
        &self,
        blockchain: &BlockchainName,
        event: EventLog,
    ) {
        let filter = blockchain::utils::decode_event_log::<KnowledgeCollectionCreatedFilter>(event);
        tracing::info!(
            "KnowledgeCollectionCreated on {}: id={}, merkleRoot=0x{}, byteSize={}",
            blockchain,
            filter.id,
            to_hex_string(filter.merkle_root.to_vec()),
            filter.byte_size
        );

        // TODO: Queue publishFinalizationCommand
        // In JS: commandExecutor.add({ name: 'publishFinalizationCommand', data: { event }, ... })
    }
}
