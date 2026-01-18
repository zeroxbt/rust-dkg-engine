use std::sync::Arc;

use blockchain::{
    AssetStorageChangedFilter, BlockchainId, BlockchainManager, ContractChangedFilter, ContractLog,
    ContractName, KnowledgeCollectionCreatedFilter, NewAssetStorageFilter, NewContractFilter,
    ParameterChangedFilter, error::BlockchainError, utils::to_hex_string,
};
use repository::RepositoryManager;

use crate::{
    blockchain_event_spec::{ContractEvent, decode_contract_event, monitored_contract_events},
    context::Context,
};

const EVENT_FETCH_INTERVAL_MAINNET_MS: u64 = 10_000;
const EVENT_FETCH_INTERVAL_DEV_MS: u64 = 4_000;

/// Maximum number of blocks we can sync historically.
/// If the node has been offline for longer than this (in blocks), we skip missed events.
/// This is roughly 1 hour worth of blocks assuming ~12 second block time.
/// In dev/test environments, this is set to u64::MAX (effectively unlimited).
const MAX_BLOCKS_TO_SYNC_MAINNET: u64 = 300; // ~1 hour at 12s blocks
const MAX_BLOCKS_TO_SYNC_DEV: u64 = u64::MAX; // unlimited for dev


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
            for blockchain in self.blockchain_manager.get_blockchain_ids() {
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
        blockchain: &BlockchainId,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get current block (JS uses: currentBlock - 2 for finality safety)
        let current_block = self
            .blockchain_manager
            .get_block_number(blockchain)
            .await?
            .saturating_sub(2);

        // Fetch events from all monitored contracts
        let mut all_events = Vec::new();
        let mut contracts_to_update = Vec::new();
        let monitored = monitored_contract_events();

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
            contracts_to_update.push(contract_name.clone());
        }

        if !all_events.is_empty() {
            tracing::debug!(
                "Fetched {} events for blockchain {}",
                all_events.len(),
                blockchain
            );

            // Sort events by (blockNumber, transactionIndex, logIndex) as in JS
            all_events.sort_by(|a, b| {
                let a_log = a.log();
                let b_log = b.log();

                let a_block = a_log.block_number.unwrap_or_default();
                let b_block = b_log.block_number.unwrap_or_default();
                if a_block != b_block {
                    return a_block.cmp(&b_block);
                }

                let a_tx_index = a_log.transaction_index.unwrap_or_default();
                let b_tx_index = b_log.transaction_index.unwrap_or_default();
                if a_tx_index != b_tx_index {
                    return a_tx_index.cmp(&b_tx_index);
                }

                let a_log_index = a_log.log_index.unwrap_or_default();
                let b_log_index = b_log.log_index.unwrap_or_default();
                a_log_index.cmp(&b_log_index)
            });

            // Process all events sequentially (all dependent in current JS implementation)
            for event in all_events {
                self.process_event(blockchain, event).await?;
            }
        }

        // Update last checked block only after successful processing.
        for contract_name in contracts_to_update {
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

        Ok(())
    }

    /// Process a single event - dispatch to appropriate handler
    async fn process_event(
        &self,
        blockchain: &BlockchainId,
        event: ContractLog,
    ) -> Result<(), BlockchainError> {
        let block_number = event.log().block_number.unwrap_or_default();
        tracing::trace!(
            "Processing event from {} in block {}",
            event.contract_name().as_str(),
            block_number
        );

        match decode_contract_event(event.contract_name(), event.log()) {
            Some(ContractEvent::KnowledgeCollectionStorage(decoded)) => {
                if let blockchain::KnowledgeCollectionStorage::KnowledgeCollectionStorageEvents::KnowledgeCollectionCreated(
                    filter,
                ) = decoded
                {
                    self.handle_knowledge_collection_created_event(blockchain, &filter)
                        .await?;
                }
            }
            Some(ContractEvent::ParametersStorage(decoded)) => {
                let blockchain::ParametersStorage::ParametersStorageEvents::ParameterChanged(
                    filter,
                ) = decoded;
                self.handle_parameter_changed_event(blockchain, &filter)
                    .await?;
            }
            Some(ContractEvent::Hub(decoded)) => match decoded {
                blockchain::Hub::HubEvents::NewContract(filter) => {
                    self.handle_new_contract_event(blockchain, &filter).await?;
                }
                blockchain::Hub::HubEvents::ContractChanged(filter) => {
                    self.handle_contract_changed_event(blockchain, &filter)
                        .await?;
                }
                blockchain::Hub::HubEvents::NewAssetStorage(filter) => {
                    self.handle_new_asset_storage_event(blockchain, &filter)
                        .await?;
                }
                blockchain::Hub::HubEvents::AssetStorageChanged(filter) => {
                    self.handle_asset_storage_changed_event(blockchain, &filter)
                        .await?;
                }
                _ => {}
            },
            None => {
                tracing::warn!(
                    "Failed to decode event for contract {}",
                    event.contract_name().as_str()
                );
            }
        }

        Ok(())
    }

    // === Event Handlers (aligned with JS) ===

    async fn handle_parameter_changed_event(
        &self,
        blockchain: &BlockchainId,
        filter: &ParameterChangedFilter,
    ) -> Result<(), BlockchainError> {
        tracing::debug!(
            "ParameterChanged on {}: {} = {}",
            blockchain,
            filter.parameterName,
            filter.parameterValue
        );
        // TODO: Update contract call cache with new parameter values
        // In JS: blockchainModuleManager.setContractCallCache(blockchain,
        // CONTRACTS.PARAMETERS_STORAGE, parameterName, parameterValue)
        Ok(())
    }

    async fn handle_new_contract_event(
        &self,
        blockchain: &BlockchainId,
        filter: &NewContractFilter,
    ) -> Result<(), BlockchainError> {
        tracing::info!(
            "NewContract on {}: {} at {:?}",
            blockchain,
            filter.contractName,
            filter.newContractAddress
        );

        // Silently skip contracts not tracked by this node
        let Ok(_) = filter.contractName.parse::<ContractName>() else {
            return Ok(());
        };
        self.blockchain_manager
            .re_initialize_contract(
                blockchain,
                filter.contractName.clone(),
                filter.newContractAddress,
            )
            .await?;
        Ok(())
    }

    async fn handle_contract_changed_event(
        &self,
        blockchain: &BlockchainId,
        filter: &ContractChangedFilter,
    ) -> Result<(), BlockchainError> {
        // Silently skip contracts not tracked by this node
        let Ok(_) = filter.contractName.parse::<ContractName>() else {
            return Ok(());
        };
        self.blockchain_manager
            .re_initialize_contract(
                blockchain,
                filter.contractName.clone(),
                filter.newContractAddress,
            )
            .await?;
        Ok(())
    }

    async fn handle_new_asset_storage_event(
        &self,
        blockchain: &BlockchainId,
        filter: &NewAssetStorageFilter,
    ) -> Result<(), BlockchainError> {
        // Silently skip contracts not tracked by this node
        let Ok(_) = filter.contractName.parse::<ContractName>() else {
            return Ok(());
        };
        self.blockchain_manager
            .re_initialize_contract(
                blockchain,
                filter.contractName.clone(),
                filter.newContractAddress,
            )
            .await?;
        Ok(())
    }

    async fn handle_asset_storage_changed_event(
        &self,
        blockchain: &BlockchainId,
        filter: &AssetStorageChangedFilter,
    ) -> Result<(), BlockchainError> {
        // Silently skip contracts not tracked by this node
        let Ok(_) = filter.contractName.parse::<ContractName>() else {
            return Ok(());
        };
        self.blockchain_manager
            .re_initialize_contract(
                blockchain,
                filter.contractName.clone(),
                filter.newContractAddress,
            )
            .await?;
        Ok(())
    }

    async fn handle_knowledge_collection_created_event(
        &self,
        blockchain: &BlockchainId,
        filter: &KnowledgeCollectionCreatedFilter,
    ) -> Result<(), BlockchainError> {
        tracing::info!(
            "KnowledgeCollectionCreated on {}: id={}, merkleRoot=0x{}, byteSize={}",
            blockchain,
            filter.id,
            to_hex_string(filter.merkleRoot),
            filter.byteSize
        );

        // TODO: Queue publishFinalizationCommand
        // In JS: commandExecutor.add({ name: 'publishFinalizationCommand', data: { event }, ... })
        Ok(())
    }
}
