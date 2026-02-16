//! Blockchain event listener periodic task implementation.

use std::{sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;

use super::blockchain_event_spec::{
    ContractEvent, decode_contract_event, monitored_contract_events,
};
use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::publish::finality::send_publish_finality_request::SendPublishFinalityRequestCommandData,
        registry::Command, scheduler::CommandScheduler,
    },
    config,
    context::Context,
    error::NodeError,
    managers::{
        blockchain::{
            Address, AssetStorageChangedFilter, BlockchainError, BlockchainId, BlockchainManager,
            ContractChangedFilter, ContractLog, ContractName, HubEvents,
            KnowledgeCollectionCreatedFilter, KnowledgeCollectionStorageEvents,
            NewAssetStorageFilter, NewContractFilter, ParameterChangedFilter,
            ParametersStorageEvents, to_hex_string,
        },
        repository::RepositoryManager,
    },
    periodic::runner::run_with_shutdown,
};

/// Event fetch interval for mainnet (10 seconds)
const EVENT_FETCH_INTERVAL_MAINNET: Duration = Duration::from_secs(10);

/// Event fetch interval for dev environments (4 seconds)
const EVENT_FETCH_INTERVAL_DEV: Duration = Duration::from_secs(4);

/// Maximum number of blocks we can sync historically.
/// If the node has been offline for longer than this (in blocks), we skip missed events.
/// This is roughly 1 hour worth of blocks assuming ~12 second block time.
/// In dev/test environments, this is set to u64::MAX (effectively unlimited).
const MAX_BLOCKS_TO_SYNC_MAINNET: u64 = 300; // ~1 hour at 12s blocks
const MAX_BLOCKS_TO_SYNC_DEV: u64 = u64::MAX; // unlimited for dev

pub(crate) struct BlockchainEventListenerTask {
    blockchain_manager: Arc<BlockchainManager>,
    repository_manager: Arc<RepositoryManager>,
    command_scheduler: CommandScheduler,
    /// Polling interval
    poll_interval: Duration,
    /// Maximum number of blocks to sync historically (beyond this, events are skipped)
    max_blocks_to_sync: u64,
}

impl BlockchainEventListenerTask {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        let is_dev_env = config::is_dev_env();

        let poll_interval = if is_dev_env {
            EVENT_FETCH_INTERVAL_DEV
        } else {
            EVENT_FETCH_INTERVAL_MAINNET
        };

        let max_blocks_to_sync = if is_dev_env {
            MAX_BLOCKS_TO_SYNC_DEV
        } else {
            MAX_BLOCKS_TO_SYNC_MAINNET
        };

        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            repository_manager: Arc::clone(context.repository_manager()),
            command_scheduler: context.command_scheduler().clone(),
            poll_interval,
            max_blocks_to_sync,
        }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("blockchain_events", shutdown, || {
            self.execute(blockchain_id)
        })
        .await;
    }

    #[tracing::instrument(
        name = "periodic.blockchain_events",
        skip(self),
        fields(
            blockchain_id = %blockchain_id,
            poll_interval_ms = tracing::field::Empty,
            max_blocks_to_sync = tracing::field::Empty,
        )
    )]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        tracing::Span::current().record(
            "poll_interval_ms",
            tracing::field::display(self.poll_interval.as_millis()),
        );
        tracing::Span::current().record(
            "max_blocks_to_sync",
            tracing::field::display(self.max_blocks_to_sync),
        );

        tracing::trace!(
            poll_interval_ms = self.poll_interval.as_millis(),
            "Running blockchain event listener"
        );

        if let Err(error) = self.fetch_and_handle_blockchain_events(blockchain_id).await {
            tracing::error!(
                blockchain_id = %blockchain_id,
                error = %error,
                "Error fetching/processing blockchain events"
            );
        }

        self.poll_interval
    }

    /// Fetch and handle events for a single blockchain
    async fn fetch_and_handle_blockchain_events(
        &self,
        blockchain_id: &BlockchainId,
    ) -> Result<(), NodeError> {
        // Get current block (use -2 for finality safety)
        let current_block = self
            .blockchain_manager
            .get_block_number(blockchain_id)
            .await?
            .saturating_sub(2);

        // Fetch events from all monitored contracts
        let mut all_events = Vec::new();
        // Track (contract_name, contract_address) pairs to update
        let mut contracts_to_update: Vec<(ContractName, String)> = Vec::new();
        let monitored = monitored_contract_events();

        for (contract_name, events_to_filter) in &monitored {
            // Get all addresses for this contract type (supports multiple for
            // KnowledgeCollectionStorage)
            let contract_addresses = self
                .blockchain_manager
                .get_all_contract_addresses(blockchain_id, contract_name)
                .await?;

            // If no addresses, use empty string for single-address contracts
            let addresses_to_check: Vec<String> = if contract_addresses.is_empty() {
                vec![String::new()]
            } else {
                contract_addresses
                    .iter()
                    .map(|addr| format!("{:?}", addr))
                    .collect()
            };

            for contract_address_str in addresses_to_check {
                let last_checked_block = self
                    .repository_manager
                    .blockchain_repository()
                    .get_last_checked_block(
                        blockchain_id.as_str(),
                        contract_name.as_str(),
                        &contract_address_str,
                    )
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
                        blockchain = %blockchain_id,
                        contract = %contract_name.as_str(),
                        address = %contract_address_str,
                        blocks_behind,
                        max_blocks = self.max_blocks_to_sync,
                        "Extended downtime detected; skipping missed events"
                    );

                    self.repository_manager
                        .blockchain_repository()
                        .update_last_checked_block(
                            blockchain_id.as_str(),
                            contract_name.as_str(),
                            &contract_address_str,
                            current_block,
                            chrono::Utc::now(),
                        )
                        .await?;
                    continue;
                }

                // Fetch events for this contract address
                let events = if contract_address_str.is_empty() {
                    // Single-address contract - use original method
                    self.blockchain_manager
                        .get_event_logs(
                            blockchain_id,
                            contract_name,
                            events_to_filter,
                            from_block,
                            current_block,
                        )
                        .await?
                } else {
                    // Multi-address contract - fetch for specific address
                    let contract_address: Address = contract_address_str.parse().map_err(|_| {
                        BlockchainError::Custom(format!(
                            "Invalid contract address: {}",
                            contract_address_str
                        ))
                    })?;
                    self.blockchain_manager
                        .get_event_logs_for_address(
                            blockchain_id,
                            contract_name.clone(),
                            contract_address,
                            events_to_filter,
                            from_block,
                            current_block,
                        )
                        .await?
                };

                all_events.extend(events);
                contracts_to_update.push((contract_name.clone(), contract_address_str));
            }
        }

        if !all_events.is_empty() {
            tracing::debug!(
                blockchain = %blockchain_id,
                event_count = all_events.len(),
                "Fetched blockchain events"
            );

            // Sort events by (blockNumber, transactionIndex, logIndex)
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

            // Process all events sequentially
            for event in all_events {
                self.process_event(blockchain_id, event).await?;
            }
        }

        // Update last checked block only after successful processing.
        for (contract_name, contract_address_str) in contracts_to_update {
            self.repository_manager
                .blockchain_repository()
                .update_last_checked_block(
                    blockchain_id.as_str(),
                    contract_name.as_str(),
                    &contract_address_str,
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
        blockchain_id: &BlockchainId,
        event: ContractLog,
    ) -> Result<(), BlockchainError> {
        let block_number = event.log().block_number.unwrap_or_default();
        tracing::trace!(
            contract = %event.contract_name().as_str(),
            block_number,
            "Processing event"
        );

        let log = event.log();
        match decode_contract_event(event.contract_name(), log) {
            Some(ContractEvent::KnowledgeCollectionStorage(decoded)) => {
                if let KnowledgeCollectionStorageEvents::KnowledgeCollectionCreated(filter) =
                    decoded
                {
                    self.handle_knowledge_collection_created_event(blockchain_id, &filter, log)
                        .await;
                }
            }
            Some(ContractEvent::ParametersStorage(decoded)) => {
                let ParametersStorageEvents::ParameterChanged(filter) = decoded;
                self.handle_parameter_changed_event(blockchain_id, &filter)
                    .await?;
            }
            Some(ContractEvent::Hub(decoded)) => match decoded {
                HubEvents::NewContract(filter) => {
                    self.handle_new_contract_event(blockchain_id, &filter)
                        .await?;
                }
                HubEvents::ContractChanged(filter) => {
                    self.handle_contract_changed_event(blockchain_id, &filter)
                        .await?;
                }
                HubEvents::NewAssetStorage(filter) => {
                    self.handle_new_asset_storage_event(blockchain_id, &filter)
                        .await?;
                }
                HubEvents::AssetStorageChanged(filter) => {
                    self.handle_asset_storage_changed_event(blockchain_id, &filter)
                        .await?;
                }
                _ => {}
            },
            None => {
                tracing::warn!(
                    contract = %event.contract_name().as_str(),
                    "Failed to decode contract event"
                );
            }
        }

        Ok(())
    }

    // === Event Handlers ===

    async fn handle_parameter_changed_event(
        &self,
        blockchain_id: &BlockchainId,
        filter: &ParameterChangedFilter,
    ) -> Result<(), BlockchainError> {
        tracing::debug!(
            blockchain = %blockchain_id,
            parameter = %filter.parameterName,
            value = %filter.parameterValue,
            "Parameter changed"
        );
        // TODO: Update contract call cache with new parameter values
        Ok(())
    }

    async fn handle_new_contract_event(
        &self,
        blockchain_id: &BlockchainId,
        filter: &NewContractFilter,
    ) -> Result<(), BlockchainError> {
        tracing::info!(
            blockchain = %blockchain_id,
            contract = %filter.contractName,
            address = ?filter.newContractAddress,
            "New contract deployed"
        );

        // Silently skip contracts not tracked by this node
        let Ok(_) = filter.contractName.parse::<ContractName>() else {
            return Ok(());
        };
        self.blockchain_manager
            .re_initialize_contract(
                blockchain_id,
                filter.contractName.clone(),
                filter.newContractAddress,
            )
            .await?;
        Ok(())
    }

    async fn handle_contract_changed_event(
        &self,
        blockchain_id: &BlockchainId,
        filter: &ContractChangedFilter,
    ) -> Result<(), BlockchainError> {
        // Silently skip contracts not tracked by this node
        let Ok(_) = filter.contractName.parse::<ContractName>() else {
            return Ok(());
        };
        self.blockchain_manager
            .re_initialize_contract(
                blockchain_id,
                filter.contractName.clone(),
                filter.newContractAddress,
            )
            .await?;
        Ok(())
    }

    async fn handle_new_asset_storage_event(
        &self,
        blockchain_id: &BlockchainId,
        filter: &NewAssetStorageFilter,
    ) -> Result<(), BlockchainError> {
        // Silently skip contracts not tracked by this node
        let Ok(_) = filter.contractName.parse::<ContractName>() else {
            return Ok(());
        };
        self.blockchain_manager
            .re_initialize_contract(
                blockchain_id,
                filter.contractName.clone(),
                filter.newContractAddress,
            )
            .await?;
        Ok(())
    }

    async fn handle_asset_storage_changed_event(
        &self,
        blockchain_id: &BlockchainId,
        filter: &AssetStorageChangedFilter,
    ) -> Result<(), BlockchainError> {
        // Silently skip contracts not tracked by this node
        let Ok(_) = filter.contractName.parse::<ContractName>() else {
            return Ok(());
        };
        self.blockchain_manager
            .re_initialize_contract(
                blockchain_id,
                filter.contractName.clone(),
                filter.newContractAddress,
            )
            .await?;
        Ok(())
    }

    async fn handle_knowledge_collection_created_event(
        &self,
        blockchain_id: &BlockchainId,
        filter: &KnowledgeCollectionCreatedFilter,
        log: &crate::managers::blockchain::Log,
    ) {
        tracing::info!(
            blockchain = %blockchain_id,
            kc_id = %filter.id,
            merkle_root = %to_hex_string(filter.merkleRoot),
            byte_size = %filter.byteSize,
            "Knowledge collection created"
        );

        // Extract minimal data from the log - parsing happens in the operation command handler
        let Some(transaction_hash) = log.transaction_hash else {
            tracing::error!(
                blockchain = %blockchain_id,
                "Missing transaction hash in KnowledgeCollectionCreated log"
            );
            return;
        };

        // byteSize is uint88 in the contract, convert to u128
        let byte_size: u128 = filter.byteSize.to();

        let command =
            Command::SendPublishFinalityRequest(SendPublishFinalityRequestCommandData::new(
                blockchain_id.to_owned(),
                filter.publishOperationId.clone(),
                filter.id,
                log.address(),
                byte_size,
                filter.merkleRoot,
                transaction_hash,
                log.block_number.unwrap_or_default(),
                log.block_timestamp.unwrap_or_default(),
            ));

        self.command_scheduler
            .schedule(CommandExecutionRequest::new(command))
            .await;
    }
}
