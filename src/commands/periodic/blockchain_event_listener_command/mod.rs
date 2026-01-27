use std::{sync::Arc, time::Duration};

use crate::{
    commands::{
        command_executor::{CommandExecutionRequest, CommandExecutionResult, CommandScheduler},
        command_registry::{Command, CommandHandler},
        operations::publish::protocols::finality::send_finality_request_command::SendFinalityRequestCommandData,
    },
    context::Context,
    error::NodeError,
    managers::{
        blockchain::{
            Address, AssetStorageChangedFilter, BlockchainId, BlockchainManager,
            ContractChangedFilter, ContractLog, ContractName, Hub,
            KnowledgeCollectionCreatedFilter, KnowledgeCollectionStorage, NewAssetStorageFilter,
            NewContractFilter, ParameterChangedFilter, ParametersStorage, error::BlockchainError,
            utils::to_hex_string,
        },
        repository::RepositoryManager,
    },
};

mod blockchain_event_spec;

use blockchain_event_spec::{ContractEvent, decode_contract_event, monitored_contract_events};

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

pub(crate) struct BlockchainEventListenerCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
    repository_manager: Arc<RepositoryManager>,
    command_scheduler: CommandScheduler,
    /// Polling interval
    poll_interval: Duration,
    /// Maximum number of blocks to sync historically (beyond this, events are skipped)
    max_blocks_to_sync: u64,
}

impl BlockchainEventListenerCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        let is_dev_env = context.config().is_dev_env;

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
                        "Extended downtime detected for {} ({}) on {}: {} blocks behind (max: {}). Skipping missed events.",
                        contract_name.as_str(),
                        contract_address_str,
                        blockchain_id,
                        blocks_behind,
                        self.max_blocks_to_sync
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
                "Fetched {} events for blockchain {}",
                all_events.len(),
                blockchain_id
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
            "Processing event from {} in block {}",
            event.contract_name().as_str(),
            block_number
        );

        let log = event.log();
        match decode_contract_event(event.contract_name(), log) {
            Some(ContractEvent::KnowledgeCollectionStorage(decoded)) => {
                if let KnowledgeCollectionStorage::KnowledgeCollectionStorageEvents::KnowledgeCollectionCreated(
                    filter,
                ) = decoded
                {
                    self.handle_knowledge_collection_created_event(blockchain_id, &filter, log)
                        .await;
                }
            }
            Some(ContractEvent::ParametersStorage(decoded)) => {
                let ParametersStorage::ParametersStorageEvents::ParameterChanged(
                    filter,
                ) = decoded;
                self.handle_parameter_changed_event(blockchain_id, &filter)
                    .await?;
            }
            Some(ContractEvent::Hub(decoded)) => match decoded {
                Hub::HubEvents::NewContract(filter) => {
                    self.handle_new_contract_event(blockchain_id, &filter).await?;
                }
                Hub::HubEvents::ContractChanged(filter) => {
                    self.handle_contract_changed_event(blockchain_id, &filter)
                        .await?;
                }
                Hub::HubEvents::NewAssetStorage(filter) => {
                    self.handle_new_asset_storage_event(blockchain_id, &filter)
                        .await?;
                }
                Hub::HubEvents::AssetStorageChanged(filter) => {
                    self.handle_asset_storage_changed_event(blockchain_id, &filter)
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

    // === Event Handlers ===

    async fn handle_parameter_changed_event(
        &self,
        blockchain_id: &BlockchainId,
        filter: &ParameterChangedFilter,
    ) -> Result<(), BlockchainError> {
        tracing::debug!(
            "ParameterChanged on {}: {} = {}",
            blockchain_id,
            filter.parameterName,
            filter.parameterValue
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
            "NewContract on {}: {} at {:?}",
            blockchain_id,
            filter.contractName,
            filter.newContractAddress
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
        log: &alloy::rpc::types::Log,
    ) {
        tracing::info!(
            "KnowledgeCollectionCreated on {}: id={}, merkleRoot=0x{}, byteSize={}",
            blockchain_id,
            filter.id,
            to_hex_string(filter.merkleRoot),
            filter.byteSize
        );

        // Extract minimal data from the log - parsing happens in the command handler
        let Some(transaction_hash) = log.transaction_hash else {
            tracing::error!("Missing transaction hash in KnowledgeCollectionCreated log");
            return;
        };

        // byteSize is uint88 in the contract, convert to u128
        let byte_size: u128 = filter.byteSize.to();

        let command = Command::SendFinalityRequest(SendFinalityRequestCommandData::new(
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

#[derive(Clone)]
pub(crate) struct BlockchainEventListenerCommandData {
    pub blockchain_id: BlockchainId,
}

impl BlockchainEventListenerCommandData {
    pub(crate) fn new(blockchain_id: BlockchainId) -> Self {
        Self { blockchain_id }
    }
}

impl CommandHandler<BlockchainEventListenerCommandData> for BlockchainEventListenerCommandHandler {
    async fn execute(&self, data: &BlockchainEventListenerCommandData) -> CommandExecutionResult {
        let blockchain = &data.blockchain_id;

        tracing::trace!(
            blockchain = %blockchain,
            poll_interval_ms = self.poll_interval.as_millis(),
            "Running blockchain event listener"
        );

        if let Err(error) = self.fetch_and_handle_blockchain_events(blockchain).await {
            tracing::error!(
                blockchain = %blockchain,
                error = %error,
                "Error fetching/processing blockchain events"
            );
        }

        CommandExecutionResult::Repeat {
            delay: self.poll_interval,
        }
    }
}
