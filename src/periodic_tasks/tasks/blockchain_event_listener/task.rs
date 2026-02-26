//! Blockchain event listener periodic task implementation.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use dkg_blockchain::{
    Address, BlockchainError, BlockchainId, BlockchainManager, ContractEvent, ContractLog,
    ContractName, MulticallBatch, MulticallRequest, decode_contract_event, encoders,
    monitored_contract_events, to_hex_string,
};
use dkg_observability as observability;
use dkg_repository::{BlockchainRepository, KcChainMetadataRepository};
use tokio_util::sync::CancellationToken;

use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::publish::finality::send_publish_finality_request::SendPublishFinalityRequestCommandData,
        registry::Command, scheduler::CommandScheduler,
    },
    config,
    error::NodeError,
    periodic_tasks::BlockchainEventListenerDeps,
    periodic_tasks::runner::run_with_shutdown,
    periodic_tasks::tasks::sync::burned_encoding::encode_burned_ids,
};

/// Event fetch interval for mainnet (10 seconds)
const EVENT_FETCH_INTERVAL_MAINNET: Duration = Duration::from_secs(10);

/// Event fetch interval for dev environments (4 seconds)
const EVENT_FETCH_INTERVAL_DEV: Duration = Duration::from_secs(4);

pub(crate) struct BlockchainEventListenerTask {
    blockchain_manager: Arc<BlockchainManager>,
    blockchain_repository: BlockchainRepository,
    kc_chain_metadata_repository: KcChainMetadataRepository,
    command_scheduler: CommandScheduler,
    /// Polling interval
    poll_interval: Duration,
}

#[derive(Default)]
struct EventListenerStats {
    fetched_events: usize,
    processed_events: usize,
    skipped_ranges: usize,
    contracts_updated: usize,
}

impl BlockchainEventListenerTask {
    pub(crate) fn new(deps: BlockchainEventListenerDeps) -> Self {
        let is_dev_env = config::is_dev_env();

        let poll_interval = if is_dev_env {
            EVENT_FETCH_INTERVAL_DEV
        } else {
            EVENT_FETCH_INTERVAL_MAINNET
        };

        Self {
            blockchain_manager: deps.blockchain_manager,
            blockchain_repository: deps.blockchain_repository,
            kc_chain_metadata_repository: deps.kc_chain_metadata_repository,
            command_scheduler: deps.command_scheduler,
            poll_interval,
        }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        self.bootstrap_listener_cursors(blockchain_id).await;
        run_with_shutdown("blockchain_events", shutdown, || {
            self.execute(blockchain_id)
        })
        .await;
    }

    /// Initialize listener cursors at process start.
    async fn bootstrap_listener_cursors(&self, blockchain_id: &BlockchainId) {
        let current_block = match self
            .blockchain_manager
            .get_block_number(blockchain_id)
            .await
        {
            Ok(block) => block.saturating_sub(2),
            Err(error) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to bootstrap listener cursors: cannot fetch tip block"
                );
                return;
            }
        };

        let monitored = monitored_contract_events();
        for contract_name in monitored.keys() {
            let contract_addresses = match self
                .blockchain_manager
                .get_all_contract_addresses(blockchain_id, contract_name)
                .await
            {
                Ok(addresses) => addresses,
                Err(error) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_name.as_str(),
                        error = %error,
                        "Failed to list contract addresses during listener cursor bootstrap"
                    );
                    continue;
                }
            };

            let addresses_to_check: Vec<String> = if contract_addresses.is_empty() {
                vec![String::new()]
            } else {
                contract_addresses
                    .iter()
                    .map(|addr| format!("{:?}", addr))
                    .collect()
            };

            for contract_address_str in addresses_to_check {
                let last_checked_block = match self
                    .blockchain_repository
                    .get_last_checked_block(
                        blockchain_id.as_str(),
                        contract_name.as_str(),
                        &contract_address_str,
                    )
                    .await
                {
                    Ok(block) => block,
                    Err(error) => {
                        tracing::warn!(
                            blockchain_id = %blockchain_id,
                            contract = %contract_name.as_str(),
                            address = %contract_address_str,
                            error = %error,
                            "Failed to read listener cursor during bootstrap"
                        );
                        continue;
                    }
                };

                let target_cursor = current_block;
                if target_cursor == last_checked_block {
                    continue;
                }

                if let Err(error) = self
                    .blockchain_repository
                    .update_last_checked_block(
                        blockchain_id.as_str(),
                        contract_name.as_str(),
                        &contract_address_str,
                        target_cursor,
                        chrono::Utc::now(),
                    )
                    .await
                {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_name.as_str(),
                        address = %contract_address_str,
                        error = %error,
                        "Failed to update listener cursor during bootstrap"
                    );
                } else {
                    tracing::debug!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_name.as_str(),
                        address = %contract_address_str,
                        from = last_checked_block,
                        to = target_cursor,
                        "Rebased snapshot stream cursor to tip at startup"
                    );
                }
            }
        }
    }

    #[tracing::instrument(
        name = "periodic_tasks.blockchain_events",
        skip(self),
        fields(
            blockchain_id = %blockchain_id,
            poll_interval_ms = tracing::field::Empty,
        )
    )]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        let started = Instant::now();
        tracing::Span::current().record(
            "poll_interval_ms",
            tracing::field::display(self.poll_interval.as_millis()),
        );

        tracing::trace!(
            poll_interval_ms = self.poll_interval.as_millis(),
            "Running blockchain event listener"
        );

        match self.fetch_and_handle_blockchain_events(blockchain_id).await {
            Ok(stats) => observability::record_blockchain_event_listener_cycle(
                blockchain_id.as_str(),
                "ok",
                started.elapsed(),
                stats.fetched_events,
                stats.processed_events,
                stats.skipped_ranges,
                stats.contracts_updated,
            ),
            Err(error) => {
                observability::record_blockchain_event_listener_cycle(
                    blockchain_id.as_str(),
                    "error",
                    started.elapsed(),
                    0,
                    0,
                    0,
                    0,
                );
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Error fetching/processing blockchain events"
                );
            }
        }

        self.poll_interval
    }

    /// Fetch and handle events for a single blockchain
    async fn fetch_and_handle_blockchain_events(
        &self,
        blockchain_id: &BlockchainId,
    ) -> Result<EventListenerStats, NodeError> {
        let mut stats = EventListenerStats::default();

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
                    .blockchain_repository
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

                stats.fetched_events += events.len();
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
                stats.processed_events += 1;
            }
        }

        // Update last checked block only after successful processing.
        stats.contracts_updated = contracts_to_update.len();
        for (contract_name, contract_address_str) in contracts_to_update {
            self.blockchain_repository
                .update_last_checked_block(
                    blockchain_id.as_str(),
                    contract_name.as_str(),
                    &contract_address_str,
                    current_block,
                    chrono::Utc::now(),
                )
                .await?;
        }

        Ok(stats)
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
            Some(ContractEvent::KnowledgeCollectionCreated {
                event,
                contract_address,
                transaction_hash,
                block_number,
                block_timestamp,
            }) => {
                let byte_size: u128 = event.byteSize.to();
                self.handle_knowledge_collection_created_event(
                    blockchain_id,
                    event.publishOperationId.clone(),
                    event.id,
                    event.merkleRoot,
                    byte_size,
                    contract_address,
                    transaction_hash,
                    block_number,
                    block_timestamp,
                )
                .await;
            }
            Some(ContractEvent::ParameterChanged(event)) => {
                self.handle_parameter_changed_event(
                    blockchain_id,
                    &event.parameterName,
                    event.parameterValue,
                )
                .await?;
            }
            Some(ContractEvent::NewContract(event)) => {
                self.handle_contract_address_update_event(
                    blockchain_id,
                    &event.contractName,
                    event.newContractAddress,
                    "New contract deployed",
                )
                .await?;
            }
            Some(ContractEvent::ContractChanged(event)) => {
                self.handle_contract_address_update_event(
                    blockchain_id,
                    &event.contractName,
                    event.newContractAddress,
                    "Contract changed",
                )
                .await?;
            }
            Some(ContractEvent::NewAssetStorage(event)) => {
                self.handle_contract_address_update_event(
                    blockchain_id,
                    &event.contractName,
                    event.newContractAddress,
                    "New asset storage deployed",
                )
                .await?;
            }
            Some(ContractEvent::AssetStorageChanged(event)) => {
                self.handle_contract_address_update_event(
                    blockchain_id,
                    &event.contractName,
                    event.newContractAddress,
                    "Asset storage changed",
                )
                .await?;
            }
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
        parameter_name: &str,
        parameter_value: dkg_blockchain::U256,
    ) -> Result<(), BlockchainError> {
        tracing::debug!(
            blockchain = %blockchain_id,
            parameter = %parameter_name,
            value = %parameter_value,
            "Parameter changed"
        );
        // TODO: Update contract call cache with new parameter values
        Ok(())
    }

    async fn handle_contract_address_update_event(
        &self,
        blockchain_id: &BlockchainId,
        contract_name: &str,
        new_contract_address: Address,
        log_message: &str,
    ) -> Result<(), BlockchainError> {
        tracing::info!(
            blockchain = %blockchain_id,
            contract = %contract_name,
            address = ?new_contract_address,
            "{log_message}"
        );

        // Silently skip contracts not tracked by this node
        let Ok(_) = contract_name.parse::<ContractName>() else {
            return Ok(());
        };
        self.blockchain_manager
            .re_initialize_contract(
                blockchain_id,
                contract_name.to_string(),
                new_contract_address,
            )
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_knowledge_collection_created_event(
        &self,
        blockchain_id: &BlockchainId,
        publish_operation_id: String,
        knowledge_collection_id: dkg_blockchain::U256,
        merkle_root: dkg_blockchain::B256,
        byte_size: u128,
        contract_address: Address,
        transaction_hash: Option<dkg_blockchain::B256>,
        block_number: u64,
        block_timestamp: u64,
    ) {
        let kc_id_u128: u128 = knowledge_collection_id.to();
        let Ok(kc_id_u64) = u64::try_from(kc_id_u128) else {
            tracing::error!(
            blockchain = %blockchain_id,
            kc_id = %knowledge_collection_id,
            "Knowledge collection id exceeds u64::MAX; skipping metadata upsert"
            );
            return;
        };
        let contract_address_str = format!("{:?}", contract_address);

        tracing::info!(
            blockchain = %blockchain_id,
            kc_id = %knowledge_collection_id,
            merkle_root = %to_hex_string(merkle_root),
            byte_size = %byte_size,
            "Knowledge collection created"
        );

        // Extract minimal data from the log - parsing happens in the operation command handler
        let Some(transaction_hash) = transaction_hash else {
            tracing::error!(
                blockchain = %blockchain_id,
                "Missing transaction hash in KnowledgeCollectionCreated log"
            );
            return;
        };
        let transaction_hash_str = format!("{:#x}", transaction_hash);

        let publisher_address = self
            .resolve_kc_publisher(
                blockchain_id,
                contract_address,
                kc_id_u128,
                transaction_hash,
            )
            .await;

        if let Err(error) = self
            .kc_chain_metadata_repository
            .upsert_core_metadata(
                blockchain_id.as_str(),
                &contract_address_str,
                kc_id_u64,
                publisher_address.as_deref(),
                Some(block_number),
                Some(&transaction_hash_str),
                Some(block_timestamp),
                Some(&publish_operation_id),
                Some("event_listener"),
            )
            .await
        {
            tracing::error!(
                blockchain = %blockchain_id,
                contract = %contract_address_str,
                kc_id = kc_id_u64,
                error = %error,
                "Failed to upsert canonical KC chain metadata"
            );
        }

        let mut state_calls = MulticallBatch::with_capacity(3);
        state_calls.add(MulticallRequest::new(
            contract_address,
            encoders::encode_get_end_epoch(kc_id_u128),
        ));
        state_calls.add(MulticallRequest::new(
            contract_address,
            encoders::encode_get_knowledge_assets_range(kc_id_u128),
        ));
        state_calls.add(MulticallRequest::new(
            contract_address,
            encoders::encode_get_merkle_root(kc_id_u128),
        ));

        match self
            .blockchain_manager
            .execute_multicall(blockchain_id, state_calls)
            .await
        {
            Ok(results) => {
                let Some([epoch_result, range_result, merkle_result]) =
                    <&[dkg_blockchain::MulticallResult; 3]>::try_from(results.as_slice()).ok()
                else {
                    tracing::warn!(
                        blockchain = %blockchain_id,
                        contract = %contract_address_str,
                        kc_id = kc_id_u64,
                        result_count = results.len(),
                        "Unexpected multicall result count for KC state hydration"
                    );
                    self.schedule_finality_request(
                        blockchain_id,
                        publish_operation_id,
                        knowledge_collection_id,
                        contract_address,
                        byte_size,
                        merkle_root,
                        transaction_hash,
                        block_number,
                        block_timestamp,
                    )
                    .await;
                    return;
                };

                let Some((start, end, burned)) = range_result.as_knowledge_assets_range() else {
                    tracing::warn!(
                        blockchain = %blockchain_id,
                        contract = %contract_address_str,
                        kc_id = kc_id_u64,
                        "Failed to decode KC token range during live state hydration"
                    );
                    self.schedule_finality_request(
                        blockchain_id,
                        publish_operation_id,
                        knowledge_collection_id,
                        contract_address,
                        byte_size,
                        merkle_root,
                        transaction_hash,
                        block_number,
                        block_timestamp,
                    )
                    .await;
                    return;
                };

                let end_epoch = epoch_result.as_u64().filter(|v| *v != 0);
                let latest_merkle_root = merkle_result
                    .as_bytes32_hex()
                    .unwrap_or_else(|| format!("0x{}", to_hex_string(merkle_root)));
                let burned_encoding = encode_burned_ids(start, end, &burned);

                if let Err(error) = self
                    .kc_chain_metadata_repository
                    .upsert_sync_state(
                        blockchain_id.as_str(),
                        &contract_address_str,
                        kc_id_u64,
                        start,
                        end,
                        burned_encoding.mode as u32,
                        burned_encoding.payload.as_slice(),
                        end_epoch,
                        &latest_merkle_root,
                        block_number,
                        Some("event_listener"),
                    )
                    .await
                {
                    tracing::warn!(
                        blockchain = %blockchain_id,
                        contract = %contract_address_str,
                        kc_id = kc_id_u64,
                        error = %error,
                        "Failed to upsert live KC sync state metadata"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    blockchain = %blockchain_id,
                    contract = %contract_address_str,
                    kc_id = kc_id_u64,
                    error = %error,
                    "Failed to hydrate live KC sync state from chain"
                );
            }
        }

        self.schedule_finality_request(
            blockchain_id,
            publish_operation_id,
            knowledge_collection_id,
            contract_address,
            byte_size,
            merkle_root,
            transaction_hash,
            block_number,
            block_timestamp,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn schedule_finality_request(
        &self,
        blockchain_id: &BlockchainId,
        publish_operation_id: String,
        knowledge_collection_id: dkg_blockchain::U256,
        contract_address: Address,
        byte_size: u128,
        merkle_root: dkg_blockchain::B256,
        transaction_hash: dkg_blockchain::B256,
        block_number: u64,
        block_timestamp: u64,
    ) {
        let command =
            Command::SendPublishFinalityRequest(SendPublishFinalityRequestCommandData::new(
                blockchain_id.to_owned(),
                publish_operation_id,
                knowledge_collection_id,
                contract_address,
                byte_size,
                merkle_root,
                transaction_hash,
                block_number,
                block_timestamp,
            ));

        self.command_scheduler
            .schedule(CommandExecutionRequest::new(command))
            .await;
    }

    async fn resolve_kc_publisher(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        kc_id: u128,
        tx_hash: dkg_blockchain::B256,
    ) -> Option<String> {
        match self
            .blockchain_manager
            .get_knowledge_collection_publisher(blockchain_id, contract_address, kc_id)
            .await
        {
            Ok(Some(address)) => Some(format!("{:?}", address)),
            _ => match self
                .blockchain_manager
                .get_transaction_sender(blockchain_id, tx_hash)
                .await
            {
                Ok(Some(address)) => Some(format!("{:?}", address)),
                _ => None,
            },
        }
    }
}
