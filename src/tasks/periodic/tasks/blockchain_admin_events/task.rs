//! Blockchain admin events periodic task implementation.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use dkg_blockchain::{
    Address, AdminContractEvent, BlockchainError, BlockchainId, BlockchainManager, ContractName,
    decode_admin_event, monitored_admin_events,
};
use dkg_domain::canonical_evm_address;
use dkg_observability as observability;
use tokio_util::sync::CancellationToken;

use super::{BlockchainAdminEventsConfig, BlockchainAdminEventsDeps};
use crate::{
    error::NodeError,
    tasks::periodic::{PeriodicTasksDeps, registry::PeriodicTask, runner::run_with_shutdown},
};

const MINIMUM_REQUIRED_SIGNATURES_PARAMETER: &str = "minimumRequiredSignatures";
const UNINITIALIZED_CURSOR: u64 = u64::MAX;

pub(crate) struct BlockchainAdminEventsTask {
    blockchain_manager: Arc<BlockchainManager>,
    /// Polling interval
    poll_interval: Duration,
    /// Number of tip blocks to skip to reduce short-range reorg risk.
    reorg_buffer_blocks: u64,
    listener_cursor: AtomicU64,
}

#[derive(Default)]
struct ContractEventsStats {
    fetched_events: usize,
    processed_events: usize,
    skipped_ranges: usize,
    contracts_updated: usize,
}

impl BlockchainAdminEventsTask {
    pub(crate) fn new(
        deps: BlockchainAdminEventsDeps,
        task_config: BlockchainAdminEventsConfig,
        reorg_buffer_blocks: u64,
    ) -> Self {
        let poll_interval = Duration::from_secs(task_config.poll_interval_secs.max(1));

        Self {
            blockchain_manager: deps.blockchain_manager,
            poll_interval,
            reorg_buffer_blocks: reorg_buffer_blocks.max(1),
            listener_cursor: AtomicU64::new(UNINITIALIZED_CURSOR),
        }
    }

    pub(crate) async fn run(self, blockchain_id: BlockchainId, shutdown: CancellationToken) {
        self.bootstrap_listener_cursors(&blockchain_id).await;
        run_with_shutdown("blockchain_admin_events", shutdown, || {
            self.execute(&blockchain_id)
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
            Ok(block) => block.saturating_sub(self.reorg_buffer_blocks),
            Err(error) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to bootstrap listener cursors: cannot fetch tip block"
                );
                return;
            }
        };

        let monitored = monitored_admin_events();
        let mut stream_count = 0usize;
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
                    .map(canonical_evm_address)
                    .collect()
            };

            stream_count = stream_count.saturating_add(addresses_to_check.len());
        }

        self.listener_cursor.store(current_block, Ordering::Relaxed);

        tracing::info!(
            blockchain_id = %blockchain_id,
            start_block = current_block,
            contract_streams = stream_count,
            "Initialized in-memory admin event listener cursor"
        );
    }

    #[tracing::instrument(
        name = "periodic_tasks.blockchain_admin_events",
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
            "Running blockchain admin events task"
        );

        match self.fetch_and_handle_admin_events(blockchain_id).await {
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

    /// Fetch and handle admin events for a single blockchain.
    async fn fetch_and_handle_admin_events(
        &self,
        blockchain_id: &BlockchainId,
    ) -> Result<ContractEventsStats, NodeError> {
        let mut stats = ContractEventsStats::default();

        // Get current block with reorg safety buffer.
        let current_block = self
            .blockchain_manager
            .get_block_number(blockchain_id)
            .await?
            .saturating_sub(self.reorg_buffer_blocks);
        let last_checked_block = self.read_or_init_cursor(current_block);
        let from_block = last_checked_block.saturating_add(1);

        let mut all_events = Vec::new();
        let mut scanned_streams = 0usize;
        let monitored = monitored_admin_events();

        if from_block > current_block {
            stats.skipped_ranges = 1;
            return Ok(stats);
        }

        for (contract_name, events_to_filter) in &monitored {
            let contract_addresses = self
                .blockchain_manager
                .get_all_contract_addresses(blockchain_id, contract_name)
                .await?;

            let addresses_to_check: Vec<String> = if contract_addresses.is_empty() {
                vec![String::new()]
            } else {
                contract_addresses
                    .iter()
                    .map(canonical_evm_address)
                    .collect()
            };

            for contract_address_str in addresses_to_check {
                scanned_streams = scanned_streams.saturating_add(1);

                let events = if contract_address_str.is_empty() {
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
                    let contract_address: Address = contract_address_str.parse().map_err(|_| {
                        BlockchainError::InvalidAddress {
                            address: contract_address_str.clone(),
                        }
                    })?;
                    self.blockchain_manager
                        .get_event_logs_for_address(
                            blockchain_id,
                            *contract_name,
                            contract_address,
                            events_to_filter,
                            from_block,
                            current_block,
                        )
                        .await?
                };

                stats.fetched_events += events.len();
                all_events.extend(events);
            }
        }

        if !all_events.is_empty() {
            tracing::debug!(
                blockchain = %blockchain_id,
                event_count = all_events.len(),
                "Fetched blockchain events"
            );

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

            for event in all_events {
                let block_number = event.log().block_number.unwrap_or_default();
                tracing::trace!(
                    contract = %event.contract_name().as_str(),
                    block_number,
                    "Processing event"
                );

                match decode_admin_event(event.contract_name(), event.log()) {
                    Some(decoded) => {
                        self.handle_admin_event(blockchain_id, decoded).await?;
                    }
                    None => tracing::warn!(
                        contract = %event.contract_name().as_str(),
                        "Failed to decode admin contract event"
                    ),
                }
                stats.processed_events += 1;
            }
        }

        stats.contracts_updated = scanned_streams;
        self.set_cursor(current_block);

        Ok(stats)
    }

    async fn handle_admin_event(
        &self,
        blockchain_id: &BlockchainId,
        event: AdminContractEvent,
    ) -> Result<(), BlockchainError> {
        match event {
            AdminContractEvent::ParameterChanged(event) => {
                let parameter_name = event.parameterName.as_str();
                let parameter_value = event.parameterValue;
                tracing::debug!(
                    blockchain = %blockchain_id,
                    parameter = %parameter_name,
                    value = %parameter_value,
                    "Parameter changed"
                );

                if parameter_name == MINIMUM_REQUIRED_SIGNATURES_PARAMETER {
                    let minimum_required_signatures = parameter_value.to::<u64>();
                    self.blockchain_manager
                        .set_cached_minimum_required_signatures(
                            blockchain_id,
                            minimum_required_signatures,
                        )
                        .await;
                    tracing::info!(
                        blockchain = %blockchain_id,
                        minimum_required_signatures,
                        "Updated minimumRequiredSignatures cache from ParameterChanged event"
                    );
                }
                Ok(())
            }
            AdminContractEvent::NewContract(event) => {
                self.handle_contract_address_update(
                    blockchain_id,
                    &event.contractName,
                    event.newContractAddress,
                    "New contract deployed",
                )
                .await
            }
            AdminContractEvent::ContractChanged(event) => {
                self.handle_contract_address_update(
                    blockchain_id,
                    &event.contractName,
                    event.newContractAddress,
                    "Contract changed",
                )
                .await
            }
            AdminContractEvent::NewAssetStorage(event) => {
                self.handle_contract_address_update(
                    blockchain_id,
                    &event.contractName,
                    event.newContractAddress,
                    "New asset storage deployed",
                )
                .await
            }
            AdminContractEvent::AssetStorageChanged(event) => {
                self.handle_contract_address_update(
                    blockchain_id,
                    &event.contractName,
                    event.newContractAddress,
                    "Asset storage changed",
                )
                .await
            }
        }
    }

    async fn handle_contract_address_update(
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

        let Ok(contract_name_enum) = contract_name.parse::<ContractName>() else {
            return Ok(());
        };

        if contract_name_enum == ContractName::ParametersStorage {
            self.blockchain_manager
                .invalidate_cached_minimum_required_signatures(blockchain_id)
                .await;
            tracing::info!(
                blockchain = %blockchain_id,
                "Invalidated minimumRequiredSignatures cache due to ParametersStorage contract update"
            );
        }

        self.blockchain_manager
            .re_initialize_contract(
                blockchain_id,
                contract_name.to_string(),
                new_contract_address,
            )
            .await?;
        Ok(())
    }

    fn read_or_init_cursor(&self, fallback: u64) -> u64 {
        let current = self.listener_cursor.load(Ordering::Relaxed);
        if current != UNINITIALIZED_CURSOR {
            return current;
        }

        match self.listener_cursor.compare_exchange(
            UNINITIALIZED_CURSOR,
            fallback,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => fallback,
            Err(existing) if existing == UNINITIALIZED_CURSOR => fallback,
            Err(existing) => existing,
        }
    }

    fn set_cursor(&self, cursor: u64) {
        let mut current = self.listener_cursor.load(Ordering::Relaxed);
        loop {
            let next = if current == UNINITIALIZED_CURSOR {
                cursor
            } else {
                current.max(cursor)
            };

            match self.listener_cursor.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
}

impl PeriodicTask for BlockchainAdminEventsTask {
    type Config = (BlockchainAdminEventsConfig, u64);
    type Context = BlockchainId;

    fn from_deps(deps: Arc<PeriodicTasksDeps>, config: Self::Config) -> Self {
        let (task_config, reorg_buffer_blocks) = config;
        Self::new(
            deps.blockchain_admin_events.clone(),
            task_config,
            reorg_buffer_blocks,
        )
    }

    fn run_task(
        self,
        blockchain_id: Self::Context,
        shutdown: CancellationToken,
    ) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, blockchain_id, shutdown)
    }
}
