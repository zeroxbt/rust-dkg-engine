use std::{sync::Arc, time::Duration};

use futures::future::join_all;
use libp2p::PeerId;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    error::NodeError,
    managers::blockchain::{BlockchainId, BlockchainManager, chains::evm::ShardingTableLib::NodeInfo},
    services::PeerService,
};

/// Interval between sharding table synchronization checks (10 seconds)
const SHARDING_TABLE_CHECK_PERIOD: Duration = Duration::from_secs(10);
const SHARDING_TABLE_PAGE_SIZE: u128 = 100;

/// Retry configuration for startup seeding
const SEED_MAX_RETRIES: u32 = 5;
const SEED_INITIAL_BACKOFF: Duration = Duration::from_secs(2);
const SEED_MAX_BACKOFF: Duration = Duration::from_secs(30);

// ─────────────────────────────────────────────────────────────────────────────
// Startup seeding
// ─────────────────────────────────────────────────────────────────────────────

/// Seed the peer registry with sharding tables from all blockchains.
/// Call this at startup before commands begin processing.
///
/// Retries with exponential backoff on failure. Panics if any blockchain
/// fails after all retries - the node cannot operate without shard data.
pub(crate) async fn seed_sharding_tables(
    blockchain_manager: &BlockchainManager,
    peer_service: &PeerService,
) {
    let blockchain_ids = blockchain_manager.get_blockchain_ids();

    for blockchain in blockchain_ids {
        seed_blockchain_with_retry(blockchain_manager, peer_service, blockchain).await;
    }
}

async fn seed_blockchain_with_retry(
    blockchain_manager: &BlockchainManager,
    peer_service: &PeerService,
    blockchain: &BlockchainId,
) {
    let mut backoff = SEED_INITIAL_BACKOFF;

    for attempt in 1..=SEED_MAX_RETRIES {
        match pull_sharding_table(blockchain_manager, peer_service, blockchain).await {
            Ok(count) => {
                tracing::info!(
                    blockchain = %blockchain,
                    peer_count = count,
                    "Seeded sharding table"
                );
                return;
            }
            Err(error) => {
                if attempt == SEED_MAX_RETRIES {
                    panic!(
                        "Failed to seed sharding table for {} after {} attempts: {}. \
                         Node cannot operate without shard data.",
                        blockchain, SEED_MAX_RETRIES, error
                    );
                }

                tracing::warn!(
                    blockchain = %blockchain,
                    attempt = attempt,
                    max_attempts = SEED_MAX_RETRIES,
                    error = %error,
                    backoff_secs = backoff.as_secs(),
                    "Failed to seed sharding table, retrying..."
                );

                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(SEED_MAX_BACKOFF);
            }
        }
    }
}

/// Pull the full sharding table for a blockchain and update the peer registry.
/// Returns the number of peers loaded.
async fn pull_sharding_table(
    blockchain_manager: &BlockchainManager,
    peer_service: &PeerService,
    blockchain: &BlockchainId,
) -> Result<usize, NodeError> {
    let sharding_table_length = blockchain_manager
        .get_sharding_table_length(blockchain)
        .await?;

    if sharding_table_length == 0 {
        peer_service.set_shard_membership(blockchain, &[]);
        return Ok(0);
    }

    let mut starting_identity_id = blockchain_manager
        .get_sharding_table_head(blockchain)
        .await?;
    let mut page_index = 0usize;
    let mut peer_ids: Vec<PeerId> = Vec::new();
    let mut total_nodes_processed: u128 = 0;

    while total_nodes_processed < sharding_table_length {
        let remaining = sharding_table_length - total_nodes_processed;
        let slice_start = if page_index == 0 { 0 } else { 1 };
        let nodes_to_fetch = remaining
            .saturating_add(slice_start as u128)
            .min(SHARDING_TABLE_PAGE_SIZE);

        let nodes = blockchain_manager
            .get_sharding_table_page(blockchain, starting_identity_id, nodes_to_fetch)
            .await?;

        if nodes.is_empty() {
            break;
        }

        let nodes_in_page = nodes.len().saturating_sub(slice_start) as u128;
        collect_peer_ids(blockchain, &nodes, slice_start, &mut peer_ids);
        total_nodes_processed += nodes_in_page;

        let last_node = nodes.last().expect("non-empty nodes");
        if last_node.identityId == 0 || last_node.identityId == starting_identity_id {
            break;
        }

        starting_identity_id = last_node.identityId.to();
        page_index += 1;
    }

    peer_service.set_shard_membership(blockchain, &peer_ids);

    Ok(peer_ids.len())
}

fn collect_peer_ids(
    blockchain: &BlockchainId,
    nodes: &[NodeInfo],
    slice_start: usize,
    peer_ids: &mut Vec<PeerId>,
) {
    for node in nodes.iter().skip(slice_start) {
        if node.nodeId.is_empty() {
            break;
        }

        let peer_id_str = match std::str::from_utf8(&node.nodeId) {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(
                    blockchain = %blockchain,
                    error = %err,
                    "Skipping sharding table node with invalid UTF-8 node id"
                );
                continue;
            }
        };

        let peer_id: PeerId = match peer_id_str.parse() {
            Ok(peer_id) => peer_id,
            Err(err) => {
                tracing::warn!(
                    blockchain = %blockchain,
                    error = %err,
                    peer_id_str = %peer_id_str,
                    "Skipping sharding table node with invalid peer id"
                );
                continue;
            }
        };

        peer_ids.push(peer_id);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Command handler
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) struct ShardingTableCheckCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
    peer_service: Arc<PeerService>,
}

impl ShardingTableCheckCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            peer_service: Arc::clone(context.peer_service()),
        }
    }

    async fn sync_blockchain_sharding_table(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<bool, NodeError> {
        let sharding_table_length = self
            .blockchain_manager
            .get_sharding_table_length(blockchain)
            .await?;

        let local_count = self.peer_service.shard_peer_count(blockchain);

        // Skip if counts match (optimization to avoid fetching unchanged data)
        if sharding_table_length == local_count as u128 {
            return Ok(false);
        }

        tracing::debug!(
            blockchain = %blockchain,
            chain_length = sharding_table_length,
            local_count = local_count,
            "Refreshing local sharding table"
        );

        pull_sharding_table(&self.blockchain_manager, &self.peer_service, blockchain).await?;
        Ok(true)
    }
}

#[derive(Clone, Default)]
pub(crate) struct ShardingTableCheckCommandData;

impl CommandHandler<ShardingTableCheckCommandData> for ShardingTableCheckCommandHandler {
    #[tracing::instrument(
        name = "periodic.sharding_table_check",
        skip(self),
        fields(blockchain_count = tracing::field::Empty)
    )]
    async fn execute(&self, _: &ShardingTableCheckCommandData) -> CommandExecutionResult {
        let blockchain_ids = self.blockchain_manager.get_blockchain_ids();
        tracing::Span::current().record(
            "blockchain_count",
            tracing::field::display(blockchain_ids.len()),
        );

        let futures = blockchain_ids
            .iter()
            .map(|blockchain| self.sync_blockchain_sharding_table(blockchain));

        let results = join_all(futures).await;

        for (blockchain, result) in blockchain_ids.iter().zip(results) {
            if let Err(error) = result {
                tracing::error!(
                    blockchain = %blockchain,
                    error = %error,
                    "Error syncing sharding table"
                );
            }
        }

        CommandExecutionResult::Repeat {
            delay: SHARDING_TABLE_CHECK_PERIOD,
        }
    }
}
