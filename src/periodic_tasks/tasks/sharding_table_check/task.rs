use std::{sync::Arc, time::Duration};

use dkg_blockchain::{BlockchainId, BlockchainManager, NodeInfo};
use dkg_network::PeerId;
use tokio_util::sync::CancellationToken;

use crate::{
    error::NodeError, node_state::PeerRegistry, periodic_tasks::ShardingTableCheckDeps,
    periodic_tasks::runner::run_with_shutdown,
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
    peer_registry: &PeerRegistry,
    local_peer_id: &PeerId,
) {
    let blockchain_ids = blockchain_manager.get_blockchain_ids();

    for blockchain in blockchain_ids {
        seed_blockchain_with_retry(blockchain_manager, peer_registry, blockchain, local_peer_id)
            .await;
    }
}

async fn seed_blockchain_with_retry(
    blockchain_manager: &BlockchainManager,
    peer_registry: &PeerRegistry,
    blockchain: &BlockchainId,
    local_peer_id: &PeerId,
) {
    let mut backoff = SEED_INITIAL_BACKOFF;

    for attempt in 1..=SEED_MAX_RETRIES {
        match pull_sharding_table(blockchain_manager, peer_registry, blockchain).await {
            Ok(count) => {
                tracing::info!(
                    blockchain = %blockchain,
                    peer_count = count,
                    "Seeded sharding table"
                );

                validate_local_peer_id_matches_sharding_table(
                    blockchain_manager,
                    blockchain,
                    local_peer_id,
                )
                .await;

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

async fn validate_local_peer_id_matches_sharding_table(
    blockchain_manager: &BlockchainManager,
    blockchain: &BlockchainId,
    local_peer_id: &PeerId,
) {
    let identity_id = blockchain_manager.identity_id(blockchain);

    let exists = match blockchain_manager
        .sharding_table_node_exists(blockchain, identity_id)
        .await
    {
        Ok(exists) => exists,
        Err(err) => {
            tracing::warn!(
                blockchain = %blockchain,
                identity_id = identity_id,
                error = %err,
                "Failed to check sharding table membership for local identity"
            );
            return;
        }
    };

    if !exists {
        return;
    }

    let node = match blockchain_manager
        .get_sharding_table_node(blockchain, identity_id)
        .await
    {
        Ok(Some(node)) => node,
        Ok(None) => return,
        Err(err) => {
            tracing::warn!(
                blockchain = %blockchain,
                identity_id = identity_id,
                error = %err,
                "Failed to fetch sharding table node for local identity"
            );
            return;
        }
    };

    let peer_id_str = match std::str::from_utf8(&node.nodeId) {
        Ok(s) => s,
        Err(err) => {
            panic!(
                "Fatal: local identity_id={} is present in sharding table for blockchain {}, \
but stored nodeId is not valid UTF-8 (error: {}). This node cannot safely operate as a shard node.",
                identity_id, blockchain, err
            );
        }
    };

    let expected_peer_id: PeerId = match peer_id_str.parse() {
        Ok(peer_id) => peer_id,
        Err(err) => {
            panic!(
                "Fatal: local identity_id={} is present in sharding table for blockchain {}, \
but stored nodeId is not a valid libp2p PeerId (nodeId='{}', parse error: {}). \
This node cannot safely operate as a shard node.",
                identity_id, blockchain, peer_id_str, err
            );
        }
    };

    if &expected_peer_id != local_peer_id {
        panic!(
            "Fatal: local identity_id={} is present in sharding table for blockchain {}, \
but local network PeerId does not match sharding table nodeId (expected_peer_id={}, local_peer_id={}). \
This is usually caused by using the wrong network key / data directory, or a key rotation without \
updating the chain sharding table.",
            identity_id, blockchain, expected_peer_id, local_peer_id
        );
    }
}

/// Pull the full sharding table for a blockchain and update the peer registry.
/// Returns the number of peers loaded.
async fn pull_sharding_table(
    blockchain_manager: &BlockchainManager,
    peer_registry: &PeerRegistry,
    blockchain: &BlockchainId,
) -> Result<usize, NodeError> {
    let sharding_table_length = blockchain_manager
        .get_sharding_table_length(blockchain)
        .await?;

    if sharding_table_length == 0 {
        peer_registry.set_shard_membership(blockchain, &[]);
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

    peer_registry.set_shard_membership(blockchain, &peer_ids);

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
// Periodic task
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) struct ShardingTableCheckTask {
    blockchain_manager: Arc<BlockchainManager>,
    peer_registry: Arc<PeerRegistry>,
}

impl ShardingTableCheckTask {
    pub(crate) fn new(deps: ShardingTableCheckDeps) -> Self {
        Self {
            blockchain_manager: deps.blockchain_manager,
            peer_registry: deps.peer_registry,
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

        let local_count = self.peer_registry.shard_peer_count(blockchain);

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

        pull_sharding_table(&self.blockchain_manager, &self.peer_registry, blockchain).await?;
        Ok(true)
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("sharding_table_check", shutdown, || {
            self.execute(blockchain_id)
        })
        .await;
    }

    #[tracing::instrument(name = "periodic_tasks.sharding_table_check", skip(self))]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        if let Err(error) = self.sync_blockchain_sharding_table(blockchain_id).await {
            tracing::error!(
                blockchain_id = %blockchain_id,
                error = %error,
                "Error syncing sharding table"
            );
        }

        SHARDING_TABLE_CHECK_PERIOD
    }
}
