use std::sync::Arc;

use blockchain::{
    BlockchainId, BlockchainManager, ShardingTableNode,
    utils::{from_wei, sha256_hex},
};
use futures::future::join_all;
use repository::{RepositoryManager, ShardRecordInput};

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
};

const SHARDING_TABLE_CHECK_COMMAND_PERIOD_MS: i64 = 10_000;
// TODO: small value chosen for testing, this will be updated after testing. Do not update for now.
const SHARDING_TABLE_PAGE_SIZE: u128 = 2;

pub struct ShardingTableCheckCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    blockchain_manager: Arc<BlockchainManager>,
}

impl ShardingTableCheckCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
        }
    }

    async fn sync_blockchain_sharding_table(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sharding_table_length = self
            .blockchain_manager
            .get_sharding_table_length(blockchain)
            .await?;

        let local_count = self
            .repository_manager
            .shard_repository()
            .get_peers_count(blockchain.as_str())
            .await?;

        if sharding_table_length == u128::from(local_count) {
            return Ok(());
        }

        self.pull_blockchain_sharding_table(blockchain, sharding_table_length, local_count)
            .await
    }

    async fn pull_blockchain_sharding_table(
        &self,
        blockchain: &BlockchainId,
        sharding_table_length: u128,
        local_count: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::debug!(
            blockchain = %blockchain,
            chain_length = sharding_table_length,
            local_count = local_count,
            "Refreshing local sharding table"
        );

        if sharding_table_length == 0 {
            self.repository_manager
                .shard_repository()
                .replace_sharding_table(blockchain.as_str(), Vec::new())
                .await?;
            return Ok(());
        }

        let mut starting_identity_id = self
            .blockchain_manager
            .get_sharding_table_head(blockchain)
            .await?;
        let mut page_index = 0usize;
        let mut records = Vec::new();
        let mut total_nodes_processed: u128 = 0;

        while total_nodes_processed < sharding_table_length {
            let nodes = self
                .blockchain_manager
                .get_sharding_table_page(blockchain, starting_identity_id, SHARDING_TABLE_PAGE_SIZE)
                .await?;

            if nodes.is_empty() {
                break;
            }

            let slice_start = if page_index == 0 { 0 } else { 1 };
            let nodes_in_page = nodes.len().saturating_sub(slice_start) as u128;
            self.append_shard_records(blockchain, &nodes, slice_start, &mut records);
            total_nodes_processed += nodes_in_page;

            // Check if we've reached the end of the linked list
            let last_node = nodes.last().expect("non-empty nodes");
            if last_node.identity_id == 0 || last_node.identity_id == starting_identity_id {
                break;
            }

            starting_identity_id = last_node.identity_id;
            page_index += 1;
        }

        self.repository_manager
            .shard_repository()
            .replace_sharding_table(blockchain.as_str(), records)
            .await?;

        Ok(())
    }

    fn append_shard_records(
        &self,
        blockchain: &BlockchainId,
        nodes: &[ShardingTableNode],
        slice_start: usize,
        records: &mut Vec<ShardRecordInput>,
    ) {
        for node in nodes.iter().skip(slice_start) {
            if node.node_id.is_empty() {
                tracing::warn!(
                    blockchain = %blockchain,
                    "Skipping sharding table node with empty node id bytes"
                );
                continue;
            }

            // The node_id is stored on-chain as UTF-8 bytes of the base58 peer ID string,
            // so we need to convert to string first, then parse as PeerId
            let peer_id_str = match std::str::from_utf8(&node.node_id) {
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

            let peer_id: network::PeerId = match peer_id_str.parse() {
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

            let ask = from_wei(&node.ask.to_string());
            let stake = from_wei(&node.stake.to_string());
            let sha256 = sha256_hex(&peer_id.to_bytes());

            records.push(ShardRecordInput {
                peer_id: peer_id.to_base58(),
                blockchain_id: blockchain.to_string(),
                ask,
                stake,
                sha256,
            });
        }
    }
}

#[derive(Clone, Default)]
pub struct ShardingTableCheckCommandData;

impl CommandHandler<ShardingTableCheckCommandData> for ShardingTableCheckCommandHandler {
    async fn execute(&self, _: &ShardingTableCheckCommandData) -> CommandExecutionResult {
        let blockchain_ids = self.blockchain_manager.get_blockchain_ids();

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
            delay_ms: SHARDING_TABLE_CHECK_COMMAND_PERIOD_MS,
        }
    }
}
