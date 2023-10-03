use blockchain::{
    AskUpdatedFilter, BlockchainName, ContractName, EventLog, EventName, NodeAddedFilter,
    NodeRemovedFilter, StakeIncreasedFilter,
};
use chrono::DateTime;
use std::{collections::HashMap, sync::Arc, vec};
use validation::HashFunction;

use crate::context::Context;

pub struct BlockchainEventHandler {
    context: Arc<Context>,
}

impl BlockchainEventHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self { context }
    }

    fn get_contracts_to_listen_to() -> Vec<ContractName> {
        vec![
            ContractName::Hub,
            ContractName::ShardingTable,
            ContractName::Staking,
            ContractName::Profile,
            ContractName::CommitManagerV1U1,
            ContractName::ServiceAgreementV1,
        ]
    }

    fn get_events_to_listen_to(contract_name: &ContractName) -> Vec<EventName> {
        match contract_name {
            ContractName::Hub => vec![
                EventName::NewContract,
                EventName::ContractChanged,
                EventName::NewAssetStorage,
                EventName::AssetStorageChanged,
            ],

            ContractName::ShardingTable => vec![EventName::NodeAdded, EventName::NodeRemoved],

            ContractName::Staking => {
                vec![EventName::StakeIncreased, EventName::StakeWithdrawalStarted]
            }

            ContractName::Profile => vec![EventName::AskUpdated],

            ContractName::CommitManagerV1U1 => vec![EventName::StateFinalized],

            ContractName::ServiceAgreementV1 => vec![
                EventName::ServiceAgreementV1Extended,
                EventName::ServiceAgreementV1Terminated,
            ],
        }
    }

    pub async fn handle_blockchain_events(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
        loop {
            interval.tick().await;
            let blockchains = self.context.blockchain_manager().get_blockchain_names();
            let mut handles = Vec::new();

            for blockchain in blockchains {
                let context_clone = Arc::clone(&self.context);
                let blockchain = blockchain.clone();

                let handle = tokio::spawn(async move {
                    let event_logs =
                        Self::get_blockchain_event_logs(&context_clone, &blockchain).await;
                    Self::process_event_logs(&context_clone, blockchain, event_logs).await;
                });

                handles.push(handle);
            }

            futures::future::join_all(handles).await;
        }
    }

    async fn get_blockchain_event_logs(
        context: &Arc<Context>,
        blockchain: &BlockchainName,
    ) -> Vec<EventLog> {
        let to_block = context
            .blockchain_manager()
            .get_block_number(blockchain)
            .await
            .unwrap();

        let mut event_logs = Vec::new();
        let contract_names = Self::get_contracts_to_listen_to();
        let filtered_contract_names = contract_names
            .iter()
            .filter(|c| **c != ContractName::Hub || !context.config().is_dev_env);
        for contract_name in filtered_contract_names {
            let last_checked_block = context
                .repository_manager()
                .blockchain_repository()
                .get_last_checked_block(blockchain.as_str(), contract_name.as_str())
                .await
                .unwrap();

            let from_block = last_checked_block + 1;
            if from_block >= to_block {
                continue;
            }

            event_logs.extend(
                context
                    .blockchain_manager()
                    .get_event_logs(
                        blockchain,
                        contract_name,
                        &Self::get_events_to_listen_to(contract_name),
                        from_block,
                        to_block,
                    )
                    .await
                    .unwrap(),
            );

            context
                .repository_manager()
                .blockchain_repository()
                .update_last_checked_block(
                    blockchain.as_str(),
                    contract_name.as_str(),
                    to_block,
                    chrono::Utc::now(),
                )
                .await
                .unwrap();
        }

        event_logs
    }

    async fn process_event_logs(
        context: &Arc<Context>,
        blockchain: BlockchainName,
        event_logs: Vec<EventLog>,
    ) {
        let mut grouped_events: HashMap<u64, Vec<EventLog>> = HashMap::new();
        for event_log in event_logs {
            let block_number = event_log.log().block_number.unwrap_or_default();
            grouped_events
                .entry(block_number.as_u64())
                .or_default()
                .push(event_log);
        }

        let mut ordered_blocks: Vec<(u64, Vec<EventLog>)> = grouped_events.into_iter().collect();
        ordered_blocks.sort_by_key(|(block_number, _)| *block_number);

        for (_, event_logs) in ordered_blocks {
            let mut handles = Vec::new();

            for event_log in event_logs {
                // Clone necessary data
                let context_clone = Arc::clone(context);
                let blockchain_clone = blockchain.clone();

                let handle = tokio::spawn(async move {
                    match event_log.event_name() {
                        EventName::NewContract => {
                            tracing::debug!("Found NewContract event!");
                        }
                        EventName::ContractChanged => {
                            tracing::debug!("Found ContractChanged event!");
                        }
                        EventName::NewAssetStorage => {
                            tracing::debug!("Found NewAssetStorage event!");
                        }
                        EventName::AssetStorageChanged => {
                            tracing::debug!("Found AssetStorageChanged event!");
                        }
                        EventName::NodeAdded => {
                            Self::handle_node_added(&context_clone, &blockchain_clone, event_log)
                                .await;
                        }
                        EventName::NodeRemoved => {
                            Self::handle_node_removed(&context_clone, &blockchain_clone, event_log)
                                .await;
                        }
                        EventName::StakeIncreased => {
                            Self::handle_stake_increased(
                                &context_clone,
                                &blockchain_clone,
                                event_log,
                            )
                            .await;
                        }
                        EventName::StakeWithdrawalStarted => {
                            tracing::debug!("Found StakeWithdrawalStarted event!");
                        }
                        EventName::AskUpdated => {
                            Self::handle_ask_updated(&context_clone, &blockchain_clone, event_log)
                                .await;
                        }
                        EventName::StateFinalized => {
                            tracing::debug!("Found StateFinalized event!");
                        }
                        EventName::ServiceAgreementV1Extended => {
                            tracing::debug!("Found ServiceAgreementV1Extended event!");
                        }
                        EventName::ServiceAgreementV1Terminated => {
                            tracing::debug!("Found ServiceAgreementV1Terminated event!");
                        }
                    }
                });

                handles.push(handle);
            }

            futures::future::join_all(handles).await;
        }
    }

    async fn handle_node_added(
        context: &Arc<Context>,
        blockchain: &BlockchainName,
        event_log: EventLog,
    ) {
        let node_added_filter = blockchain::utils::decode_event_log::<NodeAddedFilter>(event_log);
        let peer_id_bytes = node_added_filter.node_id.into_iter().collect::<Vec<u8>>();
        let peer_id = String::from_utf8(peer_id_bytes.clone()).unwrap();
        tracing::debug!("Adding peer id: {} to sharding table.", peer_id);

        context
            .repository_manager()
            .shard_repository()
            .create_peer_record(repository::models::shard::Model {
                peer_id,
                blockchain_id: blockchain.as_str().to_owned(),
                ask: blockchain::utils::from_wei(&node_added_filter.ask.to_string()),
                stake: blockchain::utils::from_wei(&node_added_filter.stake.to_string()),
                last_seen: DateTime::from_timestamp(0, 0).unwrap(),
                last_dialed: DateTime::from_timestamp(0, 0).unwrap(),
                sha256: context
                    .validation_manager()
                    .call_hash_function(HashFunction::Sha256, peer_id_bytes)
                    .await,
            })
            .await
            .unwrap();
    }

    async fn handle_node_removed(
        context: &Arc<Context>,
        blockchain: &BlockchainName,
        event_log: EventLog,
    ) {
        let node_removed_filter =
            blockchain::utils::decode_event_log::<NodeRemovedFilter>(event_log);
        let peer_id_bytes = node_removed_filter.node_id.into_iter().collect::<Vec<u8>>();
        let peer_id = String::from_utf8(peer_id_bytes.clone()).unwrap();

        tracing::debug!("Removing peer id: {} from sharding table.", peer_id);

        context
            .repository_manager()
            .shard_repository()
            .remove_peer_record(blockchain.as_str(), &peer_id)
            .await
            .unwrap();
    }

    async fn handle_stake_increased(
        context: &Arc<Context>,
        blockchain: &BlockchainName,
        event_log: EventLog,
    ) {
        let stake_increased_filter =
            blockchain::utils::decode_event_log::<StakeIncreasedFilter>(event_log);
        let peer_id_bytes = stake_increased_filter
            .node_id
            .into_iter()
            .collect::<Vec<u8>>();
        let peer_id = String::from_utf8(peer_id_bytes.clone()).unwrap();

        tracing::debug!(
            "Updating stake value for peer id: {} in sharding table.",
            peer_id
        );

        context
            .repository_manager()
            .shard_repository()
            .update_peer_stake(
                peer_id,
                blockchain.as_str().to_owned(),
                blockchain::utils::from_wei(&stake_increased_filter.new_stake.to_string()),
            )
            .await
            .unwrap();
    }

    async fn handle_ask_updated(
        context: &Arc<Context>,
        blockchain: &BlockchainName,
        event_log: EventLog,
    ) {
        let ask_updated_filter = blockchain::utils::decode_event_log::<AskUpdatedFilter>(event_log);
        let peer_id_bytes = ask_updated_filter.node_id.into_iter().collect::<Vec<u8>>();
        let peer_id = String::from_utf8(peer_id_bytes.clone()).unwrap();

        tracing::debug!(
            "Updating ask value for peer id: {} in sharding table.",
            peer_id
        );

        context
            .repository_manager()
            .shard_repository()
            .update_peer_ask(
                blockchain.as_str().to_owned(),
                peer_id,
                blockchain::utils::from_wei(&ask_updated_filter.ask.to_string()),
            )
            .await
            .unwrap();
    }
}
