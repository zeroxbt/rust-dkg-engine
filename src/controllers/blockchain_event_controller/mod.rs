use blockchain::{
    AskUpdatedFilter, AssetStorageChangedFilter, BlockchainManager, BlockchainName,
    ContractChangedFilter, ContractName, EventLog, EventName, NewAssetStorageFilter,
    NewContractFilter, NodeAddedFilter, NodeRemovedFilter, StakeIncreasedFilter,
    StakeWithdrawalStartedFilter,
};
use chrono::DateTime;
use futures::stream::{FuturesUnordered, StreamExt};
use repository::RepositoryManager;
use std::{collections::HashMap, sync::Arc, vec};
use validation::{HashFunction, ValidationManager};

use crate::{config::Config, context::Context};

const EVENT_FETCH_INTERVAL_SEC: u64 = 2;

pub struct BlockchainEventController {
    config: Arc<Config>,
    blockchain_manager: Arc<BlockchainManager>,
    repository_manager: Arc<RepositoryManager>,
    validation_manager: Arc<ValidationManager>,
}

impl BlockchainEventController {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            config: Arc::clone(context.config()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            repository_manager: Arc::clone(context.repository_manager()),
            validation_manager: Arc::clone(context.validation_manager()),
        }
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

            ContractName::ContentAssetStorage => vec![],
        }
    }

    pub async fn listen_and_handle_events(&self) {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(EVENT_FETCH_INTERVAL_SEC));
        loop {
            interval.tick().await;
            self.retrieve_and_handle_unprocessed_events().await
        }
    }

    pub async fn retrieve_and_handle_unprocessed_events(&self) {
        let blockchains = self.blockchain_manager.get_blockchain_names();
        let mut futures_unordered = FuturesUnordered::new();

        for blockchain in blockchains {
            futures_unordered.push(async {
                let event_logs = self.get_blockchain_event_logs(blockchain).await;
                self.process_event_logs(blockchain, event_logs).await;
            });
        }

        while (futures_unordered.next().await).is_some() {}
    }

    async fn get_blockchain_event_logs(&self, blockchain: &BlockchainName) -> Vec<EventLog> {
        let to_block = self
            .blockchain_manager
            .get_block_number(blockchain)
            .await
            .unwrap();

        let mut event_logs = Vec::new();
        let contract_names = Self::get_contracts_to_listen_to();
        let filtered_contract_names = contract_names
            .iter()
            .filter(|c| **c != ContractName::Hub || !self.config.is_dev_env);
        for contract_name in filtered_contract_names {
            let last_checked_block = self
                .repository_manager
                .blockchain_repository()
                .get_last_checked_block(blockchain.as_str(), contract_name.as_str())
                .await
                .unwrap();

            let from_block = last_checked_block + 1;
            if from_block >= to_block {
                continue;
            }

            event_logs.extend(
                self.blockchain_manager
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

            self.repository_manager
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

    async fn process_event_logs(&self, blockchain: &BlockchainName, event_logs: Vec<EventLog>) {
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
            let mut futures_unordered = FuturesUnordered::new();

            for event_log in event_logs {
                futures_unordered.push(async {
                    match event_log.event_name() {
                        EventName::NewContract => {
                            self.handle_contract_changed(blockchain, event_log).await;
                        }
                        EventName::ContractChanged => {
                            self.handle_contract_changed(blockchain, event_log).await;
                        }
                        EventName::NewAssetStorage => {
                            self.handle_contract_changed(blockchain, event_log).await;
                        }
                        EventName::AssetStorageChanged => {
                            self.handle_contract_changed(blockchain, event_log).await;
                        }
                        EventName::NodeAdded => {
                            self.handle_node_added(blockchain, event_log).await;
                        }
                        EventName::NodeRemoved => {
                            self.handle_node_removed(blockchain, event_log).await;
                        }
                        EventName::StakeIncreased => {
                            self.handle_stake_updated(blockchain, event_log).await;
                        }
                        EventName::StakeWithdrawalStarted => {
                            self.handle_stake_updated(blockchain, event_log).await;
                        }
                        EventName::AskUpdated => {
                            self.handle_ask_updated(blockchain, event_log).await;
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
            }

            while (futures_unordered.next().await).is_some() {}
        }
    }

    async fn handle_node_added(&self, blockchain: &BlockchainName, event_log: EventLog) {
        let node_added_filter = blockchain::utils::decode_event_log::<NodeAddedFilter>(event_log);
        let peer_id_bytes = node_added_filter.node_id.into_iter().collect::<Vec<u8>>();
        let peer_id = String::from_utf8(peer_id_bytes.clone()).unwrap();
        tracing::debug!("Adding peer id: {} to sharding table.", peer_id);

        self.repository_manager
            .shard_repository()
            .create_peer_record(repository::models::shard::Model {
                peer_id,
                blockchain_id: blockchain.as_str().to_owned(),
                ask: blockchain::utils::from_wei(&node_added_filter.ask.to_string()),
                stake: blockchain::utils::from_wei(&node_added_filter.stake.to_string()),
                last_seen: DateTime::from_timestamp(0, 0).unwrap(),
                last_dialed: DateTime::from_timestamp(0, 0).unwrap(),
                sha256: self
                    .validation_manager
                    .call_hash_function(HashFunction::Sha256, peer_id_bytes)
                    .await,
            })
            .await
            .unwrap();
    }

    async fn handle_node_removed(&self, blockchain: &BlockchainName, event_log: EventLog) {
        let node_removed_filter =
            blockchain::utils::decode_event_log::<NodeRemovedFilter>(event_log);
        let peer_id_bytes = node_removed_filter.node_id.into_iter().collect::<Vec<u8>>();
        let peer_id = String::from_utf8(peer_id_bytes.clone()).unwrap();

        tracing::debug!("Removing peer id: {} from sharding table.", peer_id);

        self.repository_manager
            .shard_repository()
            .remove_peer_record(blockchain.as_str(), &peer_id)
            .await
            .unwrap();
    }

    async fn handle_stake_updated(&self, blockchain: &BlockchainName, event_log: EventLog) {
        let (node_id, new_stake) = match event_log.event_name() {
            EventName::StakeIncreased => {
                let filter = blockchain::utils::decode_event_log::<StakeIncreasedFilter>(event_log);
                (filter.node_id, filter.new_stake)
            }
            EventName::StakeWithdrawalStarted => {
                let filter =
                    blockchain::utils::decode_event_log::<StakeWithdrawalStartedFilter>(event_log);
                (filter.node_id, filter.new_stake)
            }
            _ => panic!("Unexpected event name in handle stake updated log!"),
        };

        let peer_id_bytes = node_id.into_iter().collect::<Vec<u8>>();
        let peer_id = String::from_utf8(peer_id_bytes.clone()).unwrap();

        tracing::debug!(
            "Updating stake value for peer id: {} in sharding table.",
            peer_id
        );

        self.repository_manager
            .shard_repository()
            .update_peer_stake(
                peer_id,
                blockchain.as_str().to_owned(),
                blockchain::utils::from_wei(&new_stake.to_string()),
            )
            .await
            .unwrap();
    }

    async fn handle_ask_updated(&self, blockchain: &BlockchainName, event_log: EventLog) {
        let ask_updated_filter = blockchain::utils::decode_event_log::<AskUpdatedFilter>(event_log);
        let peer_id_bytes = ask_updated_filter.node_id.into_iter().collect::<Vec<u8>>();
        let peer_id = String::from_utf8(peer_id_bytes.clone()).unwrap();

        tracing::debug!(
            "Updating ask value for peer id: {} in sharding table.",
            peer_id
        );

        self.repository_manager
            .shard_repository()
            .update_peer_ask(
                blockchain.as_str().to_owned(),
                peer_id,
                blockchain::utils::from_wei(&ask_updated_filter.ask.to_string()),
            )
            .await
            .unwrap();
    }

    async fn handle_contract_changed(&self, blockchain: &BlockchainName, event_log: EventLog) {
        let (contract_name, new_contract_address) = match event_log.event_name() {
            EventName::NewContract => {
                let filter = blockchain::utils::decode_event_log::<NewContractFilter>(event_log);
                (filter.contract_name, filter.new_contract_address)
            }
            EventName::ContractChanged => {
                let filter =
                    blockchain::utils::decode_event_log::<ContractChangedFilter>(event_log);
                (filter.contract_name, filter.new_contract_address)
            }
            EventName::NewAssetStorage => {
                let filter =
                    blockchain::utils::decode_event_log::<NewAssetStorageFilter>(event_log);
                (filter.contract_name, filter.new_contract_address)
            }
            EventName::AssetStorageChanged => {
                let filter =
                    blockchain::utils::decode_event_log::<AssetStorageChangedFilter>(event_log);
                (filter.contract_name, filter.new_contract_address)
            }
            _ => panic!("Unexpected event name in handle stake updated log!"),
        };

        tracing::debug!(
            "New contract address found for {}: {}",
            contract_name,
            new_contract_address
        );

        let _ = self
            .blockchain_manager
            .re_initialize_contract(blockchain, contract_name, new_contract_address)
            .await;
    }
}
