use blockchain::{BlockchainManager, BlockchainName, ContractName, EventLog, EventName};
use repository::RepositoryManager;
use std::{collections::HashMap, sync::Arc};

pub struct BlockchainEventHandler {
    blockchain_manager: Arc<BlockchainManager>,
    repository_manager: Arc<RepositoryManager>,
    contract_events: HashMap<ContractName, Vec<EventName>>,
}

impl BlockchainEventHandler {
    pub fn new(
        blockchain_manager: Arc<BlockchainManager>,
        repository_manager: Arc<RepositoryManager>,
    ) -> Self {
        let mut contract_events: HashMap<ContractName, Vec<EventName>> = HashMap::new();

        contract_events.insert(
            ContractName::Hub,
            vec![
                EventName::NewContract,
                EventName::ContractChanged,
                EventName::NewAssetStorage,
                EventName::AssetStorageChanged,
            ],
        );
        contract_events.insert(
            ContractName::ShardingTable,
            vec![EventName::NodeAdded, EventName::NodeRemoved],
        );
        contract_events.insert(
            ContractName::Staking,
            vec![EventName::StakeIncreased, EventName::StakeWithdrawalStarted],
        );
        contract_events.insert(ContractName::Profile, vec![EventName::AskUpdated]);
        contract_events.insert(
            ContractName::CommitManagerV1U1, // Note: I'm assuming a typo in your original code ("CommitManagerV1U1")
            vec![EventName::StateFinalized],
        );
        contract_events.insert(
            ContractName::ServiceAgreementV1,
            vec![
                EventName::ServiceAgreementV1Extended,
                EventName::ServiceAgreementV1Terminated,
            ],
        );

        Self {
            blockchain_manager,
            repository_manager,
            contract_events,
        }
    }
    pub async fn handle_blockchain_events(&self) {
        loop {
            for blockchain in self.blockchain_manager.get_blockchain_names() {
                let event_logs = self.get_blockchain_event_logs(blockchain).await;

                for event_log in event_logs {
                    match event_log.event_name() {
                        EventName::NewContract => {
                            tracing::debug!("Found NewContract event! {:?}", event_log.log());
                        }
                        EventName::ContractChanged => {
                            tracing::debug!("Found ContractChanged event! {:?}", event_log.log());
                        }
                        EventName::NewAssetStorage => {
                            tracing::debug!("Found NewAssetStorage event! {:?}", event_log.log());
                        }
                        EventName::AssetStorageChanged => {
                            tracing::debug!(
                                "Found AssetStorageChanged event! {:?}",
                                event_log.log()
                            );
                        }
                        EventName::NodeAdded => {
                            tracing::debug!("Found NodeAdded event! {:?}", event_log.log());
                        }
                        EventName::NodeRemoved => {
                            tracing::debug!("Found NodeRemoved event! {:?}", event_log.log());
                        }
                        EventName::StakeIncreased => {
                            tracing::debug!("Found StakeIncreased event! {:?}", event_log.log());
                        }
                        EventName::StakeWithdrawalStarted => {
                            tracing::debug!(
                                "Found StakeWithdrawalStarted event! {:?}",
                                event_log.log()
                            );
                        }
                        EventName::AskUpdated => {
                            tracing::debug!("Found AskUpdated event! {:?}", event_log.log());
                        }
                        EventName::StateFinalized => {
                            tracing::debug!("Found StateFinalized event! {:?}", event_log.log());
                        }
                        EventName::ServiceAgreementV1Extended => {
                            tracing::debug!(
                                "Found ServiceAgreementV1Extended event! {:?}",
                                event_log.log()
                            );
                        }
                        EventName::ServiceAgreementV1Terminated => {
                            tracing::debug!(
                                "Found ServiceAgreementV1Terminated event! {:?}",
                                event_log.log()
                            );
                        }
                    }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
    }

    pub async fn get_blockchain_event_logs(&self, blockchain: &BlockchainName) -> Vec<EventLog> {
        let block_number = self
            .blockchain_manager
            .get_block_number(blockchain)
            .await
            .unwrap();

        let mut event_logs = Vec::new();
        for (contract_name, event_names) in self.contract_events.iter() {
            let last_checked_block = self
                .repository_manager
                .blockchain_repository()
                .get_last_checked_block(blockchain.as_str(), contract_name.as_str())
                .await
                .unwrap();

            event_logs.extend(
                self.blockchain_manager
                    .get_event_logs(
                        blockchain,
                        contract_name,
                        event_names,
                        last_checked_block,
                        block_number,
                    )
                    .await
                    .unwrap(),
            );

            self.repository_manager
                .blockchain_repository()
                .update_last_checked_block(
                    blockchain.as_str(),
                    contract_name.as_str(),
                    block_number,
                    chrono::Utc::now(),
                )
                .await
                .unwrap();
        }

        event_logs
            .sort_unstable_by_key(|event_log| event_log.log().block_number.unwrap_or_default());

        event_logs
    }
}
