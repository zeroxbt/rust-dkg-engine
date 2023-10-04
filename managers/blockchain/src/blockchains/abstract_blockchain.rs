use crate::{
    blockchains::blockchain_creator::{BlockchainProvider, Contracts},
    utils::handle_contract_call,
    BlockchainConfig, BlockchainName,
};
use async_trait::async_trait;
use ethers::{
    abi::Address,
    middleware::Middleware,
    prelude::Contract,
    types::{Bytes, Filter, Log},
};
use std::{str::FromStr, sync::Arc};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

const MAXIMUM_NUMBERS_OF_BLOCKS_TO_FETCH: u64 = 50;

#[derive(Debug, thiserror::Error)]
pub enum BlockchainError {
    #[error("Contract error: {0}")]
    Contract(#[from] ethers::contract::ContractError<BlockchainProvider>),

    #[error("Failed to decode message")]
    Decode,
    #[error("Failed to parse address")]
    Parse,
    #[error("Failed to get logs")]
    GetLogs,
    #[error("Failed to get block number")]
    GetBlockNumber,
    #[error("Error: {0}")]
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ContractName {
    Hub,
    ShardingTable,
    Staking,
    Profile,
    CommitManagerV1U1,
    ServiceAgreementV1,
    ContentAssetStorage,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum EventName {
    NewContract,
    ContractChanged,
    NewAssetStorage,
    AssetStorageChanged,
    NodeAdded,
    NodeRemoved,
    StakeIncreased,
    StakeWithdrawalStarted,
    AskUpdated,
    StateFinalized,
    ServiceAgreementV1Extended,
    ServiceAgreementV1Terminated,
}

impl ContractName {
    pub fn as_str(&self) -> &str {
        match self {
            ContractName::Hub => "Hub",
            ContractName::ShardingTable => "ShardingTable",
            ContractName::Staking => "Staking",
            ContractName::CommitManagerV1U1 => "CommitManagerV1U1",
            ContractName::ServiceAgreementV1 => "ServiceAgreementV1",
            ContractName::Profile => "Profile",
            ContractName::ContentAssetStorage => "ContentAssetStorage",
        }
    }
}

impl FromStr for ContractName {
    type Err = String; // You can use a more complex error type if needed

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Hub" => Ok(ContractName::Hub),
            "ShardingTable" => Ok(ContractName::ShardingTable),
            "Staking" => Ok(ContractName::Staking),
            "Profile" => Ok(ContractName::Profile),
            "CommitManagerV1U1" => Ok(ContractName::CommitManagerV1U1),
            "ServiceAgreementV1" => Ok(ContractName::ServiceAgreementV1),
            // ... other match arms for your variants
            _ => Err(format!("'{}' is not a valid contract name", s)),
        }
    }
}

impl EventName {
    pub fn as_str(&self) -> &str {
        match self {
            EventName::NewContract => "NewContract",
            EventName::ContractChanged => "ContractChanged",
            EventName::NewAssetStorage => "NewAssetStorage",
            EventName::AssetStorageChanged => "AssetStorageChanged",
            EventName::NodeAdded => "NodeAdded",
            EventName::NodeRemoved => "NodeRemoved",
            EventName::StakeIncreased => "StakeIncreased",
            EventName::StakeWithdrawalStarted => "StakeWithdrawalStarted",
            EventName::AskUpdated => "AskUpdated",
            EventName::StateFinalized => "StateFinalized",
            EventName::ServiceAgreementV1Extended => "ServiceAgreementV1Extended",
            EventName::ServiceAgreementV1Terminated => "ServiceAgreementV1Terminated",
        }
    }
}

pub struct EventLog {
    contract_name: ContractName,
    event_name: EventName,
    log: Log,
}

impl EventLog {
    pub fn new(contract_name: ContractName, event_name: EventName, log: Log) -> Self {
        Self {
            contract_name,
            event_name,
            log,
        }
    }

    pub fn contract_name(&self) -> &ContractName {
        &self.contract_name
    }

    pub fn event_name(&self) -> &EventName {
        &self.event_name
    }

    pub fn log(&self) -> &Log {
        &self.log
    }
}

#[async_trait]
pub trait AbstractBlockchain: Send + Sync {
    fn name(&self) -> &BlockchainName;
    fn config(&self) -> &BlockchainConfig;
    fn provider(&self) -> &Arc<BlockchainProvider>;
    async fn contracts(&self) -> RwLockReadGuard<'_, Contracts>;
    async fn contracts_mut(&self) -> RwLockWriteGuard<'_, Contracts>;
    fn set_identity_id(&mut self, id: u128);

    async fn re_initialize_contract(
        &self,
        contract_name: String,
        contract_address: Address,
    ) -> Result<(), BlockchainError> {
        let contract_name = contract_name
            .parse::<ContractName>()
            .map_err(BlockchainError::Custom)?;

        let mut contracts = self.contracts_mut().await;

        contracts
            .replace_contract(self.provider(), contract_name, contract_address)
            .await
    }

    async fn get_block_number(&self) -> Result<u64, BlockchainError> {
        let block_number = self
            .provider()
            .get_block_number()
            .await
            .map_err(|_| BlockchainError::GetLogs)?;

        Ok(block_number.as_u64())
    }

    async fn get_event_logs(
        &self,
        contract_name: &ContractName,
        events_to_filter: &Vec<EventName>,
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<EventLog>, BlockchainError> {
        let contracts = self.contracts().await;
        let contract = contracts.get(contract_name);

        let mut events = Vec::new();
        let mut from_block = from_block;
        while from_block <= current_block {
            let to_block = std::cmp::min(
                from_block + MAXIMUM_NUMBERS_OF_BLOCKS_TO_FETCH - 1,
                current_block,
            );
            let new_events = self
                .process_block_range(
                    from_block,
                    to_block,
                    contract,
                    contract_name,
                    events_to_filter,
                )
                .await?;
            events.extend(new_events);
            from_block = to_block + 1;
        }

        Ok(events)
    }

    async fn process_block_range(
        &self,
        from_block: u64,
        to_block: u64,
        contract: &Contract<BlockchainProvider>,
        contract_name: &ContractName,
        events_to_filter: &Vec<EventName>,
    ) -> Result<Vec<EventLog>, BlockchainError> {
        let mut all_events = Vec::new();

        for event_name in events_to_filter {
            let filter = contract
                .event_for_name::<Filter>(event_name.as_str())
                .unwrap()
                .filter;
            let logs = self
                .provider()
                .get_logs(&filter.from_block(from_block).to_block(to_block))
                .await
                .map_err(|_| BlockchainError::GetLogs)?;

            for log in logs {
                all_events.push(EventLog::new(
                    contract_name.clone(),
                    event_name.clone(),
                    log,
                ));
            }
        }

        Ok(all_events)
    }

    async fn get_identity_id(&self) -> Option<u128> {
        let evm_operational_address = self
            .config()
            .evm_operational_wallet_public_key
            .parse::<Address>();
        let Ok(evm_operational_address) = evm_operational_address else {
            return None;
        };

        let contracts = self.contracts().await;

        let result = contracts
            .identity_storage()
            .get_identity_id(evm_operational_address)
            .call()
            .await;

        match result {
            Ok(id) if id != 0 => Some(id),
            _ => None,
        }
    }

    async fn identity_id_exists(&self) -> bool {
        let identity_id = self.get_identity_id().await;

        identity_id.is_some()
    }

    async fn create_profile(&self, peer_id: &str) -> Result<(), BlockchainError> {
        let config = self.config();
        let admin_wallet = config
            .evm_management_wallet_public_key
            .parse::<Address>()
            .map_err(|_| BlockchainError::Parse)?;
        let peer_id_bytes = Bytes::from(peer_id.as_bytes().to_vec());
        let shares_token_name = config.shares_token_name.to_string();
        let shares_token_symbol = config.shares_token_symbol.to_string();

        let contracts = self.contracts().await;

        let create_profile_call = contracts.profile().create_profile(
            admin_wallet,
            peer_id_bytes,
            shares_token_name.clone(),
            shares_token_symbol.clone(),
        );

        let result = create_profile_call.send().await;

        match handle_contract_call(result).await {
            Ok(_) => {
                tracing::info!(
                    "Profile created with token name: {}, token symbol: {}.",
                    shares_token_name,
                    shares_token_symbol
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn initialize_identity(&mut self, peer_id: &str) -> Result<(), BlockchainError> {
        if !self.identity_id_exists().await {
            let result = self.create_profile(peer_id).await;

            if result.is_err() {
                panic!("Unable to create profile");
            }
        }

        let identity_id = self.get_identity_id().await;

        if let Some(id) = identity_id {
            tracing::info!("Identity ID: {}", id);

            self.set_identity_id(id);
            Ok(())
        } else {
            panic!("Identity not found");
        }
    }
}
