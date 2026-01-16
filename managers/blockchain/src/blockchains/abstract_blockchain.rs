use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use ethers::{
    abi::Address,
    middleware::Middleware,
    prelude::Contract,
    types::{Bytes, Filter, Log},
};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    BlockchainConfig, BlockchainId,
    blockchains::blockchain_creator::{
        BlockchainProvider, Contracts, ShardingTableNode, Staking, Token,
    },
    error::BlockchainError,
    utils::handle_contract_call,
};

const MAXIMUM_NUMBERS_OF_BLOCKS_TO_FETCH: u64 = 50;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ContractName {
    Hub,
    ShardingTable,
    ShardingTableStorage,
    Staking,
    Profile,
    ParametersStorage,
    KnowledgeCollectionStorage,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum EventName {
    // Hub events
    NewContract,
    ContractChanged,
    NewAssetStorage,
    AssetStorageChanged,
    // ParametersStorage events
    ParameterChanged,
    // KnowledgeCollectionStorage events
    KnowledgeCollectionCreated,
}

impl ContractName {
    pub fn as_str(&self) -> &str {
        match self {
            ContractName::Hub => "Hub",
            ContractName::ShardingTable => "ShardingTable",
            ContractName::ShardingTableStorage => "ShardingTableStorage",
            ContractName::Staking => "Staking",
            ContractName::Profile => "Profile",
            ContractName::ParametersStorage => "ParametersStorage",
            ContractName::KnowledgeCollectionStorage => "KnowledgeCollectionStorage",
        }
    }
}

impl FromStr for ContractName {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Hub" => Ok(ContractName::Hub),
            "ShardingTable" => Ok(ContractName::ShardingTable),
            "ShardingTableStorage" => Ok(ContractName::ShardingTableStorage),
            "Staking" => Ok(ContractName::Staking),
            "Profile" => Ok(ContractName::Profile),
            "ParametersStorage" => Ok(ContractName::ParametersStorage),
            "KnowledgeCollectionStorage" => Ok(ContractName::KnowledgeCollectionStorage),
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
            EventName::ParameterChanged => "ParameterChanged",
            EventName::KnowledgeCollectionCreated => "KnowledgeCollectionCreated",
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

// Note: Must use async-trait here because this trait is used with trait objects (Box<dyn
// AbstractBlockchain>) Native async traits are not dyn-compatible yet
#[async_trait]
pub trait AbstractBlockchain: Send + Sync {
    fn blockchain_id(&self) -> &BlockchainId;
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
        if *contract_name == ContractName::KnowledgeCollectionStorage {
            let storage_addresses = contracts.get_knowledge_collection_storage_addresses();
            let mut all_events = Vec::new();

            for address in storage_addresses {
                let Some(contract) = contracts.get_knowledge_collection_storage(&address) else {
                    continue;
                };
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
                    all_events.extend(new_events);
                    from_block = to_block + 1;
                }
            }

            return Ok(all_events);
        }

        let contract = contracts.get(contract_name)?;

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

    async fn get_sharding_table_head(&self) -> Result<u128, BlockchainError> {
        let contracts = self.contracts().await;
        let head = contracts.sharding_table_storage().head().call().await?;
        Ok(head)
    }

    async fn get_sharding_table_length(&self) -> Result<u128, BlockchainError> {
        let contracts = self.contracts().await;
        let nodes_count = contracts
            .sharding_table_storage()
            .nodes_count()
            .call()
            .await?;
        Ok(nodes_count)
    }

    async fn get_sharding_table_page(
        &self,
        starting_identity_id: u128,
        nodes_num: u128,
    ) -> Result<Vec<ShardingTableNode>, BlockchainError> {
        let contracts = self.contracts().await;
        let nodes = contracts
            .sharding_table()
            .get_sharding_table_with_starting_identity_id_and_nodes_number(
                starting_identity_id,
                nodes_num,
            )
            .call()
            .await?;

        Ok(nodes)
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
                .map_err(|_| BlockchainError::EventNotFound {
                    event_name: event_name.as_str().to_string(),
                })?
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
            .map_err(|_| BlockchainError::InvalidAddress {
                address: config.evm_management_wallet_public_key.clone(),
            })?;
        let peer_id_bytes = Bytes::from(peer_id.as_bytes().to_vec());
        let shares_token_name = config.shares_token_name.to_string();
        let shares_token_symbol = config.shares_token_symbol.to_string();

        let contracts = self.contracts().await;

        // Profile ABI: create_profile(adminWallet, operationalWallets, nodeName, nodeId,
        // initialOperatorFee)
        let create_profile_call = contracts.profile().create_profile(
            admin_wallet,
            vec![],
            shares_token_name.clone(), // nodeName
            peer_id_bytes,             // nodeId
            0u16,                      // initialOperatorFee
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
            self.create_profile(peer_id)
                .await
                .map_err(|_| BlockchainError::ProfileCreation)?;
        }

        let identity_id = self.get_identity_id().await;

        if let Some(id) = identity_id {
            tracing::info!("Identity ID: {}", id);

            self.set_identity_id(id);
            Ok(())
        } else {
            Err(BlockchainError::IdentityNotFound)
        }
    }

    // Note: get_assertion_id_by_index removed - ContentAssetStorage not currently in use

    /// Sets the stake for this node's identity (dev environment only).
    /// Requires management wallet private key to be configured.
    async fn set_stake(&self, stake_wei: u128) -> Result<(), BlockchainError> {
        use ethers::{
            middleware::MiddlewareBuilder,
            providers::{Http, Provider},
            signers::{LocalWallet, Signer},
        };

        let config = self.config();

        // Get management wallet private key (required for staking)
        let management_pk = config.evm_management_wallet_private_key().ok_or_else(|| {
            BlockchainError::Custom(
                "Management wallet private key required for set_stake".to_string(),
            )
        })?;

        // Create a provider with the management wallet for staking operations
        let management_wallet = management_pk
            .parse::<LocalWallet>()
            .map_err(|e| BlockchainError::Custom(format!("Invalid management wallet key: {}", e)))?
            .with_chain_id(config.chain_id());
        let management_address = management_wallet.address();

        let http = Http::from_str(&config.rpc_endpoints()[0]).map_err(|e| {
            BlockchainError::Custom(format!("Failed to create HTTP provider: {}", e))
        })?;
        let management_provider = Arc::new(
            Provider::new(http)
                .with_signer(management_wallet)
                .nonce_manager(management_address),
        );

        // Get contract addresses from existing contracts
        let contracts = self.contracts().await;
        let staking_address = contracts.staking().address();
        let token_address = contracts.token().address();
        drop(contracts);

        // Create contracts with management wallet provider
        let staking = Staking::new(staking_address, Arc::clone(&management_provider));
        let token = Token::new(token_address, Arc::clone(&management_provider));

        // Get identity ID
        let identity_id = self.get_identity_id().await.ok_or_else(|| {
            BlockchainError::Custom("Identity ID not found for staking".to_string())
        })?;

        // Approve token spending
        let approve_call =
            token.increase_allowance(staking_address, ethers::types::U256::from(stake_wei));
        let approve_result = approve_call.send().await;

        handle_contract_call(approve_result).await?;

        // Stake tokens
        let stake_call = staking.stake(identity_id, stake_wei);
        let stake_result = stake_call.send().await;

        handle_contract_call(stake_result).await?;

        tracing::info!("Set stake completed for identity {}", identity_id);
        Ok(())
    }

    /// Sets the ask price for this node's identity (dev environment only).
    async fn set_ask(&self, ask_wei: u128) -> Result<(), BlockchainError> {
        let contracts = self.contracts().await;

        // Get identity ID
        let identity_id = self.get_identity_id().await.ok_or_else(|| {
            BlockchainError::Custom("Identity ID not found for set_ask".to_string())
        })?;

        // Update ask via Profile contract
        let update_ask_call = contracts.profile().update_ask(identity_id, ask_wei);
        let update_ask_result = update_ask_call.send().await;

        handle_contract_call(update_ask_result).await?;

        tracing::info!("Set ask completed for identity {}", identity_id);
        Ok(())
    }

    async fn sign_message(&self, message_hash: &str) -> Result<Vec<u8>, BlockchainError> {
        use ethers::{signers::Signer, utils::hex};

        // Decode the hex message hash
        let message_bytes = hex::decode(message_hash.strip_prefix("0x").unwrap_or(message_hash))
            .map_err(|e| {
                BlockchainError::Custom(format!("Failed to decode message hash: {}", e))
            })?;

        // Access the signer through the provider's inner middleware
        // BlockchainProvider is NonceManagerMiddleware<SignerMiddleware<Provider<Http>,
        // LocalWallet>>
        let provider = self.provider();
        let signer = provider.inner().signer();

        // Sign the message
        let signature = signer
            .sign_message(&message_bytes)
            .await
            .map_err(|e| BlockchainError::Custom(format!("Failed to sign message: {}", e)))?;

        // Return the flat signature as bytes
        Ok(signature.to_vec())
    }
}
