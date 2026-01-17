use std::str::FromStr;

use alloy::{
    network::EthereumWallet,
    primitives::{Address, Bytes, B256, hex},
    providers::{Provider, ProviderBuilder},
    rpc::types::Filter,
    signers::local::PrivateKeySigner,
    sol_types::SolEvent,
};
use async_trait::async_trait;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    BlockchainConfig, BlockchainId,
    blockchains::blockchain_creator::{
        BlockchainProvider, Contracts, Staking, Token, sharding_table::ShardingTableLib::NodeInfo,
    },
    AssetStorageChangedFilter, ContractChangedFilter, KnowledgeCollectionCreatedFilter,
    NewAssetStorageFilter, NewContractFilter, ParameterChangedFilter,
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
    log: alloy::rpc::types::Log,
}

impl EventLog {
    pub fn new(
        contract_name: ContractName,
        event_name: EventName,
        log: alloy::rpc::types::Log,
    ) -> Self {
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

    pub fn log(&self) -> &alloy::rpc::types::Log {
        &self.log
    }
}

// Note: Must use async-trait here because this trait is used with trait objects (Box<dyn
// AbstractBlockchain>) Native async traits are not dyn-compatible yet
#[async_trait]
pub trait AbstractBlockchain: Send + Sync {
    fn blockchain_id(&self) -> &BlockchainId;
    fn config(&self) -> &BlockchainConfig;
    fn provider(&self) -> &BlockchainProvider;
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

        Ok(block_number)
    }

    async fn get_event_logs(
        &self,
        contract_name: &ContractName,
        events_to_filter: &Vec<EventName>,
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<EventLog>, BlockchainError> {
        let mut topic_map: std::collections::HashMap<B256, EventName> = std::collections::HashMap::new();
        for event in events_to_filter {
            let signature_hash = match event {
                EventName::NewContract => NewContractFilter::SIGNATURE_HASH,
                EventName::ContractChanged => ContractChangedFilter::SIGNATURE_HASH,
                EventName::NewAssetStorage => NewAssetStorageFilter::SIGNATURE_HASH,
                EventName::AssetStorageChanged => AssetStorageChangedFilter::SIGNATURE_HASH,
                EventName::ParameterChanged => ParameterChangedFilter::SIGNATURE_HASH,
                EventName::KnowledgeCollectionCreated => {
                    KnowledgeCollectionCreatedFilter::SIGNATURE_HASH
                }
            };
            topic_map.insert(signature_hash, event.clone());
        }

        let topic_signatures: Vec<B256> = topic_map.keys().copied().collect();

        let contracts = self.contracts().await;

        let addresses: Vec<Address> = vec![contracts.get_address(contract_name)?];
        drop(contracts);

        let mut all_events = Vec::new();

        for address in addresses {
            let mut block = from_block;
            while block <= current_block {
                let to_block = std::cmp::min(
                    block + MAXIMUM_NUMBERS_OF_BLOCKS_TO_FETCH - 1,
                    current_block,
                );

                let mut filter = Filter::new()
                    .address(address)
                    .from_block(block)
                    .to_block(to_block);
                if !topic_signatures.is_empty() {
                    filter = filter.event_signature(topic_signatures.clone());
                }

                let logs = self
                    .provider()
                    .get_logs(&filter)
                    .await
                    .map_err(|_| BlockchainError::GetLogs)?;

                for log in logs {
                    let Some(topic0) = log.topic0() else {
                        continue;
                    };
                    let Some(event_name) = topic_map.get(topic0) else {
                        continue;
                    };
                    all_events.push(EventLog::new(
                        contract_name.clone(),
                        event_name.clone(),
                        log,
                    ));
                }

                block = to_block + 1;
            }
        }

        Ok(all_events)
    }

    async fn get_sharding_table_head(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let contracts = self.contracts().await;
        let head: Uint<72, 2> = contracts.sharding_table_storage().head().call().await?;
        Ok(head.to::<u128>())
    }

    async fn get_sharding_table_length(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let contracts = self.contracts().await;
        let nodes_count: Uint<72, 2> = contracts
            .sharding_table_storage()
            .nodesCount()
            .call()
            .await?;
        Ok(nodes_count.to::<u128>())
    }

    async fn get_sharding_table_page(
        &self,
        starting_identity_id: u128,
        nodes_num: u128,
    ) -> Result<Vec<NodeInfo>, BlockchainError> {
        use alloy::primitives::Uint;
        let contracts = self.contracts().await;
        let nodes = contracts
            .sharding_table()
            .getShardingTable_1(
                Uint::<72, 2>::from(starting_identity_id),
                Uint::<72, 2>::from(nodes_num),
            )
            .call()
            .await?;

        Ok(nodes)
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
            .getIdentityId(evm_operational_address)
            .call()
            .await;

        match result {
            Ok(id) if !id.is_zero() => Some(id.to::<u128>()),
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

        // Profile ABI: createProfile(adminWallet, operationalWallets, nodeName, nodeId,
        // initialOperatorFee)
        let create_profile_call = contracts.profile().createProfile(
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
        use alloy::primitives::{U256, Uint};

        let config = self.config();

        // Get management wallet private key (required for staking)
        let management_pk = config.evm_management_wallet_private_key().ok_or_else(|| {
            BlockchainError::Custom(
                "Management wallet private key required for set_stake".to_string(),
            )
        })?;

        // Create a provider with the management wallet for staking operations
        let management_signer: PrivateKeySigner = management_pk.parse().map_err(|e| {
            BlockchainError::Custom(format!("Invalid management wallet key: {}", e))
        })?;
        let management_wallet = EthereumWallet::from(management_signer);

        let endpoint_url = config.rpc_endpoints()[0]
            .parse()
            .map_err(|e| BlockchainError::Custom(format!("Failed to parse RPC endpoint: {}", e)))?;

        let management_provider = ProviderBuilder::new()
            .wallet(management_wallet)
            .connect_http(endpoint_url);

        // Get contract addresses from existing contracts
        let contracts = self.contracts().await;
        let staking_address = *contracts.staking().address();
        let token_address = *contracts.token().address();
        drop(contracts);

        // Create contracts with management wallet provider
        let staking = Staking::new(staking_address, &management_provider);
        let token = Token::new(token_address, &management_provider);

        // Get identity ID
        let identity_id = self.get_identity_id().await.ok_or_else(|| {
            BlockchainError::Custom("Identity ID not found for staking".to_string())
        })?;

        // Approve token spending
        let approve_call = token.increaseAllowance(staking_address, U256::from(stake_wei));
        let approve_result = approve_call.send().await;

        handle_contract_call(approve_result).await?;

        // Stake tokens - identity_id is uint72, stake_wei is uint96
        let stake_call = staking.stake(
            Uint::<72, 2>::from(identity_id),
            Uint::<96, 2>::from(stake_wei),
        );
        let stake_result = stake_call.send().await;

        handle_contract_call(stake_result).await?;

        tracing::info!("Set stake completed for identity {}", identity_id);
        Ok(())
    }

    /// Sets the ask price for this node's identity (dev environment only).
    async fn set_ask(&self, ask_wei: u128) -> Result<(), BlockchainError> {
        use alloy::primitives::Uint;

        let contracts = self.contracts().await;

        // Get identity ID
        let identity_id = self.get_identity_id().await.ok_or_else(|| {
            BlockchainError::Custom("Identity ID not found for set_ask".to_string())
        })?;

        // Update ask via Profile contract - identity_id is uint72, ask_wei is uint96
        let update_ask_call = contracts.profile().updateAsk(
            Uint::<72, 2>::from(identity_id),
            Uint::<96, 2>::from(ask_wei),
        );
        let update_ask_result = update_ask_call.send().await;

        handle_contract_call(update_ask_result).await?;

        tracing::info!("Set ask completed for identity {}", identity_id);
        Ok(())
    }

    async fn sign_message(&self, message_hash: &str) -> Result<Vec<u8>, BlockchainError> {
        use alloy::signers::Signer;

        // Decode the hex message hash
        let message_bytes = hex::decode(message_hash.strip_prefix("0x").unwrap_or(message_hash))
            .map_err(|e| {
                BlockchainError::Custom(format!("Failed to decode message hash: {}", e))
            })?;

        // Re-create signer from config since we can't easily access it from the provider
        let config = self.config();
        let signer: PrivateKeySigner = config
            .evm_operational_wallet_private_key
            .parse()
            .map_err(|e| BlockchainError::Custom(format!("Failed to parse private key: {}", e)))?;

        // Sign the message
        let signature = signer
            .sign_message(&message_bytes)
            .await
            .map_err(|e| BlockchainError::Custom(format!("Failed to sign message: {}", e)))?;

        // Return the signature as bytes (r, s, v format - 65 bytes total)
        Ok(signature.as_bytes().to_vec())
    }
}
