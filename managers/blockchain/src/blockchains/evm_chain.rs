use std::str::FromStr;

use alloy::{
    network::EthereumWallet,
    primitives::{Address, B256, Bytes, U256, hex},
    providers::Provider,
    rpc::types::Filter,
    signers::local::{LocalSignerError, PrivateKeySigner},
};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    BlockchainConfig, BlockchainId, GasConfig,
    blockchains::blockchain_creator::{
        BlockchainProvider, Contracts, Profile, Staking, Token, initialize_contracts,
        initialize_provider, initialize_provider_with_wallet,
        sharding_table::ShardingTableLib::NodeInfo,
    },
    error::BlockchainError,
    error_utils::handle_contract_call,
    gas::fetch_gas_price_from_oracle,
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

pub struct ContractLog {
    contract_name: ContractName,
    log: alloy::rpc::types::Log,
}

impl ContractLog {
    pub fn new(contract_name: ContractName, log: alloy::rpc::types::Log) -> Self {
        Self { contract_name, log }
    }

    pub fn contract_name(&self) -> &ContractName {
        &self.contract_name
    }

    pub fn log(&self) -> &alloy::rpc::types::Log {
        &self.log
    }
}

pub struct EvmChain {
    config: BlockchainConfig,
    provider: BlockchainProvider,
    contracts: RwLock<Contracts>,
    identity_id: Option<u128>,
    gas_config: GasConfig,
}

impl EvmChain {
    pub async fn new(
        config: BlockchainConfig,
        gas_config: GasConfig,
    ) -> Result<Self, BlockchainError> {
        let provider =
            initialize_provider(&config)
                .await
                .map_err(|e| BlockchainError::ProviderInit {
                    reason: e.to_string(),
                })?;

        let contracts = initialize_contracts(&config, &provider)
            .await
            .map_err(|e| BlockchainError::ContractInit {
                reason: e.to_string(),
            })?;

        tracing::info!(
            "Initialized {} blockchain with gas config: default={} wei, max={} wei",
            config.blockchain_id(),
            gas_config.default_gas_price,
            gas_config.max_gas_price
        );

        Ok(Self {
            provider,
            config,
            contracts: RwLock::new(contracts),
            identity_id: None,
            gas_config,
        })
    }

    pub fn blockchain_id(&self) -> &BlockchainId {
        self.config.blockchain_id()
    }

    pub fn config(&self) -> &BlockchainConfig {
        &self.config
    }

    pub fn provider(&self) -> &BlockchainProvider {
        &self.provider
    }

    pub async fn contracts(&self) -> RwLockReadGuard<'_, Contracts> {
        self.contracts.read().await
    }

    pub async fn contracts_mut(&self) -> RwLockWriteGuard<'_, Contracts> {
        self.contracts.write().await
    }

    pub fn set_identity_id(&mut self, id: u128) {
        self.identity_id = Some(id);
    }

    pub fn gas_config(&self) -> &GasConfig {
        &self.gas_config
    }

    /// Get the current gas price.
    ///
    /// Tries sources in order:
    /// 1. Gas price oracle (if configured) - only used if price > default
    /// 2. Provider's gas price estimate - only used if price >= default
    /// 3. Default gas price from config
    ///
    /// This matches the JS implementation behavior where:
    /// - Gnosis validates oracle price > default before using it
    /// - Provider price must be >= default to be used
    pub async fn get_gas_price(&self) -> U256 {
        let default_price = self.gas_config.default_gas_price;

        // Try oracle first if configured
        if let Some(oracle_url) = self.config.gas_price_oracle_url() {
            match fetch_gas_price_from_oracle(oracle_url).await {
                Ok(price) => {
                    tracing::debug!("Gas price from oracle: {} wei", price);
                    // JS behavior: only use oracle price if > default (uses .gt(), not .gte())
                    // See gnosis-service.js: gasPrice.gt(this.defaultGasPrice)
                    if price > default_price {
                        return price;
                    }
                    tracing::debug!(
                        "Oracle price {} <= default {}, using default",
                        price,
                        default_price
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to fetch gas price from oracle: {}", e);
                }
            }
        }

        // Try provider's gas price estimate
        match self.provider.get_gas_price().await {
            Ok(price) => {
                let price = U256::from(price);
                tracing::debug!("Gas price from provider: {} wei", price);
                // Ensure we don't return less than the default
                if price >= default_price {
                    return price;
                }
                tracing::debug!(
                    "Provider price {} < default {}, using default",
                    price,
                    default_price
                );
            }
            Err(e) => {
                tracing::warn!("Failed to get gas price from provider: {}", e);
            }
        }

        // Fall back to default
        tracing::debug!("Using default gas price: {} wei", default_price);
        default_price
    }

    pub async fn re_initialize_contract(
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

    pub async fn get_block_number(&self) -> Result<u64, BlockchainError> {
        self.provider()
            .get_block_number()
            .await
            .map_err(BlockchainError::get_block_number)
    }

    pub async fn get_event_logs(
        &self,
        contract_name: &ContractName,
        event_signatures: &[B256],
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<ContractLog>, BlockchainError> {
        let topic_signatures: Vec<B256> = event_signatures.to_vec();

        let contracts = self.contracts().await;

        let address = contracts.get_address(contract_name)?;
        drop(contracts);

        let mut all_events = Vec::new();

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
                .map_err(BlockchainError::get_logs)?;

            for log in logs {
                if log.topic0().is_some() {
                    all_events.push(ContractLog::new(contract_name.clone(), log));
                }
            }

            block = to_block + 1;
        }

        Ok(all_events)
    }

    pub async fn get_sharding_table_head(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let contracts = self.contracts().await;
        let head: Uint<72, 2> = contracts.sharding_table_storage().head().call().await?;
        Ok(head.to::<u128>())
    }

    pub async fn get_sharding_table_length(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let contracts = self.contracts().await;
        let nodes_count: Uint<72, 2> = contracts
            .sharding_table_storage()
            .nodesCount()
            .call()
            .await?;
        Ok(nodes_count.to::<u128>())
    }

    pub async fn get_sharding_table_page(
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

    pub async fn get_identity_id(&self) -> Option<u128> {
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

    pub async fn identity_id_exists(&self) -> bool {
        let identity_id = self.get_identity_id().await;

        identity_id.is_some()
    }

    pub async fn create_profile(&self, peer_id: &str) -> Result<(), BlockchainError> {
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

        // Get gas price before sending transaction
        let gas_price = self.get_gas_price().await;

        // Profile ABI: createProfile(adminWallet, operationalWallets, nodeName, nodeId,
        // initialOperatorFee)
        let create_profile_call = contracts
            .profile()
            .createProfile(
                admin_wallet,
                vec![],
                shares_token_name.clone(), // nodeName
                peer_id_bytes,             // nodeId
                0u16,                      // initialOperatorFee
            )
            .gas_price(gas_price.to::<u128>());

        let result = create_profile_call.send().await;

        match result {
            Ok(pending_tx) => {
                handle_contract_call(Ok(pending_tx)).await?;
                tracing::info!(
                    "Profile created with token name: {}, token symbol: {}.",
                    shares_token_name,
                    shares_token_symbol
                );
                Ok(())
            }
            Err(err) => {
                // Check for "already exists" errors - treat as success (idempotent)
                if let Some(Profile::IdentityAlreadyExists { identityId, wallet }) =
                    err.as_decoded_error::<Profile::IdentityAlreadyExists>()
                {
                    tracing::info!(
                        "Profile already exists for identity {} (wallet {})",
                        identityId,
                        wallet
                    );
                    return Ok(());
                }

                if let Some(Profile::NodeIdAlreadyExists { nodeId }) =
                    err.as_decoded_error::<Profile::NodeIdAlreadyExists>()
                {
                    tracing::info!(
                        "Profile already exists for nodeId 0x{}",
                        hex::encode(&nodeId)
                    );
                    return Ok(());
                }

                if let Some(Profile::NodeNameAlreadyExists { nodeName }) =
                    err.as_decoded_error::<Profile::NodeNameAlreadyExists>()
                {
                    tracing::info!("Profile already exists for nodeName {}", nodeName);
                    return Ok(());
                }

                // Log detailed error for other cases
                tracing::error!("Profile creation failed: {:?}", err);
                Err(BlockchainError::ProfileCreation {
                    reason: format!("{:?}", err),
                })
            }
        }
    }

    pub async fn initialize_identity(&mut self, peer_id: &str) -> Result<(), BlockchainError> {
        if !self.identity_id_exists().await {
            self.create_profile(peer_id).await?;
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
    pub async fn set_stake(&self, stake_wei: u128) -> Result<(), BlockchainError> {
        use alloy::primitives::{U256, Uint};

        let config = self.config();

        // Get management wallet private key (required for staking)
        let management_pk = config
            .evm_management_wallet_private_key()
            .ok_or(BlockchainError::ManagementKeyRequired)?;

        // Create a provider with the management wallet for staking operations
        let management_signer: PrivateKeySigner =
            management_pk.parse().map_err(|e: LocalSignerError| {
                BlockchainError::InvalidPrivateKey {
                    key_length: management_pk.len(),
                    source: e,
                }
            })?;
        let management_wallet = EthereumWallet::from(management_signer);

        // Create provider with management wallet using same RPC endpoints (HTTP + WS fallback)
        let management_provider =
            initialize_provider_with_wallet(config.rpc_endpoints(), management_wallet).await?;

        // Get contract addresses from existing contracts
        let contracts = self.contracts().await;
        let staking_address = *contracts.staking().address();
        let token_address = *contracts.token().address();
        drop(contracts);

        // Create contracts with management wallet provider
        let staking = Staking::new(staking_address, &management_provider);
        let token = Token::new(token_address, &management_provider);

        // Get identity ID
        let identity_id = self
            .get_identity_id()
            .await
            .ok_or(BlockchainError::IdentityIdNotFound)?;

        // Get gas price before sending transactions
        let gas_price = self.get_gas_price().await;

        // Approve token spending
        let approve_call = token
            .increaseAllowance(staking_address, U256::from(stake_wei))
            .gas_price(gas_price.to::<u128>());
        match approve_call.send().await {
            Ok(pending_tx) => {
                handle_contract_call(Ok(pending_tx)).await?;
            }
            Err(err) => {
                tracing::error!("Token approval failed: {:?}", err);
                return Err(BlockchainError::TransactionFailed {
                    contract: "Token".to_string(),
                    function: "increaseAllowance".to_string(),
                    reason: format!("{:?}", err),
                });
            }
        }

        // Stake tokens - identity_id is uint72, stake_wei is uint96
        let stake_call = staking
            .stake(
                Uint::<72, 2>::from(identity_id),
                Uint::<96, 2>::from(stake_wei),
            )
            .gas_price(gas_price.to::<u128>());

        match stake_call.send().await {
            Ok(pending_tx) => {
                handle_contract_call(Ok(pending_tx)).await?;
                tracing::info!("Set stake completed for identity {}", identity_id);
                Ok(())
            }
            Err(err) => {
                tracing::error!("Staking failed: {:?}", err);
                Err(BlockchainError::TransactionFailed {
                    contract: "Staking".to_string(),
                    function: "stake".to_string(),
                    reason: format!("{:?}", err),
                })
            }
        }
    }

    /// Sets the ask price for this node's identity (dev environment only).
    pub async fn set_ask(&self, ask_wei: u128) -> Result<(), BlockchainError> {
        use alloy::primitives::Uint;

        let contracts = self.contracts().await;

        // Get identity ID
        let identity_id = self
            .get_identity_id()
            .await
            .ok_or(BlockchainError::IdentityIdNotFound)?;

        // Get gas price before sending transaction
        let gas_price = self.get_gas_price().await;

        // Update ask via Profile contract - identity_id is uint72, ask_wei is uint96
        let update_ask_call = contracts
            .profile()
            .updateAsk(
                Uint::<72, 2>::from(identity_id),
                Uint::<96, 2>::from(ask_wei),
            )
            .gas_price(gas_price.to::<u128>());

        match update_ask_call.send().await {
            Ok(pending_tx) => {
                handle_contract_call(Ok(pending_tx)).await?;
                tracing::info!("Set ask completed for identity {}", identity_id);
                Ok(())
            }
            Err(err) => {
                tracing::error!("Set ask failed: {:?}", err);
                Err(BlockchainError::TransactionFailed {
                    contract: "Profile".to_string(),
                    function: "updateAsk".to_string(),
                    reason: format!("{:?}", err),
                })
            }
        }
    }

    pub async fn sign_message(
        &self,
        message_hash: &str,
    ) -> Result<crate::SignatureComponents, BlockchainError> {
        use alloy::signers::Signer;

        // Decode the hex message hash
        let message_bytes =
            hex::decode(message_hash.strip_prefix("0x").unwrap_or(message_hash)).map_err(|e| {
                BlockchainError::HexDecode {
                    context: "message hash".to_string(),
                    source: e,
                }
            })?;

        // Re-create signer from config since we can't easily access it from the provider
        let config = self.config();
        let signer: PrivateKeySigner =
            config
                .evm_operational_wallet_private_key
                .parse()
                .map_err(|e: LocalSignerError| BlockchainError::InvalidPrivateKey {
                    key_length: config.evm_operational_wallet_private_key.len(),
                    source: e,
                })?;

        // Sign the message
        let signature = signer.sign_message(&message_bytes).await.map_err(|e| {
            BlockchainError::SigningFailed {
                reason: e.to_string(),
            }
        })?;

        // Extract r, s, v components directly from alloy's Signature type
        // v() returns bool (y_parity), convert to 27/28 format
        let v = if signature.v() { 28u8 } else { 27u8 };
        let r = format!("0x{}", hex::encode(signature.r().to_be_bytes::<32>()));
        let s_bytes = signature.s().to_be_bytes::<32>();
        let s = format!("0x{}", hex::encode(s_bytes));

        // Compute vs (compact signature format: s with the parity bit from v encoded in the high bit)
        let mut vs_bytes = s_bytes;
        if v == 28 {
            vs_bytes[0] |= 0x80;
        }
        let vs = format!("0x{}", hex::encode(vs_bytes));

        Ok(crate::SignatureComponents { v, r, s, vs })
    }
}
