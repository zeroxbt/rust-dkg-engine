use std::str::FromStr;

use alloy::{
    primitives::{Address, B256, Bytes, U256, hex},
    providers::Provider,
    rpc::types::Filter,
    signers::local::{LocalSignerError, PrivateKeySigner},
};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::managers::blockchain::{
    AccessPolicy, BlockchainConfig, BlockchainId, GasConfig, PermissionedNode, SignatureComponents,
    blockchains::blockchain_creator::{
        BlockchainProvider, Contracts, Profile, initialize_contracts, initialize_provider,
        sharding_table::ShardingTableLib::NodeInfo,
    },
    error::BlockchainError,
    error_utils::handle_contract_call,
    gas::fetch_gas_price_from_oracle,
    substrate::validate_evm_wallets,
};

const MAXIMUM_NUMBERS_OF_BLOCKS_TO_FETCH: u64 = 50;

/// Format a balance in wei to a human-readable string with the specified decimals.
pub(crate) fn format_balance(wei: U256, decimals: u8) -> String {
    let divisor = U256::from(10u64).pow(U256::from(decimals));

    if wei.is_zero() {
        return "0".to_string();
    }

    let whole = wei / divisor;
    let fraction = wei % divisor;

    if fraction.is_zero() {
        whole.to_string()
    } else {
        let fraction_str = format!("{:0>width$}", fraction, width = decimals as usize);
        let trimmed = fraction_str.trim_end_matches('0');
        format!("{}.{}", whole, trimmed)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ContractName {
    Hub,
    ShardingTable,
    ShardingTableStorage,
    Staking,
    Profile,
    ParametersStorage,
    KnowledgeCollectionStorage,
    ParanetsRegistry,
}

impl ContractName {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            ContractName::Hub => "Hub",
            ContractName::ShardingTable => "ShardingTable",
            ContractName::ShardingTableStorage => "ShardingTableStorage",
            ContractName::Staking => "Staking",
            ContractName::Profile => "Profile",
            ContractName::ParametersStorage => "ParametersStorage",
            ContractName::KnowledgeCollectionStorage => "KnowledgeCollectionStorage",
            ContractName::ParanetsRegistry => "ParanetsRegistry",
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
            "ParanetsRegistry" => Ok(ContractName::ParanetsRegistry),
            _ => Err(format!("'{}' is not a valid contract name", s)),
        }
    }
}

pub(crate) struct ContractLog {
    contract_name: ContractName,
    log: alloy::rpc::types::Log,
}

impl ContractLog {
    pub(crate) fn new(contract_name: ContractName, log: alloy::rpc::types::Log) -> Self {
        Self { contract_name, log }
    }

    pub(crate) fn contract_name(&self) -> &ContractName {
        &self.contract_name
    }

    pub(crate) fn log(&self) -> &alloy::rpc::types::Log {
        &self.log
    }
}

pub(crate) struct EvmChain {
    config: BlockchainConfig,
    provider: BlockchainProvider,
    contracts: RwLock<Contracts>,
    identity_id: Option<u128>,
    gas_config: GasConfig,
    native_token_decimals: u8,
    native_token_ticker: &'static str,
    is_development_chain: bool,
    requires_evm_account_mapping: bool,
}

impl EvmChain {
    pub(crate) async fn new(
        config: BlockchainConfig,
        gas_config: GasConfig,
        native_token_decimals: u8,
        native_token_ticker: &'static str,
        is_development_chain: bool,
        requires_evm_account_mapping: bool,
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
            "Initialized {} blockchain with gas config: default={} wei, max={} wei, native token: {} ({} decimals)",
            config.blockchain_id(),
            gas_config.default_gas_price,
            gas_config.max_gas_price,
            native_token_ticker,
            native_token_decimals
        );

        // Validate EVM account mappings for chains that require it (NeuroWeb)
        if requires_evm_account_mapping {
            // Get Substrate RPC endpoints - can use same endpoints as EVM or dedicated ones
            let substrate_endpoints = config
                .substrate_rpc_endpoints()
                .cloned()
                .unwrap_or_else(|| config.rpc_endpoints().clone());

            if substrate_endpoints.is_empty() {
                tracing::warn!(
                    "{}: No Substrate RPC endpoints configured for EVM account mapping validation. \
                     Skipping validation - ensure wallets are properly mapped.",
                    config.blockchain_id()
                );
            } else {
                tracing::info!(
                    "{}: Validating EVM account mappings via Substrate RPC...",
                    config.blockchain_id()
                );
                validate_evm_wallets(
                    &substrate_endpoints,
                    config.evm_management_wallet_address(),
                    config.evm_operational_wallet_address(),
                )
                .await?;
            }
        }

        Ok(Self {
            provider,
            config,
            contracts: RwLock::new(contracts),
            identity_id: None,
            gas_config,
            native_token_decimals,
            native_token_ticker,
            is_development_chain,
            requires_evm_account_mapping,
        })
    }

    pub(crate) fn blockchain_id(&self) -> &BlockchainId {
        self.config.blockchain_id()
    }

    pub(crate) fn config(&self) -> &BlockchainConfig {
        &self.config
    }

    pub(crate) fn provider(&self) -> &BlockchainProvider {
        &self.provider
    }

    pub(crate) async fn contracts(&self) -> RwLockReadGuard<'_, Contracts> {
        self.contracts.read().await
    }

    pub(crate) async fn contracts_mut(&self) -> RwLockWriteGuard<'_, Contracts> {
        self.contracts.write().await
    }

    pub(crate) fn set_identity_id(&mut self, id: u128) {
        self.identity_id = Some(id);
    }

    pub(crate) fn gas_config(&self) -> &GasConfig {
        &self.gas_config
    }

    /// Returns the number of decimal places for the native token.
    pub(crate) fn native_token_decimals(&self) -> u8 {
        self.native_token_decimals
    }

    /// Returns the native token ticker symbol.
    pub(crate) fn native_token_ticker(&self) -> &'static str {
        self.native_token_ticker
    }

    /// Returns true if this is a development/test chain.
    pub(crate) fn is_development_chain(&self) -> bool {
        self.is_development_chain
    }

    /// Returns true if this chain requires EVM account mapping validation.
    pub(crate) fn requires_evm_account_mapping(&self) -> bool {
        self.requires_evm_account_mapping
    }

    /// Get the native token balance formatted with proper decimals.
    ///
    /// NeuroWeb uses 12 decimals while most other chains use 18.
    pub(crate) async fn get_native_token_balance(
        &self,
        address: Address,
    ) -> Result<String, BlockchainError> {
        let balance = self
            .provider
            .get_balance(address)
            .await
            .map_err(|e| BlockchainError::Custom(format!("Failed to get balance: {}", e)))?;

        Ok(format_balance(balance, self.native_token_decimals))
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
    pub(crate) async fn get_gas_price(&self) -> U256 {
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

    pub(crate) async fn re_initialize_contract(
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

    pub(crate) async fn get_block_number(&self) -> Result<u64, BlockchainError> {
        self.provider()
            .get_block_number()
            .await
            .map_err(BlockchainError::get_block_number)
    }

    /// Get the sender address of a transaction by its hash.
    pub(crate) async fn get_transaction_sender(
        &self,
        tx_hash: B256,
    ) -> Result<Option<Address>, BlockchainError> {
        let tx = self
            .provider()
            .get_transaction_by_hash(tx_hash)
            .await
            .map_err(|e| BlockchainError::Custom(format!("Failed to get transaction: {}", e)))?;

        Ok(tx.map(|t| t.inner.signer()))
    }

    pub(crate) async fn get_event_logs(
        &self,
        contract_name: &ContractName,
        event_signatures: &[B256],
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<ContractLog>, BlockchainError> {
        let contracts = self.contracts().await;
        let address = contracts.get_address(contract_name)?;
        drop(contracts);

        self.get_event_logs_for_address(contract_name.clone(), address, event_signatures, from_block, current_block).await
    }

    /// Get event logs for a specific contract address.
    /// Use this for contracts that may have multiple addresses (e.g., KnowledgeCollectionStorage).
    pub(crate) async fn get_event_logs_for_address(
        &self,
        contract_name: ContractName,
        contract_address: Address,
        event_signatures: &[B256],
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<ContractLog>, BlockchainError> {
        let topic_signatures: Vec<B256> = event_signatures.to_vec();
        let mut all_events = Vec::new();

        let mut block = from_block;
        while block <= current_block {
            let to_block = std::cmp::min(
                block + MAXIMUM_NUMBERS_OF_BLOCKS_TO_FETCH - 1,
                current_block,
            );

            let mut filter = Filter::new()
                .address(contract_address)
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

    /// Get all contract addresses for a contract type.
    pub(crate) async fn get_all_contract_addresses(
        &self,
        contract_name: &ContractName,
    ) -> Vec<Address> {
        let contracts = self.contracts().await;
        contracts.get_all_addresses(contract_name)
    }

    pub(crate) async fn get_sharding_table_head(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let contracts = self.contracts().await;
        let head: Uint<72, 2> = contracts.sharding_table_storage().head().call().await?;
        Ok(head.to::<u128>())
    }

    pub(crate) async fn get_minimum_required_signatures(&self) -> Result<u64, BlockchainError> {
        use alloy::primitives::U256;
        let contracts = self.contracts().await;
        let min_signatures: U256 = contracts
            .parameters_storage()
            .minimumRequiredSignatures()
            .call()
            .await?;
        Ok(min_signatures.to::<u64>())
    }

    pub(crate) async fn get_sharding_table_length(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let contracts = self.contracts().await;
        let nodes_count: Uint<72, 2> = contracts
            .sharding_table_storage()
            .nodesCount()
            .call()
            .await?;
        Ok(nodes_count.to::<u128>())
    }

    pub(crate) async fn get_sharding_table_page(
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

    pub(crate) async fn get_identity_id(&self) -> Option<u128> {
        let evm_operational_address = self
            .config()
            .evm_operational_wallet_address
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

    pub(crate) async fn identity_id_exists(&self) -> bool {
        let identity_id = self.get_identity_id().await;

        identity_id.is_some()
    }

    pub(crate) async fn create_profile(&self, peer_id: &str) -> Result<(), BlockchainError> {
        let config = self.config();

        let node_name = config.node_name();
        if node_name.is_empty() {
            return Err(BlockchainError::Custom(
                "Missing node_name in blockchain configuration".to_string(),
            ));
        }

        let admin_wallet = config
            .evm_management_wallet_address()
            .parse::<Address>()
            .map_err(|_| BlockchainError::InvalidAddress {
                address: config.evm_management_wallet_address().to_string(),
            })?;
        let peer_id_bytes = Bytes::from(peer_id.as_bytes().to_vec());
        let operator_fee = config.operator_fee().unwrap_or(0);

        let contracts = self.contracts().await;

        // Get gas price before sending transaction
        let gas_price = self.get_gas_price().await;

        // Profile ABI: createProfile(adminWallet, operationalWallets, nodeName, nodeId,
        // initialOperatorFee)
        let create_profile_call = contracts
            .profile()
            .createProfile(
                admin_wallet,
                vec![], // additional operational wallets (we only support single wallet)
                node_name.into(), // nodeName
                peer_id_bytes, // nodeId (peer ID as bytes)
                operator_fee as u16,
            )
            .gas_price(gas_price.to::<u128>());

        let result = create_profile_call.send().await;

        match result {
            Ok(pending_tx) => {
                handle_contract_call(Ok(pending_tx)).await?;
                tracing::info!(
                    "Profile created with name: {}, operator fee: {}%",
                    node_name,
                    operator_fee
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

    pub(crate) async fn initialize_identity(
        &mut self,
        peer_id: &str,
    ) -> Result<(), BlockchainError> {
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
    #[cfg(feature = "dev-tools")]
    pub(crate) async fn set_stake(&self, stake_wei: u128) -> Result<(), BlockchainError> {
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
    #[cfg(feature = "dev-tools")]
    pub(crate) async fn set_ask(&self, ask_wei: u128) -> Result<(), BlockchainError> {
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

    pub(crate) async fn sign_message(
        &self,
        message_hash: &str,
    ) -> Result<SignatureComponents, BlockchainError> {
        use alloy::signers::Signer;

        // Decode the hex message hash
        let message_bytes = hex::decode(message_hash.strip_prefix("0x").unwrap_or(message_hash))
            .map_err(|e| BlockchainError::HexDecode {
                context: "message hash".to_string(),
                source: e,
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

        // Compute vs (compact signature format: s with the parity bit from v encoded in the high
        // bit)
        let mut vs_bytes = s_bytes;
        if v == 28 {
            vs_bytes[0] |= 0x80;
        }
        let vs = format!("0x{}", hex::encode(vs_bytes));

        Ok(SignatureComponents { v, r, s, vs })
    }

    /// Check if a knowledge collection exists on-chain.
    ///
    /// Validates that the knowledge collection ID exists by checking if it has a publisher.
    /// Returns the publisher address if the collection exists, or None if it doesn't.
    ///
    /// Returns an error if the contract address is not a registered KnowledgeCollectionStorage.
    /// The node only works with contracts discovered at initialization or via NewAssetStorage events.
    pub(crate) async fn get_knowledge_collection_publisher(
        &self,
        contract_address: Address,
        knowledge_collection_id: u128,
    ) -> Result<Option<Address>, BlockchainError> {
        let contracts = self.contracts().await;
        let kc_storage = contracts
            .knowledge_collection_storage_by_address(&contract_address)
            .ok_or_else(|| {
                BlockchainError::Custom(format!(
                    "KnowledgeCollectionStorage at {:?} is not registered. \
                     The node only knows about contracts from initialization or NewAssetStorage events.",
                    contract_address
                ))
            })?;

        let publisher = kc_storage
            .getLatestMerkleRootPublisher(U256::from(knowledge_collection_id))
            .call()
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to get knowledge collection publisher from {:?}: {}",
                    contract_address, e
                ))
            })?;

        // If publisher is zero address, the collection doesn't exist
        if publisher.is_zero() {
            Ok(None)
        } else {
            Ok(Some(publisher))
        }
    }

    /// Get the range of knowledge assets (token IDs) for a knowledge collection.
    ///
    /// Returns (start_token_id, end_token_id, burned_token_ids) or None if the collection
    /// doesn't exist.
    ///
    /// Returns an error if the contract address is not a registered KnowledgeCollectionStorage.
    pub(crate) async fn get_knowledge_assets_range(
        &self,
        contract_address: Address,
        knowledge_collection_id: u128,
    ) -> Result<Option<(u64, u64, Vec<u64>)>, BlockchainError> {
        let contracts = self.contracts().await;
        let kc_storage = contracts
            .knowledge_collection_storage_by_address(&contract_address)
            .ok_or_else(|| {
                BlockchainError::Custom(format!(
                    "KnowledgeCollectionStorage at {:?} is not registered. \
                     The node only knows about contracts from initialization or NewAssetStorage events.",
                    contract_address
                ))
            })?;

        let result = kc_storage
            .getKnowledgeAssetsRange(U256::from(knowledge_collection_id))
            .call()
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to get knowledge assets range from {:?}: {}",
                    contract_address, e
                ))
            })?;

        let start_token_id = result
            ._0
            .try_into()
            .map_err(|_| BlockchainError::Custom("Start token ID overflow".to_string()))?;
        let end_token_id = result
            ._1
            .try_into()
            .map_err(|_| BlockchainError::Custom("End token ID overflow".to_string()))?;
        let burned: Vec<u64> = result
            ._2
            .into_iter()
            .filter_map(|v| v.try_into().ok())
            .collect();

        // If both start and end are 0, the collection doesn't exist or is empty
        if start_token_id == 0 && end_token_id == 0 {
            Ok(None)
        } else {
            Ok(Some((start_token_id, end_token_id, burned)))
        }
    }

    /// Get the latest merkle root for a knowledge collection.
    ///
    /// Returns the merkle root as a hex string (with 0x prefix), or None if the collection
    /// doesn't exist.
    ///
    /// Returns an error if the contract address is not a registered KnowledgeCollectionStorage.
    pub(crate) async fn get_knowledge_collection_merkle_root(
        &self,
        contract_address: Address,
        knowledge_collection_id: u128,
    ) -> Result<Option<String>, BlockchainError> {
        let contracts = self.contracts().await;
        let kc_storage = contracts
            .knowledge_collection_storage_by_address(&contract_address)
            .ok_or_else(|| {
                BlockchainError::Custom(format!(
                    "KnowledgeCollectionStorage at {:?} is not registered. \
                     The node only knows about contracts from initialization or NewAssetStorage events.",
                    contract_address
                ))
            })?;

        let merkle_root = kc_storage
            .getLatestMerkleRoot(U256::from(knowledge_collection_id))
            .call()
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to get knowledge collection merkle root from {:?}: {}",
                    contract_address, e
                ))
            })?;

        // If merkle root is zero, the collection doesn't exist
        if merkle_root.is_zero() {
            Ok(None)
        } else {
            Ok(Some(format!(
                "0x{}",
                crate::managers::blockchain::utils::to_hex_string(merkle_root)
            )))
        }
    }

    /// Get the latest knowledge collection ID for a contract.
    ///
    /// Returns the highest KC ID that has been created on this contract.
    /// Returns 0 if no collections have been created yet.
    pub(crate) async fn get_latest_knowledge_collection_id(
        &self,
        contract_address: Address,
    ) -> Result<u64, BlockchainError> {
        let contracts = self.contracts().await;
        let kc_storage = contracts
            .knowledge_collection_storage_by_address(&contract_address)
            .ok_or_else(|| {
                BlockchainError::Custom(format!(
                    "KnowledgeCollectionStorage at {:?} is not registered. \
                     The node only knows about contracts from initialization or NewAssetStorage events.",
                    contract_address
                ))
            })?;

        let latest_id = kc_storage
            .getLatestKnowledgeCollectionId()
            .call()
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to get latest knowledge collection ID from {:?}: {}",
                    contract_address, e
                ))
            })?;

        latest_id.try_into().map_err(|_| {
            BlockchainError::Custom("Latest knowledge collection ID overflow".to_string())
        })
    }

    /// Get the current epoch from the Chronos contract.
    pub(crate) async fn get_current_epoch(&self) -> Result<u64, BlockchainError> {
        let contracts = self.contracts().await;
        let chronos = contracts.chronos();

        let current_epoch = chronos.getCurrentEpoch().call().await.map_err(|e| {
            BlockchainError::Custom(format!("Failed to get current epoch: {}", e))
        })?;

        current_epoch.try_into().map_err(|_| {
            BlockchainError::Custom("Current epoch overflow".to_string())
        })
    }

    /// Get the end epoch (expiration) for a knowledge collection.
    ///
    /// Returns None if the KC doesn't exist or hasn't been created yet.
    pub(crate) async fn get_kc_end_epoch(
        &self,
        contract_address: Address,
        knowledge_collection_id: u128,
    ) -> Result<u64, BlockchainError> {
        let contracts = self.contracts().await;
        let kc_storage = contracts
            .knowledge_collection_storage_by_address(&contract_address)
            .ok_or_else(|| {
                BlockchainError::Custom(format!(
                    "KnowledgeCollectionStorage at {:?} is not registered.",
                    contract_address
                ))
            })?;

        let end_epoch = kc_storage
            .getEndEpoch(U256::from(knowledge_collection_id))
            .call()
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to get KC end epoch from {:?}: {}",
                    contract_address, e
                ))
            })?;

        // end_epoch is Uint<40, 1> - get the inner value and convert
        // The Uint type has a limbs() method or we can use to_limbs()
        Ok(end_epoch.to::<u64>())
    }

    // ==================== Paranet Methods ====================

    /// Check if a paranet exists on-chain.
    pub(crate) async fn paranet_exists(&self, paranet_id: B256) -> Result<bool, BlockchainError> {
        let contracts = self.contracts().await;
        let registry = contracts.paranets_registry()?;

        // Single return value - result IS the bool directly
        let exists = registry
            .paranetExists(paranet_id)
            .call()
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!("Failed to check paranet exists: {}", e))
            })?;

        Ok(exists)
    }

    /// Get the nodes access policy for a paranet.
    pub(crate) async fn get_nodes_access_policy(
        &self,
        paranet_id: B256,
    ) -> Result<AccessPolicy, BlockchainError> {
        let contracts = self.contracts().await;
        let registry = contracts.paranets_registry()?;

        // Single return value - result IS the u8 directly
        let policy = registry
            .getNodesAccessPolicy(paranet_id)
            .call()
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!("Failed to get nodes access policy: {}", e))
            })?;

        Ok(AccessPolicy::from(policy))
    }

    /// Get the list of permissioned nodes for a paranet.
    pub(crate) async fn get_permissioned_nodes(
        &self,
        paranet_id: B256,
    ) -> Result<Vec<PermissionedNode>, BlockchainError> {
        let contracts = self.contracts().await;
        let registry = contracts.paranets_registry()?;

        // Single return value - result IS the Vec directly
        let nodes = registry
            .getPermissionedNodes(paranet_id)
            .call()
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!("Failed to get permissioned nodes: {}", e))
            })?;

        Ok(nodes
            .into_iter()
            .map(|node| PermissionedNode {
                identity_id: node.identityId.to::<u128>(),
                node_id: node.nodeId.to_vec(),
            })
            .collect())
    }

    /// Check if a knowledge collection is registered in a paranet.
    pub(crate) async fn is_knowledge_collection_registered(
        &self,
        paranet_id: B256,
        knowledge_collection_id: B256,
    ) -> Result<bool, BlockchainError> {
        let contracts = self.contracts().await;
        let registry = contracts.paranets_registry()?;

        // Single return value - result IS the bool directly
        let registered = registry
            .isKnowledgeCollectionRegistered(paranet_id, knowledge_collection_id)
            .call()
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to check knowledge collection registration: {}",
                    e
                ))
            })?;

        Ok(registered)
    }
}
