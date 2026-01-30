use super::super::*;

impl EvmChain {
    pub(crate) async fn get_identity_id(&self) -> Option<u128> {
        let evm_operational_address = self
            .config()
            .evm_operational_wallet_address
            .parse::<Address>();
        let Ok(evm_operational_address) = evm_operational_address else {
            return None;
        };

        let contracts = self.contracts().await;

        let result = self
            .rpc_call(
                contracts
                    .identity_storage()
                    .getIdentityId(evm_operational_address)
                    .call(),
            )
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
}
