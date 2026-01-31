use alloy::hex;

use crate::{
    managers::blockchain::{
        chains::evm::{
            EvmChain,
            contracts::{Staking, Token},
            initialize_provider_with_wallet,
        },
        error::BlockchainError,
        error_utils::handle_contract_call,
    },
    types::SignatureComponents,
};

impl EvmChain {
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
        let private_key = config.evm_operational_wallet_private_key();
        let signer = super::super::wallets::signer_from_private_key(private_key)?;

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

    /// Sets the stake for this node's identity (dev environment only).
    /// Requires management wallet private key to be configured.
    pub(crate) async fn set_stake(&self, stake_wei: u128) -> Result<(), BlockchainError> {
        use alloy::primitives::{U256, Uint};

        let config = self.config();

        // Get management wallet private key (required for staking)
        let management_pk = config
            .evm_management_wallet_private_key()
            .ok_or(BlockchainError::ManagementKeyRequired)?;

        // Create a provider with the management wallet for staking operations
        let management_wallet = super::super::wallets::wallet_from_private_key(management_pk)?;

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
        match self.tx_call(approve_call.send()).await {
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

        match self.tx_call(stake_call.send()).await {
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

        match self.tx_call(update_ask_call.send()).await {
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
}
