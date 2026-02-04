use alloy::primitives::{Address, U256, Uint};

use crate::managers::blockchain::{chains::evm::EvmChain, error::BlockchainError};

impl EvmChain {
    pub(crate) async fn get_delegators(
        &self,
        identity_id: u128,
    ) -> Result<Vec<Address>, BlockchainError> {
        let delegators = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts
                    .delegators_info()
                    .getDelegators(Uint::<72, 2>::from(identity_id))
                    .call()
                    .await
            })
            .await?;

        Ok(delegators)
    }

    pub(crate) async fn has_ever_delegated_to_node(
        &self,
        identity_id: u128,
        delegator: Address,
    ) -> Result<bool, BlockchainError> {
        let has_ever_delegated = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts
                    .delegators_info()
                    .hasEverDelegatedToNode(Uint::<72, 2>::from(identity_id), delegator)
                    .call()
                    .await
            })
            .await?;

        Ok(has_ever_delegated)
    }

    pub(crate) async fn get_last_claimed_epoch(
        &self,
        identity_id: u128,
        delegator: Address,
    ) -> Result<u64, BlockchainError> {
        let epoch = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts
                    .delegators_info()
                    .getLastClaimedEpoch(Uint::<72, 2>::from(identity_id), delegator)
                    .call()
                    .await
            })
            .await?;

        Ok(epoch.to::<u64>())
    }

    pub(crate) async fn batch_claim_delegator_rewards(
        &self,
        identity_id: u128,
        epochs: &[u64],
        delegators: &[Address],
    ) -> Result<(), BlockchainError> {
        let epoch_args: Vec<U256> = epochs.iter().map(|e| U256::from(*e)).collect();
        let delegator_args: Vec<Address> = delegators.to_vec();

        match self
            .tx_call(|contracts| {
                contracts.staking().batchClaimDelegatorRewards(
                    Uint::<72, 2>::from(identity_id),
                    epoch_args.clone(),
                    delegator_args.clone(),
                )
            })
            .await
        {
            Ok(pending_tx) => {
                self.handle_contract_call(Ok(pending_tx)).await?;
                Ok(())
            }
            Err(err) => Err(BlockchainError::TransactionFailed {
                contract: "Staking".to_string(),
                function: "batchClaimDelegatorRewards".to_string(),
                reason: format!("{:?}", err),
            }),
        }
    }
}
