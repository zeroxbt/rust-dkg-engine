use alloy::primitives::{Address, B256, FixedBytes};
use dkg_domain::BlockchainId;

use crate::managers::{
    BlockchainManager,
    blockchain::{ContractLog, ContractName, error::BlockchainError},
};

impl BlockchainManager {
    pub(crate) async fn get_event_logs(
        &self,
        blockchain: &BlockchainId,
        contract_name: &ContractName,
        event_signatures: &[FixedBytes<32>],
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<ContractLog>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .get_event_logs(contract_name, event_signatures, from_block, current_block)
            .await
    }

    pub(crate) async fn get_block_number(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u64, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_block_number().await
    }

    /// Get the sender address of a transaction by its hash.
    pub(crate) async fn get_transaction_sender(
        &self,
        blockchain: &BlockchainId,
        tx_hash: FixedBytes<32>,
    ) -> Result<Option<Address>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_transaction_sender(tx_hash).await
    }

    /// Get event logs for a specific contract address.
    pub(crate) async fn get_event_logs_for_address(
        &self,
        blockchain: &BlockchainId,
        contract_name: ContractName,
        contract_address: Address,
        event_signatures: &[B256],
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<ContractLog>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .get_event_logs_for_address(
                contract_name,
                contract_address,
                event_signatures,
                from_block,
                current_block,
            )
            .await
    }
}
