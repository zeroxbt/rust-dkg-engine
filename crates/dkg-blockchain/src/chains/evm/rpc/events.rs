use alloy::{
    primitives::{Address, B256},
    providers::Provider,
    rpc::types::Filter,
};
use std::time::Instant;

use crate::{
    ContractLog, ContractName,
    chains::evm::{EvmChain, MAXIMUM_NUMBERS_OF_BLOCKS_TO_FETCH},
    error::BlockchainError,
};

impl EvmChain {
    pub async fn get_block_number(&self) -> Result<u64, BlockchainError> {
        self.rpc_call(|| async {
            let provider = self.provider().await;
            provider.get_block_number().await
        })
        .await
        .map_err(BlockchainError::get_block_number)
    }

    /// Get the sender address of a transaction by its hash.
    pub async fn get_transaction_sender(
        &self,
        tx_hash: B256,
    ) -> Result<Option<Address>, BlockchainError> {
        let tx = self
            .rpc_call(|| async {
                let provider = self.provider().await;
                provider.get_transaction_by_hash(tx_hash).await
            })
            .await
            .map_err(|e| BlockchainError::Custom(format!("Failed to get transaction: {}", e)))?;

        Ok(tx.map(|t| t.inner.signer()))
    }

    pub async fn get_event_logs(
        &self,
        contract_name: &ContractName,
        event_signatures: &[B256],
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<ContractLog>, BlockchainError> {
        let contracts = self.contracts().await;
        let address = contracts.get_address(contract_name)?;
        drop(contracts);

        self.get_event_logs_for_address(
            contract_name.clone(),
            address,
            event_signatures,
            from_block,
            current_block,
        )
        .await
    }

    /// Get event logs for a specific contract address.
    /// Use this for contracts that may have multiple addresses (e.g., KnowledgeCollectionStorage).
    pub async fn get_event_logs_for_address(
        &self,
        contract_name: ContractName,
        contract_address: Address,
        event_signatures: &[B256],
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<ContractLog>, BlockchainError> {
        let topic_signatures: Vec<B256> = event_signatures.to_vec();
        let mut all_events = Vec::new();
        let blockchain_id = self.blockchain_id().as_str().to_string();

        let mut block = from_block;
        while block <= current_block {
            let to_block = std::cmp::min(
                block + MAXIMUM_NUMBERS_OF_BLOCKS_TO_FETCH - 1,
                current_block,
            );
            let block_span = (to_block - block + 1) as usize;

            let mut filter = Filter::new()
                .address(contract_address)
                .from_block(block)
                .to_block(to_block);
            if !topic_signatures.is_empty() {
                filter = filter.event_signature(topic_signatures.clone());
            }

            let batch_started = Instant::now();
            let logs = match self
                .rpc_call(|| async {
                    let provider = self.provider().await;
                    provider.get_logs(&filter).await
                })
                .await
            {
                Ok(logs) => {
                    dkg_observability::record_blockchain_event_logs_batch(
                        &blockchain_id,
                        contract_name.as_str(),
                        "ok",
                        batch_started.elapsed(),
                        block_span,
                        logs.len(),
                    );
                    logs
                }
                Err(err) => {
                    dkg_observability::record_blockchain_event_logs_batch(
                        &blockchain_id,
                        contract_name.as_str(),
                        "error",
                        batch_started.elapsed(),
                        block_span,
                        0,
                    );
                    return Err(BlockchainError::get_logs(err));
                }
            };

            for log in logs {
                if log.topic0().is_some() {
                    all_events.push(ContractLog::new(contract_name.clone(), log));
                }
            }

            block = to_block + 1;
        }

        Ok(all_events)
    }
}
