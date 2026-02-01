use alloy::{
    primitives::{Bytes, TxKind},
    providers::Provider,
    rpc::types::transaction::{TransactionInput, TransactionRequest},
};
use futures::future::join_all;

use super::*;

impl EvmChain {
    /// Execute a batch of heterogeneous calls using Multicall3.
    ///
    /// This method handles automatic chunking to avoid RPC limits, executing
    /// multiple Multicall3 requests if necessary and combining the results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use crate::managers::blockchain::multicall::{MulticallBatch, MulticallRequest, encoders};
    ///
    /// let mut batch = MulticallBatch::new();
    /// batch.add(MulticallRequest::new(contract, encoders::encode_get_merkle_root(1)));
    /// batch.add(MulticallRequest::new(contract, encoders::encode_get_end_epoch(1)));
    ///
    /// let results = evm_chain.execute_multicall(batch).await?;
    /// let merkle_root = results[0].as_bytes32_hex();
    /// let end_epoch = results[1].as_u64();
    /// ```
    pub(crate) async fn execute_multicall(
        &self,
        batch: crate::managers::blockchain::multicall::MulticallBatch,
    ) -> Result<Vec<crate::managers::blockchain::multicall::MulticallResult>, BlockchainError> {
        use crate::managers::blockchain::multicall::MulticallResult;

        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let chunks: Vec<_> = batch.into_chunks().collect();

        let mut all_results = Vec::new();
        let mut warned_fallback = false;

        for chunk in chunks {
            let contracts = self.contracts().await;
            let multicall3 = contracts.multicall3();
            let multicall_result = self
                .rpc_call(multicall3.aggregate3(chunk.clone()).call())
                .await;

            match multicall_result {
                Ok(chunk_results) => {
                    all_results.extend(chunk_results.iter().map(MulticallResult::from_result));
                }
                Err(e) => {
                    if !warned_fallback {
                        tracing::warn!(
                            blockchain = %self.blockchain_id(),
                            error = %e,
                            "Multicall failed; falling back to single calls"
                        );
                        warned_fallback = true;
                    }
                    let fallback_results = self.execute_multicall_chunk_fallback(&chunk).await;
                    all_results.extend(fallback_results);
                }
            }
        }

        Ok(all_results)
    }

    async fn execute_multicall_chunk_fallback(
        &self,
        chunk: &[Multicall3::Call3],
    ) -> Vec<crate::managers::blockchain::multicall::MulticallResult> {
        use crate::managers::blockchain::multicall::MulticallResult;

        let calls = chunk.iter().map(|call| async move {
            let tx = TransactionRequest {
                to: Some(TxKind::Call(call.target)),
                input: TransactionInput::new(call.callData.clone()),
                ..TransactionRequest::default()
            };

            match self.rpc_call(self.provider().call(tx)).await {
                Ok(bytes) => MulticallResult {
                    success: true,
                    return_data: bytes,
                },
                Err(e) => {
                    tracing::debug!(
                        target = %call.target,
                        error = %e,
                        "Multicall fallback call failed"
                    );
                    MulticallResult {
                        success: false,
                        return_data: Bytes::new(),
                    }
                }
            }
        });

        join_all(calls).await
    }
}
