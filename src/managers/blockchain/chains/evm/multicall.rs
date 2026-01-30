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

        let contracts = self.contracts().await;
        let chunks: Vec<_> = batch.into_chunks().collect();

        // Execute all chunks in parallel
        let futures: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                let multicall3 = contracts.multicall3();
                async move {
                    self.rpc_call(multicall3.aggregate3(chunk).call())
                        .await
                        .map_err(|e| {
                            BlockchainError::Custom(format!("Multicall3 aggregate3 failed: {}", e))
                        })
                }
            })
            .collect();

        let results = futures::future::try_join_all(futures).await?;

        // Flatten results preserving order
        let all_results: Vec<MulticallResult> = results
            .iter()
            .flat_map(|chunk_results| chunk_results.iter().map(MulticallResult::from_result))
            .collect();

        Ok(all_results)
    }
}
