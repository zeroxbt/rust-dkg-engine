use std::time::Instant;

use alloy::{
    primitives::{Address, B256},
    providers::Provider,
    rpc::types::Filter,
    transports::{RpcError, TransportErrorKind},
};

use crate::{ContractLog, ContractName, chains::evm::EvmChain, error::BlockchainError};

const EVENT_LOGS_MIN_SPAN: u64 = 1;
const EVENT_LOGS_INITIAL_SPAN: u64 = 512;

fn should_split_get_logs_error(err: &RpcError<TransportErrorKind>) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("timeout")
        || message.contains("query timeout")
        || message.contains("deadline")
        || message.contains("request timed out")
}

fn event_logs_span_cache_key(contract_address: Address, topic_signatures: &[B256]) -> String {
    let mut signatures: Vec<String> = topic_signatures
        .iter()
        .map(|sig| format!("{sig:#x}"))
        .collect();
    signatures.sort_unstable();
    signatures.dedup();
    format!("{contract_address:#x}|{}", signatures.join(","))
}

fn grow_span(current: u64, max_span: u64, had_logs: bool) -> u64 {
    let grown = if had_logs {
        current.saturating_add(current / 4).saturating_add(1)
    } else {
        current.saturating_add(current / 10).saturating_add(1)
    };
    grown.clamp(EVENT_LOGS_MIN_SPAN, max_span.max(1))
}

impl EvmChain {
    async fn get_cached_event_logs_span(&self, key: &str, default_span: u64) -> u64 {
        let cache = self.event_logs_span_cache.lock().await;
        cache.get(key).copied().unwrap_or(default_span)
    }

    async fn observe_failed_event_logs_span(&self, key: &str, failed_span: u64) {
        let mut cache = self.event_logs_span_cache.lock().await;
        let entry = cache.entry(key.to_string()).or_insert(failed_span);
        *entry = (*entry).min(failed_span.max(EVENT_LOGS_MIN_SPAN));
    }

    async fn observe_successful_event_logs_span(&self, key: &str, successful_span: u64) {
        let mut cache = self.event_logs_span_cache.lock().await;
        let entry = cache.entry(key.to_string()).or_insert(successful_span);
        // Smooth updates so occasional outliers do not cause abrupt span swings.
        *entry = (entry
            .saturating_mul(3)
            .saturating_add(successful_span)
            .saturating_add(3))
            / 4;
        *entry = (*entry).max(EVENT_LOGS_MIN_SPAN);
    }

    async fn fetch_logs_batch(
        &self,
        blockchain_id: &str,
        contract_name: &ContractName,
        contract_address: Address,
        topic_signatures: &[B256],
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<alloy::rpc::types::Log>, RpcError<TransportErrorKind>> {
        let block_span = (to_block.saturating_sub(from_block).saturating_add(1)) as usize;
        let mut filter = Filter::new()
            .address(contract_address)
            .from_block(from_block)
            .to_block(to_block);
        if !topic_signatures.is_empty() {
            filter = filter.event_signature(topic_signatures.to_vec());
        }

        let batch_started = Instant::now();
        match self
            .rpc_call(|| async {
                let provider = self.provider().await;
                provider.get_logs(&filter).await
            })
            .await
        {
            Ok(logs) => {
                dkg_observability::record_blockchain_event_logs_batch(
                    blockchain_id,
                    contract_name.as_str(),
                    "ok",
                    batch_started.elapsed(),
                    block_span,
                    logs.len(),
                );
                Ok(logs)
            }
            Err(err) => {
                dkg_observability::record_blockchain_event_logs_batch(
                    blockchain_id,
                    contract_name.as_str(),
                    "error",
                    batch_started.elapsed(),
                    block_span,
                    0,
                );
                Err(err)
            }
        }
    }

    pub async fn get_block_number(&self) -> Result<u64, BlockchainError> {
        self.rpc_call(|| async {
            let provider = self.provider().await;
            provider.get_block_number().await
        })
        .await
        .map_err(BlockchainError::get_block_number)
    }

    pub async fn get_block_timestamp(
        &self,
        block_number: u64,
    ) -> Result<Option<u64>, BlockchainError> {
        let block: Option<serde_json::Value> = self
            .rpc_call(|| async {
                let provider = self.provider().await;
                provider
                    .raw_request(
                        "eth_getBlockByNumber".into(),
                        (format!("0x{block_number:x}"), false),
                    )
                    .await
            })
            .await
            .map_err(|error| BlockchainError::BlockData {
                block_number,
                reason: error.to_string(),
            })?;

        let Some(block) = block else {
            return Ok(None);
        };

        let timestamp_value = block
            .get("timestamp")
            .ok_or_else(|| BlockchainError::BlockData {
                block_number,
                reason: "missing `timestamp` field".to_string(),
            })?;

        if let Some(ts) = timestamp_value.as_u64() {
            return Ok(Some(ts));
        }

        if let Some(ts_str) = timestamp_value.as_str() {
            if let Ok(ts) = u64::from_str_radix(ts_str.trim_start_matches("0x"), 16) {
                return Ok(Some(ts));
            }
            if let Ok(ts) = ts_str.parse::<u64>() {
                return Ok(Some(ts));
            }
        }

        Err(BlockchainError::BlockData {
            block_number,
            reason: format!("unsupported timestamp value {timestamp_value}"),
        })
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
            .map_err(|error| BlockchainError::TransactionLookup {
                tx_hash: format!("{tx_hash:#x}"),
                reason: error.to_string(),
            })?;

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
            *contract_name,
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
        if from_block > current_block {
            return Ok(Vec::new());
        }

        let topic_signatures: Vec<B256> = event_signatures.to_vec();
        let mut all_events = Vec::new();
        let blockchain_id = self.blockchain_id().as_str().to_string();
        let max_span = current_block.saturating_sub(from_block).saturating_add(1);
        let cache_key = event_logs_span_cache_key(contract_address, &topic_signatures);
        let default_span = EVENT_LOGS_INITIAL_SPAN
            .min(max_span)
            .max(EVENT_LOGS_MIN_SPAN);
        let mut span = self
            .get_cached_event_logs_span(&cache_key, default_span)
            .await
            .clamp(EVENT_LOGS_MIN_SPAN, max_span.max(1));

        let mut block = from_block;
        while block <= current_block {
            let to_block = std::cmp::min(block + span - 1, current_block);
            let attempted_span = to_block.saturating_sub(block).saturating_add(1);

            let logs = match self
                .fetch_logs_batch(
                    &blockchain_id,
                    &contract_name,
                    contract_address,
                    &topic_signatures,
                    block,
                    to_block,
                )
                .await
            {
                Ok(logs) => logs,
                Err(err) => {
                    if attempted_span > 1 && should_split_get_logs_error(&err) {
                        span = std::cmp::max(EVENT_LOGS_MIN_SPAN, attempted_span / 2);
                        self.observe_failed_event_logs_span(&cache_key, span).await;
                        continue;
                    }
                    return Err(BlockchainError::get_logs(err));
                }
            };

            for log in &logs {
                if log.topic0().is_some() {
                    all_events.push(ContractLog::new(contract_name, log.clone()));
                }
            }

            self.observe_successful_event_logs_span(&cache_key, attempted_span)
                .await;
            span = grow_span(span, max_span, !logs.is_empty());
            block = to_block.saturating_add(1);
        }

        Ok(all_events)
    }

    /// Find the first block where the contract has non-empty bytecode.
    ///
    /// Returns `None` if the address has no code at `current_block` (not deployed yet).
    pub async fn find_contract_deployment_block(
        &self,
        contract_address: Address,
        current_block: u64,
    ) -> Result<Option<u64>, BlockchainError> {
        let has_code_at = |block_number: u64| async move {
            self.rpc_call(|| async {
                let provider = self.provider().await;
                provider
                    .get_code_at(contract_address)
                    .block_id(block_number.into())
                    .await
            })
            .await
            .map(|bytes| !bytes.is_empty())
            .map_err(|error| BlockchainError::CodeLookup {
                block_number,
                reason: error.to_string(),
            })
        };

        if !has_code_at(current_block).await? {
            return Ok(None);
        }

        let mut low = 0u64;
        let mut high = current_block;
        while low < high {
            let mid = low + (high - low) / 2;
            if has_code_at(mid).await? {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        Ok(Some(low))
    }
}
