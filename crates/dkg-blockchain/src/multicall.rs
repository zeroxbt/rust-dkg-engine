//! Generic Multicall3 batching support.
//!
//! Provides a builder pattern for batching heterogeneous EVM calls into
//! efficient Multicall3 requests with automatic chunking.

use alloy::{
    hex,
    primitives::{Address, Bytes, U256},
};

use super::chains::evm::Multicall3;

/// Maximum calls per Multicall3 request to avoid RPC limits.
pub const MULTICALL_CHUNK_SIZE: usize = 100;

/// A single call to be batched in a Multicall3 request.
#[derive(Clone)]
pub struct MulticallRequest {
    /// Target contract address
    pub target: Address,
    /// Whether to allow this call to fail without reverting the entire batch
    pub allow_failure: bool,
    /// Encoded calldata (selector + abi-encoded arguments)
    pub call_data: Bytes,
}

impl MulticallRequest {
    /// Create a new multicall request.
    pub fn new(target: Address, call_data: Bytes) -> Self {
        Self {
            target,
            allow_failure: true,
            call_data,
        }
    }

    /// Create a request that will revert the batch if it fails.
    #[allow(dead_code)]
    pub fn new_required(target: Address, call_data: Bytes) -> Self {
        Self {
            target,
            allow_failure: false,
            call_data,
        }
    }

    /// Convert to Multicall3::Call3 struct.
    pub fn into_call3(self) -> Multicall3::Call3 {
        Multicall3::Call3 {
            target: self.target,
            allowFailure: self.allow_failure,
            callData: self.call_data,
        }
    }
}

/// Result of a single call in a Multicall3 batch.
#[derive(Clone)]
pub struct MulticallResult {
    /// Whether the call succeeded
    pub success: bool,
    /// Return data (may be empty or error data if failed)
    pub return_data: Bytes,
}

impl MulticallResult {
    /// Create from Multicall3::Result
    pub fn from_result(result: &Multicall3::Result) -> Self {
        Self {
            success: result.success,
            return_data: result.returnData.clone(),
        }
    }

    /// Get return data as a slice.
    pub fn data(&self) -> &[u8] {
        self.return_data.as_ref()
    }

    /// Parse return data as a single U256 value.
    pub fn as_u256(&self) -> Option<U256> {
        if !self.success || self.data().len() < 32 {
            return None;
        }
        Some(U256::from_be_slice(&self.data()[0..32]))
    }

    /// Parse return data as a single u64 value.
    pub fn as_u64(&self) -> Option<u64> {
        self.as_u256().and_then(|v| v.try_into().ok())
    }

    /// Parse return data as a bytes32 hex string (0x-prefixed).
    pub fn as_bytes32_hex(&self) -> Option<String> {
        if !self.success || self.data().len() < 32 {
            return None;
        }
        let bytes = &self.data()[0..32];
        // Check if all zeros (no data)
        if bytes.iter().all(|&b| b == 0) {
            return None;
        }
        Some(format!("0x{}", hex::encode(bytes)))
    }

    /// Parse return data as a knowledge assets range tuple: (start_token_id, end_token_id,
    /// burned[]).
    ///
    /// ABI encoding:
    /// - bytes 0-31: start_token_id (uint256)
    /// - bytes 32-63: end_token_id (uint256)
    /// - bytes 64-95: offset to dynamic array
    /// - bytes at offset: length of array, then elements
    pub fn as_knowledge_assets_range(&self) -> Option<(u64, u64, Vec<u64>)> {
        let data = self.data();
        if !self.success || data.len() < 64 {
            return None;
        }

        let start_token_id = U256::from_be_slice(&data[0..32]);
        let end_token_id = U256::from_be_slice(&data[32..64]);

        // Parse burned array
        let mut burned: Vec<u64> = Vec::new();
        if data.len() >= 96 {
            let array_offset: usize = U256::from_be_slice(&data[64..96]).try_into().unwrap_or(0);
            if data.len() >= array_offset + 32 {
                let array_len: usize = U256::from_be_slice(&data[array_offset..array_offset + 32])
                    .try_into()
                    .unwrap_or(0);
                for j in 0..array_len {
                    let elem_start = array_offset + 32 + j * 32;
                    if data.len() >= elem_start + 32 {
                        let elem = U256::from_be_slice(&data[elem_start..elem_start + 32]);
                        if let Ok(v) = elem.try_into() {
                            burned.push(v);
                        }
                    }
                }
            }
        }

        let start: u64 = start_token_id.try_into().unwrap_or(0);
        let end: u64 = end_token_id.try_into().unwrap_or(0);

        // If both start and end are 0, the collection doesn't exist
        if start == 0 && end == 0 {
            None
        } else {
            Some((start, end, burned))
        }
    }
}

/// Builder for constructing Multicall3 batches.
///
/// # Example
///
/// ```ignore
/// let mut builder = MulticallBatch::new();
///
/// // Add calls for different functions/contracts
/// builder.add(MulticallRequest::new(
///     contract_address,
///     encode_get_merkle_root(kc_id),
/// ));
/// builder.add(MulticallRequest::new(
///     other_contract,
///     encode_get_end_epoch(kc_id),
/// ));
///
/// // Execute and get results
/// let results = evm_chain.execute_multicall(builder).await?;
/// ```
pub struct MulticallBatch {
    requests: Vec<MulticallRequest>,
}

impl MulticallBatch {
    /// Create a new empty batch.
    pub fn new() -> Self {
        Self {
            requests: Vec::new(),
        }
    }

    /// Create a batch with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            requests: Vec::with_capacity(capacity),
        }
    }

    /// Add a request to the batch.
    pub fn add(&mut self, request: MulticallRequest) {
        self.requests.push(request);
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    /// Consume the batch and return chunks of Call3 structs.
    ///
    /// Each chunk is sized to respect RPC limits.
    pub fn into_chunks(self) -> impl Iterator<Item = Vec<Multicall3::Call3>> {
        self.requests
            .into_iter()
            .map(MulticallRequest::into_call3)
            .collect::<Vec<_>>()
            .chunks(MULTICALL_CHUNK_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>()
            .into_iter()
    }
}

impl Default for MulticallBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper functions for encoding common call data.
pub mod encoders {
    use alloy::primitives::{Bytes, U256};

    /// Encode a call with a single uint256 argument.
    ///
    /// This is the most common pattern: `function(uint256)`
    pub fn encode_uint256_call(selector: [u8; 4], value: u128) -> Bytes {
        let mut call_data = selector.to_vec();
        call_data.extend_from_slice(&U256::from(value).to_be_bytes::<32>());
        Bytes::from(call_data)
    }

    // Well-known function selectors
    // Computed via: cast sig "functionName(args)"

    /// Selector for `getLatestMerkleRoot(uint256)` = 0xe19e8a72
    pub const GET_MERKLE_ROOT_SELECTOR: [u8; 4] = [0xe1, 0x9e, 0x8a, 0x72];

    /// Selector for `getKnowledgeAssetsRange(uint256)` = 0xbfed2578
    pub const GET_KNOWLEDGE_ASSETS_RANGE_SELECTOR: [u8; 4] = [0xbf, 0xed, 0x25, 0x78];

    /// Selector for `getEndEpoch(uint256)` = 0x22dff80c
    pub const GET_END_EPOCH_SELECTOR: [u8; 4] = [0x22, 0xdf, 0xf8, 0x0c];

    /// Encode `getLatestMerkleRoot(uint256 knowledgeCollectionId)`
    pub fn encode_get_merkle_root(kc_id: u128) -> Bytes {
        encode_uint256_call(GET_MERKLE_ROOT_SELECTOR, kc_id)
    }

    /// Encode `getKnowledgeAssetsRange(uint256 knowledgeCollectionId)`
    pub fn encode_get_knowledge_assets_range(kc_id: u128) -> Bytes {
        encode_uint256_call(GET_KNOWLEDGE_ASSETS_RANGE_SELECTOR, kc_id)
    }

    /// Encode `getEndEpoch(uint256 knowledgeCollectionId)`
    pub fn encode_get_end_epoch(kc_id: u128) -> Bytes {
        encode_uint256_call(GET_END_EPOCH_SELECTOR, kc_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multicall_batch_chunking() {
        let mut batch = MulticallBatch::new();
        let addr = Address::ZERO;

        // Add 250 requests
        for i in 0..250 {
            batch.add(MulticallRequest::new(
                addr,
                encoders::encode_get_merkle_root(i),
            ));
        }

        let chunks: Vec<_> = batch.into_chunks().collect();

        // Should be 3 chunks: 100 + 100 + 50
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 100);
        assert_eq!(chunks[1].len(), 100);
        assert_eq!(chunks[2].len(), 50);
    }

    #[test]
    fn test_encode_uint256_call() {
        let call_data = encoders::encode_get_merkle_root(123);
        let bytes = call_data.as_ref();

        // First 4 bytes should be selector
        assert_eq!(&bytes[0..4], &encoders::GET_MERKLE_ROOT_SELECTOR);

        // Remaining 32 bytes should be uint256(123)
        let value = U256::from_be_slice(&bytes[4..36]);
        assert_eq!(value, U256::from(123));
    }

    #[test]
    fn test_multicall_result_parsing() {
        // Simulate a successful result with a bytes32 value
        let mut data = vec![0u8; 32];
        data[31] = 0x7b; // 123 in hex
        let result = MulticallResult {
            success: true,
            return_data: Bytes::from(data),
        };

        assert_eq!(result.as_u64(), Some(123));
        assert!(result.as_bytes32_hex().is_some());
    }

    #[test]
    fn test_multicall_result_failure() {
        let result = MulticallResult {
            success: false,
            return_data: Bytes::new(),
        };

        assert_eq!(result.as_u64(), None);
        assert_eq!(result.as_bytes32_hex(), None);
    }
}
