use ethers::{
    abi::{encode_packed, Token},
    types::U256,
    utils::{hex, keccak256},
};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct ValidationManagerConfig {}

pub struct ValidationManager;

impl ValidationManager {
    pub async fn new() -> Self {
        Self
    }

    /// Splits concatenated quads into fixed-size byte chunks.
    fn split_into_chunks(&self, quads: &[String], chunk_size_bytes: usize) -> Vec<String> {
        let concatenated = quads.join("\n");
        let bytes = concatenated.as_bytes();

        bytes
            .chunks(chunk_size_bytes)
            .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
            .collect()
    }

    /// Converts a usize index to a 32-byte big-endian representation (uint256).
    /// Used with Token::FixedBytes to ensure encode_packed outputs exactly 32 bytes,
    /// matching Solidity's abi.encodePacked behavior for uint256.
    fn index_to_uint256_bytes(index: usize) -> Vec<u8> {
        let mut bytes = [0u8; 32];
        U256::from(index).to_big_endian(&mut bytes);
        bytes.to_vec()
    }

    /// Calculates the Merkle root of the given quads using keccak256 hashing.
    /// This matches the Solidity/ethers.js implementation using solidityKeccak256.
    pub fn calculate_merkle_root(&self, quads: &[String]) -> String {
        let chunks = self.split_into_chunks(quads, 32);

        // Create leaves by hashing each chunk with its index using Solidity's packed encoding
        // This matches ethers.js solidityKeccak256(["string", "uint256"], [chunk, index])
        // Note: We use Token::FixedBytes for the index because ethers-rs encode_packed
        // strips leading zeros from Token::Uint, but Solidity uses full 32 bytes for uint256.
        let mut leaves: Vec<[u8; 32]> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let packed = encode_packed(&[
                    Token::String(chunk.clone()),
                    Token::FixedBytes(Self::index_to_uint256_bytes(index)),
                ])
                .expect("Failed to encode packed data");
                keccak256(packed)
            })
            .collect();

        // Build the Merkle tree
        while leaves.len() > 1 {
            let mut next_level = Vec::new();

            let mut i = 0;
            while i < leaves.len() {
                let left = leaves[i];

                if i + 1 >= leaves.len() {
                    // Odd number of leaves, promote the last one
                    next_level.push(left);
                    break;
                }

                let right = leaves[i + 1];

                // Sort the pair for consistent ordering (like Buffer.compare in JS)
                let mut combined = [left, right];
                combined.sort();

                // Concatenate and hash
                let mut concat = Vec::with_capacity(64);
                concat.extend_from_slice(&combined[0]);
                concat.extend_from_slice(&combined[1]);
                next_level.push(keccak256(&concat));

                i += 2;
            }

            leaves = next_level;
        }

        format!("0x{}", hex::encode(leaves[0]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_into_chunks() {
        let vm = ValidationManager;

        // Test that quads are joined with newline and split into 32-byte chunks
        let quads = vec!["hello".to_string()];
        let chunks = vm.split_into_chunks(&quads, 32);
        assert_eq!(chunks, vec!["hello"]);

        // Test with multiple quads
        let quads = vec!["a".to_string(), "b".to_string()];
        let chunks = vm.split_into_chunks(&quads, 32);
        assert_eq!(chunks, vec!["a\nb"]);
    }

    #[test]
    fn test_index_to_uint256_bytes() {
        // Test that index is converted to 32-byte big-endian
        let bytes = ValidationManager::index_to_uint256_bytes(0);
        assert_eq!(bytes.len(), 32);
        assert_eq!(bytes, vec![0u8; 32]);

        let bytes = ValidationManager::index_to_uint256_bytes(1);
        assert_eq!(bytes.len(), 32);
        assert_eq!(bytes[31], 1);
        assert_eq!(&bytes[..31], &[0u8; 31]);

        let bytes = ValidationManager::index_to_uint256_bytes(256);
        assert_eq!(bytes.len(), 32);
        assert_eq!(bytes[30], 1);
        assert_eq!(bytes[31], 0);
    }

    #[test]
    fn test_encode_packed_produces_correct_length() {
        // Verify that using FixedBytes produces correct 36-byte output
        let packed = encode_packed(&[
            Token::String("test".to_string()),
            Token::FixedBytes(ValidationManager::index_to_uint256_bytes(0)),
        ])
        .unwrap();

        // For "test" + uint256(0):
        // - "test" = 0x74657374 (4 bytes)
        // - uint256(0) = 0x0000...0000 (32 bytes of zeros)
        // Total should be 36 bytes
        assert_eq!(packed.len(), 36);
        assert_eq!(&packed[..4], b"test");
        assert_eq!(&packed[4..], &[0u8; 32]);
    }

    #[test]
    fn test_merkle_root_single_leaf() {
        let vm = ValidationManager;

        // Single leaf should return the hash of that leaf
        let quads = vec!["test".to_string()];
        let root = vm.calculate_merkle_root(&quads);

        // Verify root is a valid hex string
        assert!(root.starts_with("0x"));
        assert_eq!(root.len(), 66); // "0x" + 64 hex chars
    }
}
