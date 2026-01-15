use ethers::{
    abi::{encode, Token},
    utils::keccak256,
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

    /// Calculates the Merkle root of the given quads using keccak256 hashing.
    /// This matches the Solidity/ethers.js implementation using solidityKeccak256.
    pub fn calculate_merkle_root(&self, quads: &[String]) -> String {
        let chunks = self.split_into_chunks(quads, 32);

        // Create leaves by hashing each chunk with its index using Solidity's packed encoding
        let mut leaves: Vec<[u8; 32]> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                // solidityKeccak256(["string", "uint256"], [chunk, index])
                let encoded = encode(&[Token::String(chunk.clone()), Token::Uint(index.into())]);
                keccak256(encoded)
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
