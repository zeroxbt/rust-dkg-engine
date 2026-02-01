use alloy::{
    primitives::{B256, U256, hex, keccak256},
    sol_types::SolValue,
};

const CHUNK_SIZE: usize = 32;

pub(crate) fn calculate_merkle_root(quads: &[String]) -> String {
    let chunks = split_into_chunks(quads);

    let mut leaves: Vec<B256> = chunks
        .iter()
        .enumerate()
        .map(|(i, c)| leaf_hash(c, i))
        .collect();

    if leaves.is_empty() {
        return "0x".to_string();
    }

    while leaves.len() > 1 {
        let mut next = Vec::with_capacity(leaves.len().div_ceil(2));

        let mut i = 0usize;
        while i < leaves.len() {
            let left = leaves[i];

            if i + 1 >= leaves.len() {
                next.push(left); // carry
                break;
            }

            let right = leaves[i + 1];
            let (a, b) = if left <= right {
                (left, right)
            } else {
                (right, left)
            };

            let mut buf = [0u8; 64];
            buf[..32].copy_from_slice(a.as_slice());
            buf[32..].copy_from_slice(b.as_slice());

            next.push(keccak256(buf));
            i += 2;
        }

        leaves = next;
    }

    format!("0x{}", hex::encode(leaves[0].as_slice()))
}

/// Calculate the byte size of an assertion based on its chunks.
/// This matches the on-chain byteSize calculation: numberOfChunks * CHUNK_SIZE
///
/// The calculation:
/// 1. Join quads with newline
/// 2. Encode as UTF-8 bytes
/// 3. numberOfChunks = ceil(totalBytes / CHUNK_SIZE)
/// 4. byteSize = numberOfChunks * CHUNK_SIZE
pub(crate) fn calculate_assertion_size(quads: &[String]) -> usize {
    let concatenated = quads.join("\n");
    let total_bytes = concatenated.len();
    let num_chunks = total_bytes.div_ceil(CHUNK_SIZE);
    num_chunks * CHUNK_SIZE
}

/// Result of Merkle proof calculation.
#[derive(Debug)]
pub(crate) struct MerkleProofResult {
    /// The chunk content (string)
    pub chunk: String,
    /// The Merkle proof path (32-byte hashes)
    pub proof: Vec<B256>,
}

/// Calculate Merkle proof for a specific chunk.
///
/// Returns the chunk content and proof path, or an error if chunk_index is out of range.
pub(crate) fn calculate_merkle_proof(
    quads: &[String],
    chunk_index: usize,
) -> Result<MerkleProofResult, String> {
    let chunks = split_into_chunks(quads);

    if chunks.is_empty() {
        return Err("No chunks to prove".to_string());
    }

    if chunk_index >= chunks.len() {
        return Err(format!(
            "Chunk index {} out of range, only {} chunks exist",
            chunk_index,
            chunks.len()
        ));
    }

    // Hash all leaves
    let leaves: Vec<B256> = chunks
        .iter()
        .enumerate()
        .map(|(i, c)| leaf_hash(c, i))
        .collect();

    // Build proof by walking up the tree
    let proof = build_merkle_proof(&leaves, chunk_index);

    Ok(MerkleProofResult {
        chunk: chunks[chunk_index].clone(),
        proof,
    })
}

/// Build a Merkle proof for the given leaf index.
/// Matches the JS implementation: traverse tree upward, collecting sibling hashes.
fn build_merkle_proof(leaves: &[B256], leaf_index: usize) -> Vec<B256> {
    if leaves.len() <= 1 {
        return vec![];
    }

    let mut proof = Vec::new();
    let mut current_level = leaves.to_vec();
    let mut index = leaf_index;

    while current_level.len() > 1 {
        let mut next_level = Vec::new();

        let mut i = 0usize;
        while i < current_level.len() {
            let left = current_level[i];

            if i + 1 >= current_level.len() {
                // Odd node - no sibling, just carry up
                next_level.push(left);

                // If this is our target index, update for next level
                if i == index {
                    index = i / 2;
                }
                break;
            }

            let right = current_level[i + 1];

            // If this pair contains our target index, add sibling to proof
            if i == index || i + 1 == index {
                let sibling = if index == i { right } else { left };
                proof.push(sibling);
                index = i / 2; // Update index to parent position
            }

            // Hash pair (sorted order)
            let (a, b) = if left <= right {
                (left, right)
            } else {
                (right, left)
            };

            let mut buf = [0u8; 64];
            buf[..32].copy_from_slice(a.as_slice());
            buf[32..].copy_from_slice(b.as_slice());

            next_level.push(keccak256(buf));
            i += 2;
        }

        current_level = next_level;
    }

    proof
}

fn split_into_chunks(quads: &[String]) -> Vec<String> {
    let concatenated = quads.join("\n");
    let bytes = concatenated.as_bytes();

    let mut chunks = Vec::new();
    let mut start = 0usize;

    while start < bytes.len() {
        let end = (start + CHUNK_SIZE).min(bytes.len());
        chunks.push(String::from_utf8_lossy(&bytes[start..end]).into_owned());
        start = end;
    }

    chunks
}

fn leaf_hash(chunk: &str, index: usize) -> B256 {
    let packed = (chunk.to_string(), U256::from(index)).abi_encode_packed();
    keccak256(packed)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_calculate_merkle_root() {
        let quads: Vec<String> = [
            "<urn:us-cities:data:new-york> <http://schema.org/averageIncome> \"$63,998\" .",
            "<urn:us-cities:data:new-york> <http://schema.org/crimeRate> \"Low\" .",
            "<urn:us-cities:data:new-york> <http://schema.org/infrastructureScore> \"8.5\" .",
            "<urn:us-cities:data:new-york> <http://schema.org/relatedCities> <urn:us-cities:info:chicago> .",
            "<urn:us-cities:data:new-york> <http://schema.org/relatedCities> <urn:us-cities:info:los-angeles> .",
            "<urn:us-cities:data:new-york> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/CityPrivateData> .",
            "<urn:us-cities:info:chicago> <http://schema.org/name> \"Chicago\" .",
            "<urn:us-cities:info:los-angeles> <http://schema.org/name> \"Los Angeles\" ."
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let root = calculate_merkle_root(&quads);
        assert_eq!(
            root,
            "0xaac2a420672a1eb77506c544ff01beed2be58c0ee3576fe037c846f97481cefd".to_string()
        );

        let quads = ["<urn:us-cities:info:new-york> <http://schema.org/area> \"468.9 sq mi\" .",
            "<urn:us-cities:info:new-york> <http://schema.org/name> \"New York\" .",
            "<urn:us-cities:info:new-york> <http://schema.org/population> \"8,336,817\" .",
            "<urn:us-cities:info:new-york> <http://schema.org/state> \"New York\" .",
            "<urn:us-cities:info:new-york> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/City> .",
            "<uuid:1e91a527-a3ef-430e-819e-64710ab0f797> <https://ontology.origintrail.io/dkg/1.0#privateMerkleRoot> \"0xaac2a420672a1eb77506c544ff01beed2be58c0ee3576fe037c846f97481cefd\" .",
            "<https://ontology.origintrail.io/dkg/1.0#metadata-hash:0x5cb6421dd41c7a62a84c223779303919e7293753d8a1f6f49da2e598013fe652> <https://ontology.origintrail.io/dkg/1.0#representsPrivateResource> <uuid:b88ffefd-ce8e-42fb-8a49-7b91d77c71bf> .",
            "<https://ontology.origintrail.io/dkg/1.0#metadata-hash:0x6a2292b30c844d2f8f2910bf11770496a3a79d5a6726d1b2fd3ddd18e09b5850> <https://ontology.origintrail.io/dkg/1.0#representsPrivateResource> <uuid:88a388be-5822-49e9-8663-8820e02707ab> .",
            "<https://ontology.origintrail.io/dkg/1.0#metadata-hash:0xc1f682b783b1b93c9d5386eb1730c9647cf4b55925ec24f5e949e7457ba7bfac> <https://ontology.origintrail.io/dkg/1.0#representsPrivateResource> <uuid:dcb5abcf-66c4-4e63-9dbe-db9da20cb22a> ."].iter().map(|s|s.to_string()).collect::<Vec<String>>();
        let root = calculate_merkle_root(&quads);
        assert_eq!(
            root,
            "0x66ca3160277b181d0307262a0127f5f570f1d8c1b3276e8fe3b0e19ba8edcc35".to_string()
        );
    }

    #[test]
    fn test_split_into_chunks() {
        // Test that quads are joined with newline and split into 32-byte chunks
        let quads = vec!["hello".to_string()];
        let chunks = split_into_chunks(&quads);
        assert_eq!(chunks, vec!["hello"]);

        // Test with multiple quads
        let quads = vec!["a".to_string(), "b".to_string()];
        let chunks = split_into_chunks(&quads);
        assert_eq!(chunks, vec!["a\nb"]);
    }

    #[test]
    fn test_merkle_proof_verifies() {
        // Test that proof can be used to reconstruct the root
        let quads: Vec<String> = [
            "<urn:us-cities:data:new-york> <http://schema.org/averageIncome> \"$63,998\" .",
            "<urn:us-cities:data:new-york> <http://schema.org/crimeRate> \"Low\" .",
            "<urn:us-cities:data:new-york> <http://schema.org/infrastructureScore> \"8.5\" .",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let expected_root = calculate_merkle_root(&quads);

        // Test proof for each chunk
        let chunks = split_into_chunks(&quads);
        for chunk_index in 0..chunks.len() {
            let result = calculate_merkle_proof(&quads, chunk_index).unwrap();

            // Verify by reconstructing root from leaf and proof
            let mut current = leaf_hash(&result.chunk, chunk_index);

            for sibling in &result.proof {
                let (a, b) = if current <= *sibling {
                    (current, *sibling)
                } else {
                    (*sibling, current)
                };

                let mut buf = [0u8; 64];
                buf[..32].copy_from_slice(a.as_slice());
                buf[32..].copy_from_slice(b.as_slice());
                current = keccak256(buf);
            }

            let reconstructed_root = format!("0x{}", hex::encode(current.as_slice()));
            assert_eq!(
                reconstructed_root, expected_root,
                "Proof verification failed for chunk {}",
                chunk_index
            );
        }
    }

    #[test]
    fn test_merkle_proof_matches_js_implementation() {
        // Test data with known merkle root from JS implementation.
        // Values computed by scripts/compute_merkle_test_values.js
        let quads: Vec<String> = [
            "<urn:us-cities:data:new-york> <http://schema.org/averageIncome> \"$63,998\" .",
            "<urn:us-cities:data:new-york> <http://schema.org/crimeRate> \"Low\" .",
            "<urn:us-cities:data:new-york> <http://schema.org/infrastructureScore> \"8.5\" .",
            "<urn:us-cities:data:new-york> <http://schema.org/relatedCities> <urn:us-cities:info:chicago> .",
            "<urn:us-cities:data:new-york> <http://schema.org/relatedCities> <urn:us-cities:info:los-angeles> .",
            "<urn:us-cities:data:new-york> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/CityPrivateData> .",
            "<urn:us-cities:info:chicago> <http://schema.org/name> \"Chicago\" .",
            "<urn:us-cities:info:los-angeles> <http://schema.org/name> \"Los Angeles\" ."
        ]
        .into_iter()
        .map(String::from)
        .collect();

        // Expected values from JS implementation (ground truth)
        let expected_root = "0xaac2a420672a1eb77506c544ff01beed2be58c0ee3576fe037c846f97481cefd";

        // Chunk 0 expected values
        let expected_chunk_0 = "<urn:us-cities:data:new-york> <h";
        let expected_leaf_0 = "0xb089efe712bfafeedb85909d5f66e31477c8bde6f51ef17a50bd52724359ea33";
        let expected_proof_0 = vec![
            "0x775be8c6d24fab821b0ddf791c82e7f0c438fa7c30a4cb5dc65002780ad35fdc",
            "0x7237ac7bfd6365610cf199f5f0a2e6650932057f665f646221106a0003709534",
            "0xb805e9c3d1990b59ef39029b06f720f6bb2cc23bdc6a3cf9b07ab32b95c3682a",
            "0x9687dcbece3e1cfb0dc081053bb5701d4955368d1c77e285c6b0c6092459d9b7",
            "0x2d62427a000a95d1f57b2f4e16cf588935039af6134a8014fbbbcfbcffdb4971",
        ];

        // Chunk 5 expected values
        let expected_chunk_5 = "ata:new-york> <http://schema.org";
        let expected_leaf_5 = "0x4a583ba6710db1d4c189100fecdec85bf966f0eae7c7f2401aa0254999a08bd3";
        let expected_proof_5 = vec![
            "0xc3619fcb5bd5188c2874bbd6c456660817157af10910000de0e7b18dec3d1266",
            "0x6f78f502387aef8b40113a1cfafd2cbbaa8d24398c3d7469072ea67da906fc76",
            "0x8985dbdd01925ab25b1586cb66124c1a785c5a9b8551302ec2b493335e7bf21c",
            "0x9687dcbece3e1cfb0dc081053bb5701d4955368d1c77e285c6b0c6092459d9b7",
            "0x2d62427a000a95d1f57b2f4e16cf588935039af6134a8014fbbbcfbcffdb4971",
        ];

        // Verify root calculation matches JS
        let root = calculate_merkle_root(&quads);
        assert_eq!(root, expected_root, "Root calculation must match JS");

        let chunks = split_into_chunks(&quads);
        assert_eq!(chunks.len(), 22, "Should have 22 chunks");

        // Test chunk 0
        let result_0 = calculate_merkle_proof(&quads, 0).unwrap();
        assert_eq!(
            result_0.chunk, expected_chunk_0,
            "Chunk 0 content must match"
        );
        let leaf_0 = format!(
            "0x{}",
            hex::encode(leaf_hash(&result_0.chunk, 0).as_slice())
        );
        assert_eq!(leaf_0, expected_leaf_0, "Chunk 0 leaf hash must match");
        let proof_0: Vec<String> = result_0
            .proof
            .iter()
            .map(|b| format!("0x{}", hex::encode(b.as_slice())))
            .collect();
        assert_eq!(proof_0, expected_proof_0, "Chunk 0 proof must match");

        // Test chunk 5
        let result_5 = calculate_merkle_proof(&quads, 5).unwrap();
        assert_eq!(
            result_5.chunk, expected_chunk_5,
            "Chunk 5 content must match"
        );
        let leaf_5 = format!(
            "0x{}",
            hex::encode(leaf_hash(&result_5.chunk, 5).as_slice())
        );
        assert_eq!(leaf_5, expected_leaf_5, "Chunk 5 leaf hash must match");
        let proof_5: Vec<String> = result_5
            .proof
            .iter()
            .map(|b| format!("0x{}", hex::encode(b.as_slice())))
            .collect();
        assert_eq!(proof_5, expected_proof_5, "Chunk 5 proof must match");

        // Verify all proofs reconstruct the root
        for chunk_idx in 0..chunks.len() {
            let result = calculate_merkle_proof(&quads, chunk_idx).unwrap();
            let mut current = leaf_hash(&result.chunk, chunk_idx);

            for sibling in &result.proof {
                let (a, b) = if current <= *sibling {
                    (current, *sibling)
                } else {
                    (*sibling, current)
                };
                let mut buf = [0u8; 64];
                buf[..32].copy_from_slice(a.as_slice());
                buf[32..].copy_from_slice(b.as_slice());
                current = keccak256(buf);
            }

            let reconstructed = format!("0x{}", hex::encode(current.as_slice()));
            assert_eq!(
                reconstructed, expected_root,
                "Proof for chunk {} must reconstruct the root",
                chunk_idx
            );
        }
    }
}
