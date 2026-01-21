use alloy::{
    primitives::{B256, U256, hex, keccak256},
    sol_types::SolValue,
};

const CHUNK_SIZE: usize = 32;

pub fn calculate_merkle_root(quads: &[String]) -> String {
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
pub fn calculate_assertion_size(quads: &[String]) -> usize {
    let concatenated = quads.join("\n");
    let total_bytes = concatenated.len();
    let num_chunks = total_bytes.div_ceil(CHUNK_SIZE);
    num_chunks * CHUNK_SIZE
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
}
