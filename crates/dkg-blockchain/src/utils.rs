use alloy::primitives::{hex, keccak256};
use sha2::{Digest, Sha256};

pub fn to_hex_string(data: impl AsRef<[u8]>) -> String {
    hex::encode(data)
}

/// Keccak256 of pre-packed ABI bytes (address/bytesN should already be packed).
pub fn keccak256_encode_packed(parts: &[&[u8]]) -> [u8; 32] {
    let total_len = parts.iter().map(|part| part.len()).sum();
    let mut packed = Vec::with_capacity(total_len);
    for part in parts {
        packed.extend_from_slice(part);
    }

    let hash = keccak256(packed);
    let mut out = [0u8; 32];
    out.copy_from_slice(hash.as_ref());
    out
}

pub fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    let digest = hasher.finalize();
    to_hex_string(digest)
}

pub fn parse_ether_to_u128(amount: &str) -> Result<u128, String> {
    let wei = alloy::primitives::utils::parse_ether(amount).map_err(|e| e.to_string())?;
    wei.try_into().map_err(|_| "Amount too large".to_string())
}
