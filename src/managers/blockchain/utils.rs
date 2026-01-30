use alloy::primitives::{U256, hex, keccak256};
use sha2::{Digest, Sha256};

pub(crate) fn from_wei(wei: &str) -> String {
    let wei_value = U256::from_str_radix(wei, 10).unwrap_or(U256::ZERO);
    alloy::primitives::utils::format_ether(wei_value)
}

pub(crate) fn to_hex_string(data: impl AsRef<[u8]>) -> String {
    hex::encode(data)
}

/// Keccak256 of pre-packed ABI bytes (address/bytesN should already be packed).
pub(crate) fn keccak256_encode_packed(parts: &[&[u8]]) -> [u8; 32] {
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

pub(crate) fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    let digest = hasher.finalize();
    to_hex_string(digest)
}
