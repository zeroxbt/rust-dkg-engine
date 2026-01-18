use alloy::{
    contract::Error as ContractError,
    primitives::{Address, U256, hex, keccak256},
    providers::PendingTransactionBuilder,
    rpc::types::TransactionReceipt,
    sol_types::SolEventInterface,
};
use sha2::{Digest, Sha256};

use crate::error::BlockchainError;

pub fn decode_event<E: SolEventInterface>(log: &alloy::rpc::types::Log) -> Option<E> {
    E::decode_log(log.as_ref()).ok().map(|decoded| decoded.data)
}

pub fn from_wei(wei: &str) -> String {
    let wei_value = U256::from_str_radix(wei, 10).unwrap_or(U256::ZERO);
    alloy::primitives::utils::format_ether(wei_value)
}

pub fn to_wei(ether: &str) -> U256 {
    alloy::primitives::utils::parse_ether(ether).unwrap_or(U256::ZERO)
}

pub fn encode_packed_keyword(address: Address, assertion_id: [u8; 32]) -> Vec<u8> {
    let mut packed = Vec::with_capacity(20 + 32);
    packed.extend_from_slice(address.as_slice());
    packed.extend_from_slice(&assertion_id);
    packed
}

pub fn to_hex_string(data: impl AsRef<[u8]>) -> String {
    hex::encode(data)
}

pub fn from_hex_string(data: &str) -> Result<Vec<u8>, hex::FromHexError> {
    let cleaned = data.strip_prefix("0x").unwrap_or(data);
    hex::decode(cleaned)
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SignatureComponents {
    pub v: u8,
    pub r: String,
    pub s: String,
    pub vs: String,
}

pub fn split_signature(flat_signature: Vec<u8>) -> Result<SignatureComponents, BlockchainError> {
    if flat_signature.len() != 65 {
        return Err(BlockchainError::Custom(format!(
            "Invalid signature length: expected 65 bytes, got {}",
            flat_signature.len()
        )));
    }

    let r_bytes: [u8; 32] = flat_signature[0..32]
        .try_into()
        .map_err(|_| BlockchainError::Custom("Invalid r length".to_string()))?;
    let s_bytes: [u8; 32] = flat_signature[32..64]
        .try_into()
        .map_err(|_| BlockchainError::Custom("Invalid s length".to_string()))?;
    let v = flat_signature[64];

    let r = format!("0x{}", hex::encode(r_bytes));
    let s = format!("0x{}", hex::encode(s_bytes));

    // Compute vs (compact signature format: s with the parity bit from v encoded in the high bit)
    let mut vs_bytes = s_bytes;
    // If v is 28 (or 1 in the 0/1 encoding), set the high bit of s
    if v == 28 || v == 1 {
        vs_bytes[0] |= 0x80;
    }
    let vs = format!("0x{}", hex::encode(vs_bytes));

    Ok(SignatureComponents { v, r, s, vs })
}

pub(super) async fn handle_contract_call(
    result: Result<PendingTransactionBuilder<alloy::network::Ethereum>, ContractError>,
) -> Result<TransactionReceipt, BlockchainError> {
    match result {
        Ok(pending_tx) => {
            let receipt = pending_tx.get_receipt().await;
            match receipt {
                Ok(r) => Ok(r),
                Err(err) => {
                    tracing::error!("Failed to retrieve transaction receipt: {:?}", err);
                    Err(BlockchainError::Custom(format!(
                        "Failed to get transaction receipt: {}",
                        err
                    )))
                }
            }
        }
        Err(err) => {
            // Log the error
            tracing::error!("Contract call failed: {:?}", err);
            Err(BlockchainError::Contract(err))
        }
    }
}
