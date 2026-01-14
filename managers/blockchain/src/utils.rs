use ethers::{
    abi::{Address, EncodePackedError, Token},
    contract::{EthLogDecode, parse_log},
    prelude::{ContractError, TransactionReceipt},
    providers::{Http, PendingTransaction},
    types::U256,
};
use hex::FromHexError;

use crate::{
    blockchains::{abstract_blockchain::EventLog, blockchain_creator::BlockchainProvider},
    error::BlockchainError,
};

pub fn decode_event_log<D: EthLogDecode>(event_log: EventLog) -> D {
    parse_log::<D>(event_log.log().to_owned()).unwrap()
}

pub fn from_wei(wei: &str) -> String {
    ethers::utils::format_ether(U256::from_dec_str(wei).unwrap())
}

pub fn to_wei(ethers: &str) -> U256 {
    ethers::utils::parse_ether(ethers).unwrap()
}

pub fn encode_packed_keyword(
    address: Address,
    assertion_id: [u8; 32],
) -> Result<Vec<u8>, EncodePackedError> {
    ethers::abi::encode_packed(&[
        Token::Address(address),
        Token::FixedBytes(assertion_id.to_vec()),
    ])
}

pub fn to_hex_string(data: Vec<u8>) -> String {
    hex::encode(data)
}

pub fn from_hex_string(data: String) -> Result<Vec<u8>, FromHexError> {
    hex::decode(data)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SignatureComponents {
    pub v: u8,
    pub r: String,
    pub s: String,
    pub vs: String,
}

pub fn split_signature(flat_signature: Vec<u8>) -> Result<SignatureComponents, BlockchainError> {
    use ethers::types::Signature;

    if flat_signature.len() != 65 {
        return Err(BlockchainError::Custom(format!(
            "Invalid signature length: expected 65 bytes, got {}",
            flat_signature.len()
        )));
    }

    // Parse the signature
    let signature = Signature::try_from(flat_signature.as_slice())
        .map_err(|e| BlockchainError::Custom(format!("Failed to parse signature: {}", e)))?;

    // Extract components (v is a u64 in ethers, but ECDSA v is actually 0/1 or 27/28)
    let v = signature.v as u8;

    // Convert U256 to bytes (32 bytes each for r and s)
    let mut r_bytes = [0u8; 32];
    signature.r.to_big_endian(&mut r_bytes);
    let r = format!("0x{}", hex::encode(r_bytes));

    let mut s_bytes = [0u8; 32];
    signature.s.to_big_endian(&mut s_bytes);
    let s = format!("0x{}", hex::encode(s_bytes));

    // Compute vs (compact signature format: s with the parity bit from v encoded in the high bit)
    let mut vs_bytes = s_bytes;
    // If v is 28 (or 1 in the 0/1 encoding), set the high bit of s
    if signature.v == 28 || signature.v == 1 {
        vs_bytes[0] |= 0x80;
    }
    let vs = format!("0x{}", hex::encode(vs_bytes));

    Ok(SignatureComponents { v, r, s, vs })
}

pub(super) async fn handle_contract_call(
    result: Result<PendingTransaction<'_, Http>, ContractError<BlockchainProvider>>,
) -> Result<Option<TransactionReceipt>, BlockchainError> {
    match result {
        Ok(future_receipt) => {
            let receipt = future_receipt.await;
            match receipt {
                Ok(r) => Ok(r),
                Err(err) => {
                    tracing::error!("Failed to retrieve transaction receipt: {:?}", err);
                    Err(BlockchainError::Contract(err.into()))
                }
            }
        }
        Err(err) => {
            match &err {
                // note the use of & to borrow rather than move
                ethers::contract::ContractError::Revert(revert_msg) => {
                    let error_msg =
                        ethers::abi::decode(&[ethers::abi::ParamType::String], &revert_msg.0[4..])
                            .map_err(|_| BlockchainError::Decode)?
                            .into_iter()
                            .next()
                            .and_then(|param| match param {
                                ethers::abi::Token::String(msg) => Some(msg),
                                _ => None,
                            });

                    if let Some(msg) = error_msg {
                        tracing::error!("Smart contract reverted with message: {}", msg);
                    } else {
                        tracing::error!("Failed to decode revert message");
                    }
                }
                _ => {
                    tracing::error!("An error occurred: {:?}", err);
                }
            }
            Err(BlockchainError::Contract(err))
        }
    }
}
