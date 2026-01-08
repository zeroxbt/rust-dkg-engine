use crate::{
    blockchains::{abstract_blockchain::EventLog, blockchain_creator::BlockchainProvider},
    error::BlockchainError,
};
use ethers::{
    abi::{Address, EncodePackedError, Token},
    contract::{parse_log, EthLogDecode},
    prelude::{ContractError, TransactionReceipt},
    providers::{Http, PendingTransaction},
    types::U256,
};
use hex::FromHexError;

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
