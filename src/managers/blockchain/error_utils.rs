use alloy::{
    contract::Error as ContractError, primitives::Bytes, providers::PendingTransactionBuilder,
    rpc::types::TransactionReceipt,
};

use crate::managers::blockchain::error::BlockchainError;

/// Handle contract call result, waiting for receipt.
pub(crate) async fn handle_contract_call(
    result: Result<PendingTransactionBuilder<alloy::network::Ethereum>, ContractError>,
) -> Result<TransactionReceipt, BlockchainError> {
    match result {
        Ok(pending_tx) => {
            let receipt = pending_tx.get_receipt().await;
            match receipt {
                Ok(r) => Ok(r),
                Err(err) => {
                    tracing::error!("Failed to retrieve transaction receipt: {:?}", err);
                    Err(BlockchainError::ReceiptFailed {
                        reason: err.to_string(),
                    })
                }
            }
        }
        Err(err) => {
            tracing::error!("Contract call failed: {:?}", err);
            Err(BlockchainError::Contract(err))
        }
    }
}

/// Selector for standard Solidity Error(string) revert.
const REVERT_SELECTOR: [u8; 4] = [0x08, 0xc3, 0x79, 0xa0];

/// Selector for Solidity Panic(uint256).
const PANIC_SELECTOR: [u8; 4] = [0x4e, 0x48, 0x7b, 0x71];

/// Decode standard Solidity revert string (Error(string)).
///
/// Returns the revert message if the data starts with the Error(string) selector.
pub(crate) fn decode_revert_string(data: &Bytes) -> Option<String> {
    if data.len() < 68 || data[..4] != REVERT_SELECTOR {
        return None;
    }

    // ABI-encoded string: offset (32 bytes) + length (32 bytes) + data
    // The string starts at byte 4 (after selector)
    // Offset is at bytes 4..36, should be 32 (0x20)
    // Length is at bytes 36..68
    // String data starts at byte 68

    let length_bytes: [u8; 32] = data[36..68].try_into().ok()?;
    let length = u64::from_be_bytes(length_bytes[24..32].try_into().ok()?) as usize;

    if data.len() < 68 + length {
        return None;
    }

    String::from_utf8(data[68..68 + length].to_vec()).ok()
}

/// Decode Solidity panic code to a human-readable message.
///
/// Returns a static string describing the panic if the data matches the Panic(uint256) selector.
pub(crate) fn decode_panic(data: &Bytes) -> Option<&'static str> {
    if data.len() < 36 || data[..4] != PANIC_SELECTOR {
        return None;
    }

    // Panic code is a uint256, but meaningful codes fit in a single byte
    let code = data[35];
    Some(match code {
        0x01 => "Assertion failed",
        0x11 => "Arithmetic overflow/underflow",
        0x12 => "Division by zero",
        0x21 => "Invalid enum value",
        0x22 => "Storage encoding error",
        0x31 => "Pop on empty array",
        0x32 => "Array index out of bounds",
        0x41 => "Too much memory allocated",
        0x51 => "Zero internal function called",
        _ => "Unknown panic code",
    })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::bytes;

    use super::*;

    #[test]
    fn test_decode_revert_string() {
        // "Error(string)" with message "test error"
        // Selector: 0x08c379a0
        // Offset: 0x20 (32)
        // Length: 0x0a (10)
        // Data: "test error"
        let data = bytes!(
            "08c379a0"  // selector
            "0000000000000000000000000000000000000000000000000000000000000020"  // offset
            "000000000000000000000000000000000000000000000000000000000000000a"  // length
            "74657374206572726f7200000000000000000000000000000000000000000000"  // "test error" padded
        );

        assert_eq!(decode_revert_string(&data), Some("test error".to_string()));
    }

    #[test]
    fn test_decode_panic_overflow() {
        // Panic(uint256) with code 0x11 (arithmetic overflow)
        let data = bytes!(
            "4e487b71"  // selector
            "0000000000000000000000000000000000000000000000000000000000000011"  // code
        );

        assert_eq!(decode_panic(&data), Some("Arithmetic overflow/underflow"));
    }

    #[test]
    fn test_decode_panic_division_by_zero() {
        let data = bytes!(
            "4e487b71"
            "0000000000000000000000000000000000000000000000000000000000000012"
        );

        assert_eq!(decode_panic(&data), Some("Division by zero"));
    }

    #[test]
    fn test_invalid_data_returns_none() {
        let data = bytes!("deadbeef");
        assert_eq!(decode_revert_string(&data), None);
        assert_eq!(decode_panic(&data), None);
    }
}
