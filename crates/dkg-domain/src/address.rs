use alloy::primitives::Address;

/// Canonical EVM address string representation used for persistence keys and UALs.
///
/// Format: `0x`-prefixed, lowercase hex.
#[must_use]
pub fn canonical_evm_address(address: &Address) -> String {
    format!("{:#x}", address)
}
