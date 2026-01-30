//! Custom validation functions for HTTP API DTOs.
//!
//! These validators are used with the `validator` crate's `#[validate(custom)]` attribute.

use validator::ValidationError;

use crate::types::parse_ual;

/// Validates that a string is a valid UAL format.
///
/// UAL format: `did:dkg:{blockchain}/{contract}/{collection_id}[/{asset_id}]`
///
/// Examples:
/// - `did:dkg:base:84532/0x6C1AeF3601cd0e04cD5e8E70e7ea2c11D2eF60f4/123`
/// - `did:dkg:base:84532/0x6C1AeF3601cd0e04cD5e8E70e7ea2c11D2eF60f4/123/1`
pub(crate) fn validate_ual_format(ual: &str) -> Result<(), ValidationError> {
    if ual.trim().is_empty() {
        let mut error = ValidationError::new("ual_empty");
        error.message = Some("UAL cannot be empty".into());
        return Err(error);
    }

    parse_ual(ual).map_err(|e| {
        let mut error = ValidationError::new("ual_format");
        error.message = Some(format!("invalid UAL format: {}", e).into());
        error
    })?;

    Ok(())
}

/// Validates an optional UAL field - only validates format if present and non-empty.
/// Note: validator crate passes inner value for Option<T>, so we receive &String not
/// &Option<String>
pub(crate) fn validate_optional_ual(ual: &str) -> Result<(), ValidationError> {
    if !ual.trim().is_empty() {
        return validate_ual_format(ual);
    }
    Ok(())
}

/// Validates BlockchainId format: `type:chain_id` (e.g., "hardhat1:31337", "base:8453")
pub(crate) fn validate_blockchain_id_format(id: &str) -> Result<(), ValidationError> {
    if !id.contains(':') {
        let mut error = ValidationError::new("blockchain_id_format");
        error.message =
            Some("blockchain must have format 'type:chain_id' (e.g., 'base:8453')".into());
        return Err(error);
    }

    let parts: Vec<&str> = id.split(':').collect();
    if parts.len() != 2 {
        let mut error = ValidationError::new("blockchain_id_format");
        error.message = Some("blockchain must have exactly one colon separator".into());
        return Err(error);
    }

    if parts[0].is_empty() {
        let mut error = ValidationError::new("blockchain_id_format");
        error.message = Some("blockchain type cannot be empty".into());
        return Err(error);
    }

    if parts[1].parse::<u64>().is_err() {
        let mut error = ValidationError::new("blockchain_id_format");
        error.message = Some("chain ID must be a valid number".into());
        return Err(error);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_ual_format_valid_collection() {
        let ual = "did:dkg:base:84532/0x6C1AeF3601cd0e04cD5e8E70e7ea2c11D2eF60f4/123";
        assert!(validate_ual_format(ual).is_ok());
    }

    #[test]
    fn test_validate_ual_format_valid_asset() {
        let ual = "did:dkg:base:84532/0x6C1AeF3601cd0e04cD5e8E70e7ea2c11D2eF60f4/123/1";
        assert!(validate_ual_format(ual).is_ok());
    }

    #[test]
    fn test_validate_ual_format_empty() {
        assert!(validate_ual_format("").is_err());
        assert!(validate_ual_format("   ").is_err());
    }

    #[test]
    fn test_validate_ual_format_invalid() {
        // Missing prefix handled by parse_ual
        assert!(validate_ual_format("invalid").is_err());
        // Too few parts
        assert!(validate_ual_format("did:dkg:base:84532/0x123").is_err());
    }

    #[test]
    fn test_validate_optional_ual_empty() {
        // Empty strings are allowed (optional field)
        assert!(validate_optional_ual("").is_ok());
        assert!(validate_optional_ual("   ").is_ok());
    }

    #[test]
    fn test_validate_optional_ual_valid() {
        let ual = "did:dkg:base:84532/0x6C1AeF3601cd0e04cD5e8E70e7ea2c11D2eF60f4/123".to_string();
        assert!(validate_optional_ual(&ual).is_ok());
    }

    #[test]
    fn test_validate_optional_ual_invalid() {
        assert!(validate_optional_ual("invalid").is_err());
    }

    #[test]
    fn test_validate_blockchain_id_format_valid() {
        assert!(validate_blockchain_id_format("hardhat1:31337").is_ok());
        assert!(validate_blockchain_id_format("base:8453").is_ok());
        assert!(validate_blockchain_id_format("otp:2043").is_ok());
        assert!(validate_blockchain_id_format("gnosis:100").is_ok());
    }

    #[test]
    fn test_validate_blockchain_id_format_invalid() {
        // No colon
        assert!(validate_blockchain_id_format("invalid").is_err());
        // Empty type
        assert!(validate_blockchain_id_format(":31337").is_err());
        // Non-numeric chain ID
        assert!(validate_blockchain_id_format("otp:abc").is_err());
        // Multiple colons
        assert!(validate_blockchain_id_format("base:8453:extra").is_err());
    }
}
