//! Gas price management for EVM transactions.
//!
//! Supports:
//! - Gas price oracle fetching (configurable URL)
//! - Blockchain-specific default gas prices
//! - Gas price bumping for transaction retries

use alloy::primitives::U256;
use serde::Deserialize;

/// Gas price configuration for a blockchain.
#[derive(Debug, Clone)]
pub struct GasConfig {
    /// URL for gas price oracle (optional).
    pub oracle_url: Option<String>,
    /// Default gas price in wei if oracle fails.
    pub default_gas_price: U256,
    /// Multiplier for gas price bumps on retry (e.g., 1.2 = 20% increase).
    pub bump_factor: f64,
    /// Maximum gas price in wei (cap for bumping).
    pub max_gas_price: U256,
}

impl Default for GasConfig {
    fn default() -> Self {
        Self {
            oracle_url: None,
            // Default: 1 gwei
            default_gas_price: U256::from(1_000_000_000u64),
            bump_factor: 1.2,
            // Default max: 500 gwei
            max_gas_price: U256::from(500_000_000_000u64),
        }
    }
}

impl GasConfig {
    /// Create a new gas config for Hardhat (local testing).
    pub fn hardhat() -> Self {
        Self {
            oracle_url: None,
            default_gas_price: U256::from(20u64), // 20 wei for hardhat
            bump_factor: 1.2,
            max_gas_price: U256::from(1_000_000_000u64), // 1 gwei max for testing
        }
    }

    /// Create a new gas config for Gnosis chain.
    pub fn gnosis(oracle_url: Option<String>) -> Self {
        Self {
            oracle_url,
            default_gas_price: U256::from(1_000_000_000u64), // 1 gwei
            bump_factor: 1.2,
            max_gas_price: U256::from(100_000_000_000u64), // 100 gwei
        }
    }

    /// Create a new gas config for NeuroWeb (OT Parachain).
    pub fn neuroweb(oracle_url: Option<String>) -> Self {
        Self {
            oracle_url,
            default_gas_price: U256::from(8u64), // 8 wei
            bump_factor: 1.2,
            max_gas_price: U256::from(1_000_000_000u64), // 1 gwei
        }
    }

    /// Create a new gas config for Base (Coinbase L2).
    ///
    /// Base uses the provider's gas price estimate (no oracle).
    /// JS implementation: base-service.js just calls provider.getGasPrice()
    pub fn base(oracle_url: Option<String>) -> Self {
        Self {
            oracle_url,
            default_gas_price: U256::from(1_000_000_000u64), // 1 gwei fallback
            bump_factor: 1.2,
            max_gas_price: U256::from(500_000_000_000u64), // 500 gwei (L2 can spike)
        }
    }

    /// Calculate bumped gas price for retry.
    /// Returns None if the bumped price would exceed max_gas_price.
    pub fn bump_gas_price(&self, current_price: U256) -> Option<U256> {
        // Convert to u128 for floating point math, then back
        let current = current_price.to::<u128>() as f64;
        let bumped = (current * self.bump_factor).ceil() as u128;
        let bumped_price = U256::from(bumped);

        if bumped_price <= self.max_gas_price {
            Some(bumped_price)
        } else {
            None
        }
    }
}

/// Response format for standard gas price oracles.
/// Supports multiple oracle response formats.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum OracleResponse {
    /// Standard format with maxFee in gwei (e.g., EthGasStation-style)
    Standard(StandardOracleResponse),
    /// Blockscout format with average in gwei
    Blockscout(BlockscoutResponse),
    /// Gnosisscan/Etherscan format with result in wei
    Etherscan(EtherscanResponse),
}

#[derive(Debug, Deserialize)]
pub struct StandardOracleResponse {
    pub standard: StandardGasData,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StandardGasData {
    pub max_fee: f64,
}

#[derive(Debug, Deserialize)]
pub struct BlockscoutResponse {
    pub average: f64,
}

#[derive(Debug, Deserialize)]
pub struct EtherscanResponse {
    pub result: String,
}

impl OracleResponse {
    /// Convert oracle response to gas price in wei.
    pub fn to_wei(&self) -> Option<U256> {
        match self {
            OracleResponse::Standard(resp) => {
                // maxFee is in gwei, convert to wei
                let wei = (resp.standard.max_fee * 1e9).round() as u128;
                Some(U256::from(wei))
            }
            OracleResponse::Blockscout(resp) => {
                // average is in gwei, convert to wei
                let wei = (resp.average * 1e9).round() as u128;
                Some(U256::from(wei))
            }
            OracleResponse::Etherscan(resp) => {
                // result is already in wei as a string
                resp.result.parse::<u128>().ok().map(U256::from)
            }
        }
    }
}

/// Fetch gas price from an oracle URL.
pub async fn fetch_gas_price_from_oracle(url: &str) -> Result<U256, GasOracleError> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|e| GasOracleError::HttpClient(e.to_string()))?;

    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| GasOracleError::Request(e.to_string()))?;

    if !response.status().is_success() {
        return Err(GasOracleError::HttpStatus(response.status().as_u16()));
    }

    let oracle_response: OracleResponse = response
        .json()
        .await
        .map_err(|e| GasOracleError::ParseResponse(e.to_string()))?;

    oracle_response
        .to_wei()
        .ok_or(GasOracleError::InvalidResponse)
}

/// Errors that can occur when fetching gas prices from an oracle.
#[derive(Debug, thiserror::Error)]
pub enum GasOracleError {
    #[error("Failed to create HTTP client: {0}")]
    HttpClient(String),

    #[error("Request failed: {0}")]
    Request(String),

    #[error("HTTP error status: {0}")]
    HttpStatus(u16),

    #[error("Failed to parse response: {0}")]
    ParseResponse(String),

    #[error("Invalid response format")]
    InvalidResponse,
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // GasConfig Default Values Tests
    // These tests verify our Rust implementation matches the JS implementation's
    // default gas prices for each blockchain.
    // =========================================================================

    #[test]
    fn test_hardhat_defaults_match_js() {
        // JS: ethers.utils.parseUnits('20', 'wei') = 20 wei
        let config = GasConfig::hardhat();
        assert_eq!(
            config.default_gas_price,
            U256::from(20u64),
            "Hardhat default should be 20 wei (matching JS hardhat-service.js)"
        );
    }

    #[test]
    fn test_gnosis_defaults_match_js() {
        // JS: GNOSIS_DEFAULT_GAS_PRICE = { TESTNET: 1, MAINNET: 1 } (in gwei)
        // JS: ethers.utils.parseUnits('1', 'gwei') = 1_000_000_000 wei
        let config = GasConfig::gnosis(None);
        assert_eq!(
            config.default_gas_price,
            U256::from(1_000_000_000u64),
            "Gnosis default should be 1 gwei = 1,000,000,000 wei (matching JS constants.js)"
        );
    }

    #[test]
    fn test_neuroweb_defaults_match_js() {
        // JS: NEURO_DEFAULT_GAS_PRICE = { TESTNET: 8, MAINNET: 8 } (in wei)
        // JS: ethers.utils.parseUnits('8', 'wei') = 8 wei
        let config = GasConfig::neuroweb(None);
        assert_eq!(
            config.default_gas_price,
            U256::from(8u64),
            "NeuroWeb default should be 8 wei (matching JS constants.js)"
        );
    }

    // =========================================================================
    // Gas Price Bumping Tests
    // JS uses 1.2x multiplier for nonce error retries (web3-service.js line 630)
    // =========================================================================

    #[test]
    fn test_bump_gas_price() {
        let config = GasConfig {
            oracle_url: None,
            default_gas_price: U256::from(1_000_000_000u64),
            bump_factor: 1.2,
            max_gas_price: U256::from(2_000_000_000u64),
        };

        // 1 gwei bumped by 20% = 1.2 gwei
        // JS: Math.ceil(gasPrice * 1.2)
        let bumped = config.bump_gas_price(U256::from(1_000_000_000u64));
        assert_eq!(bumped, Some(U256::from(1_200_000_000u64)));

        // 1.8 gwei bumped would be 2.16 gwei, exceeds max
        let bumped = config.bump_gas_price(U256::from(1_800_000_000u64));
        assert_eq!(bumped, None);
    }

    #[test]
    fn test_bump_factor_matches_js() {
        // JS: gasPrice = Math.ceil(gasPrice * 1.2) in web3-service.js
        let config = GasConfig::gnosis(None);
        assert_eq!(
            config.bump_factor, 1.2,
            "Bump factor should be 1.2 (20% increase)"
        );

        // Test the actual calculation matches JS behavior
        // JS: Math.ceil(1000000000 * 1.2) = Math.ceil(1200000000) = 1200000000
        let original = U256::from(1_000_000_000u64);
        let bumped = config.bump_gas_price(original).unwrap();
        assert_eq!(
            bumped,
            U256::from(1_200_000_000u64),
            "1 gwei * 1.2 should equal 1.2 gwei"
        );
    }

    // =========================================================================
    // Oracle Response Parsing Tests
    // These test the different oracle response formats used by different chains.
    // =========================================================================

    #[test]
    fn test_oracle_response_parsing() {
        // Standard format (used by Web3Service base class)
        // JS: Math.round(response.data.standard.maxFee * 1e9)
        let json = r#"{"standard": {"maxFee": 50.5}}"#;
        let resp: OracleResponse = serde_json::from_str(json).unwrap();
        let wei = resp.to_wei().unwrap();
        assert_eq!(wei, U256::from(50_500_000_000u64));

        // Blockscout format (used by Gnosis)
        // JS: ethers.utils.parseUnits(gasPrice.toString(), 'gwei')
        let json = r#"{"average": 1.5}"#;
        let resp: OracleResponse = serde_json::from_str(json).unwrap();
        let wei = resp.to_wei().unwrap();
        assert_eq!(wei, U256::from(1_500_000_000u64));

        // Etherscan format (result is already in wei)
        // JS: Number(response.data.result, 10)
        let json = r#"{"result": "1000000000"}"#;
        let resp: OracleResponse = serde_json::from_str(json).unwrap();
        let wei = resp.to_wei().unwrap();
        assert_eq!(wei, U256::from(1_000_000_000u64));
    }

    #[test]
    fn test_blockscout_gnosis_mainnet_format() {
        // Actual response from https://gnosis.blockscout.com/api/v1/gas-price-oracle
        // {"slow": 0.11, "average": 0.52, "fast": 3.92}
        let json = r#"{"slow": 0.11, "average": 0.52, "fast": 3.92}"#;
        let resp: OracleResponse = serde_json::from_str(json).unwrap();
        let wei = resp.to_wei().unwrap();
        // 0.52 gwei = 520,000,000 wei
        assert_eq!(
            wei,
            U256::from(520_000_000u64),
            "Blockscout 0.52 gwei should parse to 520,000,000 wei"
        );
    }

    #[test]
    fn test_blockscout_low_gas_price() {
        // Test case: oracle returns price lower than default
        // JS behavior: only use oracle price if > default
        // This test documents the oracle parsing, not the validation logic
        let json = r#"{"average": 0.1}"#;
        let resp: OracleResponse = serde_json::from_str(json).unwrap();
        let wei = resp.to_wei().unwrap();
        // 0.1 gwei = 100,000,000 wei (less than 1 gwei default)
        assert_eq!(wei, U256::from(100_000_000u64));
    }

    // =========================================================================
    // Gas Price Validation Tests
    // JS Gnosis service validates: only use oracle price if > default
    // =========================================================================

    #[test]
    fn test_oracle_price_validation_logic() {
        // This test documents the JS gnosis-service.js validation behavior:
        // if (gasPrice && ethers.utils.parseUnits(gasPrice.toString(),
        // 'gwei').gt(this.defaultGasPrice))     return gasPrice;
        // return this.defaultGasPrice;

        let config = GasConfig::gnosis(None);
        let default = config.default_gas_price; // 1 gwei = 1,000,000,000 wei

        // Case 1: Oracle returns 0.52 gwei (from actual mainnet response)
        let oracle_price = U256::from(520_000_000u64);
        // JS behavior: 520,000,000 < 1,000,000,000, so use default
        assert!(
            oracle_price < default,
            "0.52 gwei oracle price should be less than 1 gwei default"
        );

        // Case 2: Oracle returns 2.5 gwei
        let oracle_price_high = U256::from(2_500_000_000u64);
        // JS behavior: 2,500,000,000 > 1,000,000,000, so use oracle
        assert!(
            oracle_price_high > default,
            "2.5 gwei oracle price should be greater than 1 gwei default"
        );

        // Case 3: Oracle returns exactly 1 gwei (equal to default)
        let oracle_price_equal = U256::from(1_000_000_000u64);
        // JS behavior: uses .gt() (greater than), not .gte() (greater than or equal)
        // So equal values should still use default
        assert!(
            !(oracle_price_equal > default),
            "1 gwei oracle price should NOT be greater than 1 gwei default (JS uses .gt())"
        );
    }

    // =========================================================================
    // Blockchain-specific Configuration Tests
    // =========================================================================

    #[test]
    fn test_config_presets_complete() {
        // Hardhat: no oracle, fixed 20 wei
        let hardhat = GasConfig::hardhat();
        assert!(hardhat.oracle_url.is_none());
        assert_eq!(hardhat.default_gas_price, U256::from(20u64));

        // Gnosis: can have oracle, default 1 gwei
        let gnosis_no_oracle = GasConfig::gnosis(None);
        assert!(gnosis_no_oracle.oracle_url.is_none());
        assert_eq!(
            gnosis_no_oracle.default_gas_price,
            U256::from(1_000_000_000u64)
        );

        let gnosis_with_oracle = GasConfig::gnosis(Some(
            "https://gnosis.blockscout.com/api/v1/gas-price-oracle".into(),
        ));
        assert!(gnosis_with_oracle.oracle_url.is_some());
        assert_eq!(
            gnosis_with_oracle.default_gas_price,
            U256::from(1_000_000_000u64)
        );

        // NeuroWeb: can have oracle, default 8 wei
        let neuro = GasConfig::neuroweb(None);
        assert!(neuro.oracle_url.is_none());
        assert_eq!(neuro.default_gas_price, U256::from(8u64));

        // Base: can have oracle, default 1 gwei (provider-based)
        let base_no_oracle = GasConfig::base(None);
        assert!(base_no_oracle.oracle_url.is_none());
        assert_eq!(
            base_no_oracle.default_gas_price,
            U256::from(1_000_000_000u64)
        );

        let base_with_oracle = GasConfig::base(Some("https://example.com/gas".into()));
        assert!(base_with_oracle.oracle_url.is_some());
    }

    #[test]
    fn test_max_gas_price_caps() {
        // Hardhat: low max (1 gwei) since it's for testing
        let hardhat = GasConfig::hardhat();
        assert_eq!(hardhat.max_gas_price, U256::from(1_000_000_000u64));

        // Gnosis: higher max (100 gwei) for mainnet
        let gnosis = GasConfig::gnosis(None);
        assert_eq!(gnosis.max_gas_price, U256::from(100_000_000_000u64));

        // NeuroWeb: low max (1 gwei) since gas is cheap
        let neuro = GasConfig::neuroweb(None);
        assert_eq!(neuro.max_gas_price, U256::from(1_000_000_000u64));

        // Base: high max (500 gwei) since L2 gas can spike
        let base = GasConfig::base(None);
        assert_eq!(base.max_gas_price, U256::from(500_000_000_000u64));
    }

    #[test]
    fn test_base_defaults_match_js() {
        // JS base-service.js: uses provider.getGasPrice() with no special defaults
        // We use 1 gwei as a sensible fallback
        let config = GasConfig::base(None);
        assert_eq!(
            config.default_gas_price,
            U256::from(1_000_000_000u64),
            "Base default should be 1 gwei fallback"
        );
        assert_eq!(config.bump_factor, 1.2, "Bump factor should be 1.2");
    }
}
