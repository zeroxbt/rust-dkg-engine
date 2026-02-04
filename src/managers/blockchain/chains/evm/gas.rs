use alloy::{
    primitives::{Address, U256},
    providers::Provider,
};

use crate::managers::blockchain::{
    chains::evm::EvmChain, error::BlockchainError, gas::fetch_gas_price_from_oracle,
};

/// Format a balance in wei to a human-readable string with the specified decimals.
pub(crate) fn format_balance(wei: U256, decimals: u8) -> String {
    let divisor = U256::from(10u64).pow(U256::from(decimals));

    if wei.is_zero() {
        return "0".to_string();
    }

    let whole = wei / divisor;
    let fraction = wei % divisor;

    if fraction.is_zero() {
        whole.to_string()
    } else {
        let fraction_str = format!("{:0>width$}", fraction, width = decimals as usize);
        let trimmed = fraction_str.trim_end_matches('0');
        format!("{}.{}", whole, trimmed)
    }
}

impl EvmChain {
    /// Get the native token balance formatted with proper decimals.
    ///
    /// NeuroWeb uses 12 decimals while most other chains use 18.
    pub(crate) async fn get_native_token_balance(
        &self,
        address: Address,
    ) -> Result<String, BlockchainError> {
        let balance = self
            .rpc_call(|| async {
                let provider = self.provider().await;
                provider.get_balance(address).await
            })
            .await
            .map_err(|e| BlockchainError::Custom(format!("Failed to get balance: {}", e)))?;

        Ok(format_balance(balance, self.native_token_decimals))
    }

    /// Get the current gas price.
    ///
    /// Tries sources in order:
    /// 1. Gas price oracle (if configured) - only used if price > default
    /// 2. Provider's gas price estimate - only used if price >= default
    /// 3. Default gas price from config
    ///
    /// This matches the JS implementation behavior where:
    /// - Gnosis validates oracle price > default before using it
    /// - Provider price must be >= default to be used
    pub(crate) async fn get_gas_price(&self) -> U256 {
        let default_price = self.gas_config.default_gas_price;

        // Try oracle first if configured
        if let Some(oracle_url) = self.config.gas_price_oracle_url() {
            match fetch_gas_price_from_oracle(oracle_url).await {
                Ok(price) => {
                    tracing::debug!("Gas price from oracle: {} wei", price);
                    // JS behavior: only use oracle price if > default (uses .gt(), not .gte())
                    // See gnosis-service.js: gasPrice.gt(this.defaultGasPrice)
                    if price > default_price {
                        return price;
                    }
                    tracing::debug!(
                        "Oracle price {} <= default {}, using default",
                        price,
                        default_price
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to fetch gas price from oracle: {}", e);
                }
            }
        }

        // Try provider's gas price estimate
        match self
            .rpc_call(|| async {
                let provider = self.provider().await;
                provider.get_gas_price().await
            })
            .await
        {
            Ok(price) => {
                let price = U256::from(price);
                tracing::debug!("Gas price from provider: {} wei", price);
                // Ensure we don't return less than the default
                if price >= default_price {
                    return price;
                }
                tracing::debug!(
                    "Provider price {} < default {}, using default",
                    price,
                    default_price
                );
            }
            Err(e) => {
                tracing::warn!("Failed to get gas price from provider: {}", e);
            }
        }

        // Fall back to default
        tracing::debug!("Using default gas price: {} wei", default_price);
        default_price
    }
}
