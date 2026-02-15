//! EVM fee management for transactions.
//!
//! Supports:
//! - EIP-1559 fee estimation (max fee + priority fee) when RPC supports it
//! - Legacy gas price fallback when EIP-1559 is unsupported or fails
//! - Short TTL caching of fee quotes
//! - Fee bumping for transaction retries

use alloy::{primitives::U256, providers::Provider};

use super::EvmChain;

// ─────────────────────────────────────────────────────────────────────────────
// Chain defaults (caps, floors, retry bump factor)
// ─────────────────────────────────────────────────────────────────────────────

/// Gas price configuration for a blockchain.
#[derive(Debug, Clone)]
pub(crate) struct GasConfig {
    /// Default legacy gas price in wei (floor for provider values).
    pub default_gas_price: U256,
    /// Multiplier for fee bumps on retry (e.g., 1.2 = 20% increase).
    pub bump_factor: f64,
    /// Maximum fee in wei (cap for legacy gasPrice and EIP-1559 maxFeePerGas).
    pub max_gas_price: U256,
}

#[cfg(test)]
impl Default for GasConfig {
    fn default() -> Self {
        Self {
            // Default: 1 gwei
            default_gas_price: U256::from(1_000_000_000u64),
            bump_factor: 1.2,
            // Default max: 500 gwei
            max_gas_price: U256::from(500_000_000_000u64),
        }
    }
}

impl GasConfig {
    pub(crate) fn hardhat() -> Self {
        Self {
            default_gas_price: U256::from(20u64), // 20 wei for hardhat
            bump_factor: 1.2,
            max_gas_price: U256::from(1_000_000_000u64), // 1 gwei max for testing
        }
    }

    pub(crate) fn gnosis() -> Self {
        Self {
            default_gas_price: U256::from(1_000_000_000u64), // 1 gwei
            bump_factor: 1.2,
            max_gas_price: U256::from(100_000_000_000u64), // 100 gwei
        }
    }

    pub(crate) fn neuroweb() -> Self {
        Self {
            default_gas_price: U256::from(8u64), // 8 wei
            bump_factor: 1.2,
            max_gas_price: U256::from(1_000_000_000u64), // 1 gwei
        }
    }

    pub(crate) fn base() -> Self {
        Self {
            default_gas_price: U256::from(1_000_000_000u64), // 1 gwei fallback
            bump_factor: 1.2,
            max_gas_price: U256::from(500_000_000_000u64), // 500 gwei (L2 can spike)
        }
    }

    pub(crate) fn bump_wei(&self, current: U256) -> Option<U256> {
        let current = current.to::<u128>() as f64;
        let bumped = (current * self.bump_factor).ceil() as u128;
        let bumped = U256::from(bumped);
        (bumped <= self.max_gas_price).then_some(bumped)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Fee quoting (legacy vs EIP-1559)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FeeSource {
    Provider,
    Default,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FeeQuote {
    Legacy {
        gas_price: U256,
        source: FeeSource,
    },
    Eip1559 {
        max_fee_per_gas: U256,
        max_priority_fee_per_gas: U256,
        source: FeeSource,
    },
}

impl FeeQuote {
    pub(crate) fn bump(&self, gas_config: &GasConfig) -> Option<Self> {
        match self {
            FeeQuote::Legacy { gas_price, source } => {
                gas_config
                    .bump_wei(*gas_price)
                    .map(|gas_price| FeeQuote::Legacy {
                        gas_price,
                        source: *source,
                    })
            }
            FeeQuote::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
                source,
            } => {
                let bumped_max = gas_config.bump_wei(*max_fee_per_gas)?;
                let bumped_tip = gas_config.bump_wei(*max_priority_fee_per_gas)?;
                let max_priority_fee_per_gas = bumped_tip.min(bumped_max);
                Some(FeeQuote::Eip1559 {
                    max_fee_per_gas: bumped_max,
                    max_priority_fee_per_gas,
                    source: *source,
                })
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EvmChain fee quote (with caching)
// ─────────────────────────────────────────────────────────────────────────────

fn clamp_eip1559(max_fee: U256, max_priority: U256, cap: U256, floor: U256) -> (U256, U256) {
    let max_fee = max_fee.max(floor).min(cap);
    let max_priority = max_priority.min(max_fee);
    (max_fee, max_priority)
}

fn clamp_legacy(gas_price: U256, cap: U256, floor: U256) -> U256 {
    gas_price.max(floor).min(cap)
}

impl EvmChain {
    pub(crate) async fn get_fee_quote(&self) -> FeeQuote {
        match self.try_get_eip1559_fee_quote().await {
            Some(quote) => quote,
            None => self.get_legacy_fee_quote().await,
        }
    }

    async fn try_get_eip1559_fee_quote(&self) -> Option<FeeQuote> {
        let default_floor = self.gas_config.default_gas_price;
        let cap = self.gas_config.max_gas_price;

        let est = match self
            .rpc_call(|| async {
                let provider = self.provider().await;
                provider.estimate_eip1559_fees().await
            })
            .await
        {
            Ok(est) => est,
            Err(e) => {
                tracing::debug!(
                    blockchain = %self.blockchain_id(),
                    error = %e,
                    "EIP-1559 fee estimation failed; falling back to legacy gasPrice"
                );
                return None;
            }
        };

        let mut max_fee = U256::from(est.max_fee_per_gas);
        let mut max_priority = U256::from(est.max_priority_fee_per_gas);
        (max_fee, max_priority) = clamp_eip1559(max_fee, max_priority, cap, default_floor);

        Some(FeeQuote::Eip1559 {
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: max_priority,
            source: FeeSource::Provider,
        })
    }

    async fn get_legacy_fee_quote(&self) -> FeeQuote {
        let default_floor = self.gas_config.default_gas_price;
        let cap = self.gas_config.max_gas_price;

        // Provider gasPrice (only if >= default floor)
        match self
            .rpc_call(|| async {
                let provider = self.provider().await;
                provider.get_gas_price().await
            })
            .await
        {
            Ok(price) => {
                let gas_price = U256::from(price);
                if gas_price >= default_floor {
                    return FeeQuote::Legacy {
                        gas_price: clamp_legacy(gas_price, cap, default_floor),
                        source: FeeSource::Provider,
                    };
                }
                tracing::debug!(
                    blockchain = %self.blockchain_id(),
                    provider_price = %gas_price,
                    default_floor = %default_floor,
                    "Provider gasPrice < default floor; using default"
                );
            }
            Err(e) => {
                tracing::warn!(
                    blockchain = %self.blockchain_id(),
                    error = %e,
                    "Failed to get provider gasPrice; using default"
                );
            }
        }

        FeeQuote::Legacy {
            gas_price: clamp_legacy(default_floor, cap, default_floor),
            source: FeeSource::Default,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn test_bump_legacy_and_eip1559_invariants() {
        let gas_config = GasConfig {
            default_gas_price: U256::from(1u64),
            bump_factor: 1.2,
            max_gas_price: U256::from(200u64),
        };

        let legacy = FeeQuote::Legacy {
            gas_price: U256::from(100u64),
            source: FeeSource::Provider,
        };
        let bumped = legacy.bump(&gas_config).unwrap();
        assert_eq!(
            bumped,
            FeeQuote::Legacy {
                gas_price: U256::from(120u64),
                source: FeeSource::Provider
            }
        );

        let eip = FeeQuote::Eip1559 {
            max_fee_per_gas: U256::from(100u64),
            max_priority_fee_per_gas: U256::from(10u64),
            source: FeeSource::Provider,
        };
        let bumped = eip.bump(&gas_config).unwrap();
        match bumped {
            FeeQuote::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
                ..
            } => {
                assert_eq!(max_fee_per_gas, U256::from(120u64));
                assert_eq!(max_priority_fee_per_gas, U256::from(12u64));
                assert!(max_priority_fee_per_gas <= max_fee_per_gas);
            }
            _ => panic!("expected eip1559"),
        }
    }

    #[test]
    fn test_clamps() {
        let floor = U256::from(10u64);
        let cap = U256::from(100u64);

        assert_eq!(clamp_legacy(U256::from(1u64), cap, floor), floor);
        assert_eq!(
            clamp_legacy(U256::from(50u64), cap, floor),
            U256::from(50u64)
        );
        assert_eq!(clamp_legacy(U256::from(500u64), cap, floor), cap);

        let (mf, tip) = clamp_eip1559(U256::from(1u64), U256::from(999u64), cap, floor);
        assert_eq!(mf, floor);
        assert_eq!(tip, floor);
    }
}
