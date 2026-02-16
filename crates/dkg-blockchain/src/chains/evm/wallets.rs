use alloy::{
    network::EthereumWallet,
    signers::local::{LocalSignerError, PrivateKeySigner},
};

use crate::error::BlockchainError;

pub fn signer_from_private_key(private_key: &str) -> Result<PrivateKeySigner, BlockchainError> {
    private_key
        .parse()
        .map_err(|e: LocalSignerError| BlockchainError::InvalidPrivateKey {
            key_length: private_key.len(),
            source: e,
        })
}

pub fn wallet_from_private_key(private_key: &str) -> Result<EthereumWallet, BlockchainError> {
    Ok(EthereumWallet::from(signer_from_private_key(private_key)?))
}
