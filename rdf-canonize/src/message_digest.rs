use ethers::{
    core::k256::sha2::{Digest, Sha256},
    utils::hex,
};

/// Provides a mechanism for hashing data using the SHA-256 hashing algorithm.
///
/// This struct encapsulates the `Sha256` hasher from the `sha2` crate and provides
/// a simple interface for updating the hash state and finalizing the hash.
pub struct MessageDigest {
    hasher: Sha256,
}

impl MessageDigest {
    /// Creates a new `MessageDigest`.
    pub fn new() -> MessageDigest {
        MessageDigest {
            hasher: Sha256::new(),
        }
    }

    /// Updates the hasher with a string slice.
    pub fn update(&mut self, msg: &str) {
        self.hasher.update(msg.as_bytes());
    }

    /// Finalizes the hashing process and returns the resulting hex-encoded string.
    pub fn digest(&mut self) -> String {
        hex::encode(self.hasher.clone().finalize())
    }
}
