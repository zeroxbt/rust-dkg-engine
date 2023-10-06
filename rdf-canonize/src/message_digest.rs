use sha2::{Digest, Sha256};

pub struct MessageDigest {
    hasher: Sha256,
}

impl MessageDigest {
    pub fn new() -> MessageDigest {
        MessageDigest {
            hasher: Sha256::new(),
        }
    }

    pub fn update(&mut self, msg: &str) {
        self.hasher.update(msg.as_bytes());
    }

    pub fn digest(&mut self) -> String {
        hex::encode(self.hasher.clone().finalize())
    }
}
