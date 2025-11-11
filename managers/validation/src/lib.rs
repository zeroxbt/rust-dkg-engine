use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Deserialize)]
pub enum HashFunction {
    Sha256,
}

impl HashFunction {
    pub fn to_id(self) -> u8 {
        match self {
            HashFunction::Sha256 => 1,
        }
    }

    pub fn from_id(id: u8) -> Self {
        match id {
            1 => HashFunction::Sha256,
            x => panic!("Hash function with id: {} not supported", x),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ValidationManagerConfig {}

pub struct ValidationManager;

impl ValidationManager {
    pub async fn new() -> Self {
        Self
    }

    pub fn call_hash_function(&self, hash_function: &HashFunction, data: Vec<u8>) -> Vec<u8> {
        match hash_function {
            HashFunction::Sha256 => self.sha256(data),
        }
    }

    fn sha256(&self, data: Vec<u8>) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}
