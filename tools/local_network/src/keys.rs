use std::fs;

use crate::constants::{
    MANAGEMENT_PRIVATE_KEYS_PATH, MANAGEMENT_PUBLIC_KEYS_PATH, PRIVATE_KEYS_PATH, PUBLIC_KEYS_PATH,
};

pub(crate) struct NodeKeys {
    pub(crate) private_keys: Vec<String>,
    pub(crate) public_keys: Vec<String>,
    pub(crate) management_private_keys: Vec<String>,
    pub(crate) management_public_keys: Vec<String>,
}

impl NodeKeys {
    pub(crate) fn load() -> Self {
        Self {
            private_keys: load_keys(PRIVATE_KEYS_PATH),
            public_keys: load_keys(PUBLIC_KEYS_PATH),
            management_private_keys: load_keys(MANAGEMENT_PRIVATE_KEYS_PATH),
            management_public_keys: load_keys(MANAGEMENT_PUBLIC_KEYS_PATH),
        }
    }

    pub(crate) fn validate(&self, total_nodes: usize) {
        let required_len = total_nodes + 1;
        validate_len(
            "operational private keys",
            self.private_keys.len(),
            required_len,
        );
        validate_len(
            "operational public keys",
            self.public_keys.len(),
            required_len,
        );
        validate_len(
            "management private keys",
            self.management_private_keys.len(),
            required_len,
        );
        validate_len(
            "management public keys",
            self.management_public_keys.len(),
            required_len,
        );
    }
}

fn load_keys(path: &str) -> Vec<String> {
    serde_json::from_str(&fs::read_to_string(path).expect("Failed to read key file"))
        .expect("Failed to parse key file")
}

fn validate_len(label: &str, actual: usize, required: usize) {
    if actual < required {
        panic!(
            "Not enough {}: need at least {}, found {}",
            label, required, actual
        );
    }
}
