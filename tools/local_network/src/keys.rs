use std::fs;

use alloy::signers::local::PrivateKeySigner;

use crate::constants::{
    MANAGEMENT_PRIVATE_KEYS_PATH, PRIVATE_KEYS_PATH, PUBLIC_KEYS_PATH,
    resolve_repo_relative_candidates, resolve_repo_relative_path,
};

pub(crate) struct NodeKeys {
    pub(crate) private_keys: Vec<String>,
    pub(crate) public_keys: Vec<String>,
    pub(crate) management_private_keys: Vec<String>,
    pub(crate) management_public_keys: Vec<String>,
}

impl NodeKeys {
    pub(crate) fn load() -> Self {
        let management_private_keys = load_keys(MANAGEMENT_PRIVATE_KEYS_PATH);
        let management_public_keys = derive_public_keys(&management_private_keys);

        Self {
            private_keys: load_keys(PRIVATE_KEYS_PATH),
            public_keys: load_keys(PUBLIC_KEYS_PATH),
            management_private_keys,
            management_public_keys,
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
    let resolved = resolve_repo_relative_path(path).unwrap_or_else(|| {
        let searched = resolve_repo_relative_candidates(path)
            .into_iter()
            .map(|candidate| candidate.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        panic!("Failed to read key file '{path}': file not found (searched: {searched})");
    });

    let file = fs::read_to_string(&resolved).unwrap_or_else(|error| {
        panic!("Failed to read key file '{}': {error}", resolved.display())
    });
    serde_json::from_str(&file).unwrap_or_else(|error| {
        panic!("Failed to parse key file '{}': {error}", resolved.display())
    })
}

fn validate_len(label: &str, actual: usize, required: usize) {
    if actual < required {
        panic!(
            "Not enough {}: need at least {}, found {}",
            label, required, actual
        );
    }
}

fn derive_public_keys(private_keys: &[String]) -> Vec<String> {
    private_keys
        .iter()
        .map(|private_key| {
            let signer: PrivateKeySigner = private_key.parse().unwrap_or_else(|error| {
                panic!("Invalid management private key '{}': {error}", private_key)
            });
            signer.address().to_checksum(None)
        })
        .collect()
}
