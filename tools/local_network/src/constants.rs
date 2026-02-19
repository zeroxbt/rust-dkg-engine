use std::path::{Path, PathBuf};

pub(crate) const DEFAULT_NODES: usize = 12;
pub(crate) const HTTP_PORT_BASE: usize = 8900;
pub(crate) const NETWORK_PORT_BASE: usize = 9100;
pub(crate) const HARDHAT_PORT: u16 = 8545;
pub(crate) const HARDHAT_BLOCKCHAIN_ID: &str = "hardhat1:31337";
pub(crate) const BOOTSTRAP_NODE_INDEX: usize = 0;
pub(crate) const BOOTSTRAP_KEY_PATH: &str = "network/private_key";
pub(crate) const BINARY_NAME: &str = "rust-dkg-engine";

pub(crate) const PRIVATE_KEYS_PATH: &str = "./tools/local_network/src/private_keys.json";
pub(crate) const PUBLIC_KEYS_PATH: &str = "./tools/local_network/src/public_keys.json";
pub(crate) const MANAGEMENT_PRIVATE_KEYS_PATH: &str =
    "./tools/local_network/src/management_private_keys.json";

pub(crate) fn resolve_repo_relative_candidates(path: &str) -> Vec<PathBuf> {
    let raw = Path::new(path);
    if raw.is_absolute() {
        return vec![raw.to_path_buf()];
    }

    let mut candidates = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join(raw));
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    candidates.push(manifest_dir.join(raw));

    if let Some(workspace_root) = manifest_dir.parent().and_then(Path::parent) {
        candidates.push(workspace_root.join(raw));
    }

    candidates
}

pub(crate) fn resolve_repo_relative_path(path: &str) -> Option<PathBuf> {
    resolve_repo_relative_candidates(path)
        .into_iter()
        .find(|candidate| candidate.exists())
}
