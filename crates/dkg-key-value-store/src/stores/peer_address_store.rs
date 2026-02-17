use std::collections::HashMap;

use crate::{KeyValueStoreError, KeyValueStoreManager, Table};

const TABLE_NAME: &str = "peer_addresses";
const MAX_PEERS_TO_PERSIST: usize = 500;

pub type PersistedPeerAddresses = HashMap<String, Vec<String>>;

/// Persistent store for peer addresses.
///
/// Saves `peer_id_string -> Vec<multiaddr_string>` to a KVS table so the node can
/// restore known peer addresses on restart without relying solely on
/// bootstrap nodes for discovery.
pub struct PeerAddressStore {
    table: Table<Vec<String>>,
}

impl PeerAddressStore {
    pub fn new(kv_store_manager: &KeyValueStoreManager) -> Result<Self, KeyValueStoreError> {
        let table = kv_store_manager.table(TABLE_NAME)?;
        Ok(Self { table })
    }

    pub async fn load_all(&self) -> PersistedPeerAddresses {
        let entries = match self.table.get_all().await {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to load persisted peer addresses");
                return HashMap::new();
            }
        };

        let mut result = HashMap::new();
        for (key_bytes, addr_strings) in entries {
            let Ok(peer_id) = String::from_utf8(key_bytes) else {
                continue;
            };

            let addrs: Vec<String> = addr_strings
                .into_iter()
                .filter(|addr| !addr.is_empty())
                .collect();

            if !peer_id.is_empty() && !addrs.is_empty() {
                result.insert(peer_id, addrs);
            }
        }

        result
    }

    pub async fn save_all(&self, peers: &PersistedPeerAddresses) {
        if let Err(e) = self.table.clear().await {
            tracing::warn!(error = %e, "Failed to clear peer address table");
            return;
        }

        let mut saved = 0usize;
        for (peer_id, addrs) in peers {
            if saved >= MAX_PEERS_TO_PERSIST {
                break;
            }

            let value: Vec<String> = addrs
                .iter()
                .filter(|addr| !addr.is_empty())
                .cloned()
                .collect();
            if value.is_empty() {
                continue;
            }

            if let Err(e) = self.table.store(peer_id.as_bytes().to_vec(), value).await {
                tracing::warn!(
                    peer_id = %peer_id,
                    error = %e,
                    "Failed to persist addresses for peer"
                );
            } else {
                saved += 1;
            }
        }

        tracing::debug!(saved, total = peers.len(), "Persisted peer addresses");
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use tempfile::TempDir;

    use super::*;
    use crate::KeyValueStoreManagerConfig;

    fn default_config() -> KeyValueStoreManagerConfig {
        KeyValueStoreManagerConfig {
            max_concurrent_operations: 16,
        }
    }

    #[tokio::test]
    async fn test_save_and_load_round_trip() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let kv_store = KeyValueStoreManager::connect(&db_path, &default_config())
            .await
            .unwrap();
        let store = PeerAddressStore::new(&kv_store).unwrap();

        let peer1 = "12D3KooWJ6b8uMzkA4q7hCkC4DKf8YqthpF9q2Mce3rLrFK2dP8V".to_string();
        let peer2 = "12D3KooWLA1M67Yf7dU6jMqZPyfSgh2pJf5ZQvWrcxNf4t8i3QpM".to_string();
        let addr1 = "/ip4/192.168.1.1/tcp/4001".to_string();
        let addr2 = "/ip4/10.0.0.1/tcp/4001".to_string();
        let addr3 = "/ip4/172.16.0.1/tcp/4001".to_string();

        let mut peers = HashMap::new();
        peers.insert(peer1.clone(), vec![addr1.clone(), addr2.clone()]);
        peers.insert(peer2.clone(), vec![addr3.clone()]);

        store.save_all(&peers).await;

        let loaded = store.load_all().await;
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[&peer1].len(), 2);
        assert!(loaded[&peer1].contains(&addr1));
        assert!(loaded[&peer1].contains(&addr2));
        assert_eq!(loaded[&peer2], vec![addr3]);
    }

    #[tokio::test]
    async fn test_save_replaces_previous() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let kv_store = KeyValueStoreManager::connect(&db_path, &default_config())
            .await
            .unwrap();
        let store = PeerAddressStore::new(&kv_store).unwrap();

        let peer1 = "12D3KooWJ6b8uMzkA4q7hCkC4DKf8YqthpF9q2Mce3rLrFK2dP8V".to_string();
        let peer2 = "12D3KooWLA1M67Yf7dU6jMqZPyfSgh2pJf5ZQvWrcxNf4t8i3QpM".to_string();
        let addr = "/ip4/192.168.1.1/tcp/4001".to_string();

        let mut first = HashMap::new();
        first.insert(peer1.clone(), vec![addr.clone()]);
        store.save_all(&first).await;

        let mut second = HashMap::new();
        second.insert(peer2.clone(), vec![addr]);
        store.save_all(&second).await;

        let loaded = store.load_all().await;
        assert_eq!(loaded.len(), 1);
        assert!(loaded.contains_key(&peer2));
        assert!(!loaded.contains_key(&peer1));
    }

    #[tokio::test]
    async fn test_load_empty() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let kv_store = KeyValueStoreManager::connect(&db_path, &default_config())
            .await
            .unwrap();
        let store = PeerAddressStore::new(&kv_store).unwrap();

        let loaded = store.load_all().await;
        assert!(loaded.is_empty());
    }
}
