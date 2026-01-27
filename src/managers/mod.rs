pub(crate) mod blockchain;
pub(crate) mod key_value_store;
pub(crate) mod network;
pub(crate) mod repository;
pub(crate) mod triple_store;

use std::sync::Arc;

use libp2p::identity::Keypair;

use crate::config::{AppPaths, ManagersConfig};

pub(crate) use blockchain::BlockchainManager;
pub(crate) use key_value_store::KeyValueStoreManager;
pub(crate) use network::NetworkManager;
pub(crate) use repository::RepositoryManager;
pub(crate) use triple_store::TripleStoreManager;

/// Container for all initialized managers.
pub(crate) struct Managers {
    pub network: Arc<NetworkManager>,
    pub repository: Arc<RepositoryManager>,
    pub blockchain: Arc<BlockchainManager>,
    pub triple_store: Arc<TripleStoreManager>,
    pub key_value_store: Arc<KeyValueStoreManager>,
}

/// Initialize all managers.
pub(crate) async fn initialize(
    config: &ManagersConfig,
    paths: &AppPaths,
    network_key: Keypair,
) -> Managers {
    // NetworkManager creates base protocols (kad, identify) and app protocols (store, get, etc.)
    let network = Arc::new(
        NetworkManager::connect(&config.network, network_key)
            .await
            .expect("Failed to initialize network manager"),
    );

    let repository = Arc::new(
        RepositoryManager::connect(&config.repository)
            .await
            .expect("Failed to initialize repository manager"),
    );

    let mut blockchain_manager = BlockchainManager::connect(&config.blockchain)
        .await
        .expect("Failed to initialize blockchain manager");
    blockchain_manager
        .initialize_identities(&network.peer_id().to_base58())
        .await
        .expect("Failed to initialize blockchain identities");
    let blockchain = Arc::new(blockchain_manager);

    // Use centralized triple store path, merging with existing config
    let mut triple_store_config = config.triple_store.clone();
    triple_store_config.data_path = Some(paths.triple_store.clone());

    let triple_store = Arc::new(
        TripleStoreManager::connect(&triple_store_config)
            .await
            .expect("Failed to initialize triple store manager"),
    );

    let key_value_store = Arc::new(
        KeyValueStoreManager::connect(&paths.key_value_store)
            .expect("Failed to initialize key-value store manager"),
    );

    Managers {
        network,
        repository,
        blockchain,
        triple_store,
        key_value_store,
    }
}
