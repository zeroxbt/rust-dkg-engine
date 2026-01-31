pub(crate) mod blockchain;
pub(crate) mod key_value_store;
pub(crate) mod network;
pub(crate) mod repository;
pub(crate) mod triple_store;

use std::sync::Arc;

pub(crate) use blockchain::BlockchainManager;
pub(crate) use key_value_store::KeyValueStoreManager;
use libp2p::identity::Keypair;
pub(crate) use network::{NetworkManager, NetworkEventLoop};
pub(crate) use repository::RepositoryManager;
pub(crate) use triple_store::TripleStoreManager;

use crate::config::{AppPaths, ManagersConfig};

/// Container for all initialized managers.
pub(crate) struct Managers {
    pub network: Arc<NetworkManager>,
    pub repository: Arc<RepositoryManager>,
    pub blockchain: Arc<BlockchainManager>,
    pub triple_store: Arc<TripleStoreManager>,
    pub key_value_store: Arc<KeyValueStoreManager>,
}

/// Initialize all managers.
///
/// Returns a tuple of (Managers, NetworkEventLoop). The NetworkEventLoop must be
/// spawned separately to run the network event loop.
pub(crate) async fn initialize(
    config: &ManagersConfig,
    paths: &AppPaths,
    network_key: Keypair,
) -> (Managers, NetworkEventLoop) {
    // NetworkManager::connect returns (handle, service) tuple
    let (network_handle, network_service) =
        NetworkManager::connect(&config.network, network_key)
            .expect("Failed to initialize network manager");
    let network = Arc::new(network_handle);

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

    let triple_store = Arc::new(
        TripleStoreManager::connect(&config.triple_store, &paths.triple_store)
            .await
            .expect("Failed to initialize triple store manager"),
    );

    let key_value_store = Arc::new(
        KeyValueStoreManager::connect(&paths.key_value_store)
            .expect("Failed to initialize key-value store manager"),
    );

    let managers = Managers {
        network,
        repository,
        blockchain,
        triple_store,
        key_value_store,
    };

    (managers, network_service)
}
