mod config;

use std::sync::Arc;

pub(crate) use config::{ManagersConfig, ManagersConfigRaw};
use dkg_blockchain::BlockchainManager;
use dkg_key_value_store::{KeyValueStoreManager, PeerAddressStore, PublishTmpDatasetStore};
use dkg_network::{Keypair, NetworkEventLoop, NetworkManager};
use dkg_repository::RepositoryManager;
use dkg_triple_store::TripleStoreManager;

use crate::config::AppPaths;

/// Container for all initialized managers.
pub(crate) struct Managers {
    pub network: Arc<NetworkManager>,
    pub repository: Arc<RepositoryManager>,
    pub blockchain: Arc<BlockchainManager>,
    pub triple_store: Arc<TripleStoreManager>,
    pub key_value_store: Arc<KeyValueStoreManager>,
    pub peer_address_store: Arc<PeerAddressStore>,
    pub publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
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
    let (network_handle, network_service) = NetworkManager::connect(&config.network, network_key)
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
        KeyValueStoreManager::connect(&paths.key_value_store, &config.key_value_store)
            .await
            .expect("Failed to initialize key-value store manager"),
    );
    let peer_address_store = Arc::new(key_value_store.peer_address_store());
    let publish_tmp_dataset_store = Arc::new(key_value_store.publish_tmp_dataset_store());

    let managers = Managers {
        network,
        repository,
        blockchain,
        triple_store,
        key_value_store,
        peer_address_store,
        publish_tmp_dataset_store,
    };

    (managers, network_service)
}
