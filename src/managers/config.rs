use serde::Deserialize;

use crate::managers::{
    blockchain::BlockchainManagerConfig, key_value_store::KeyValueStoreManagerConfig,
    network::NetworkManagerConfig, repository::RepositoryManagerConfig,
    triple_store::TripleStoreManagerConfig,
};

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagersConfig {
    pub network: NetworkManagerConfig,
    pub repository: RepositoryManagerConfig,
    pub blockchain: BlockchainManagerConfig,
    pub triple_store: TripleStoreManagerConfig,
    pub key_value_store: KeyValueStoreManagerConfig,
}
