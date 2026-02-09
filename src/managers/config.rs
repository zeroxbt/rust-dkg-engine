use serde::{Deserialize, Serialize};

use crate::managers::{
    blockchain::{BlockchainManagerConfig, BlockchainManagerConfigRaw},
    key_value_store::KeyValueStoreManagerConfig,
    network::NetworkManagerConfig,
    repository::{RepositoryManagerConfig, RepositoryManagerConfigRaw},
    triple_store::TripleStoreManagerConfig,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagersConfigRaw {
    pub network: NetworkManagerConfig,
    pub repository: RepositoryManagerConfigRaw,
    pub blockchain: BlockchainManagerConfigRaw,
    pub triple_store: TripleStoreManagerConfig,
    pub key_value_store: KeyValueStoreManagerConfig,
}

impl ManagersConfigRaw {
    pub(crate) fn resolve(self) -> Result<ManagersConfig, crate::config::ConfigError> {
        Ok(ManagersConfig {
            network: self.network,
            repository: self.repository.resolve()?,
            blockchain: self.blockchain.resolve()?,
            triple_store: self.triple_store,
            key_value_store: self.key_value_store,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ManagersConfig {
    pub network: NetworkManagerConfig,
    pub repository: RepositoryManagerConfig,
    pub blockchain: BlockchainManagerConfig,
    pub triple_store: TripleStoreManagerConfig,
    pub key_value_store: KeyValueStoreManagerConfig,
}
