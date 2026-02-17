use dkg_blockchain::{BlockchainManagerConfig, BlockchainManagerConfigRaw};
use dkg_key_value_store::KeyValueStoreManagerConfig;
use dkg_network::NetworkManagerConfig;
use dkg_repository::{RepositoryManagerConfig, RepositoryManagerConfigRaw};
use dkg_triple_store::TripleStoreManagerConfig;
use serde::{Deserialize, Serialize};

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
