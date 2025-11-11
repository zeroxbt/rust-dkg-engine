use std::{cmp::Ordering, sync::Arc};

use blockchain::{BlockchainManager, BlockchainName};
use network::PeerId;
use repository::RepositoryManager;
use validation::{HashFunction, ValidationManager};

pub struct ShardingTableService {
    repository_manager: Arc<RepositoryManager>,
    blockchain_manager: Arc<BlockchainManager>,
    validation_manager: Arc<ValidationManager>,
}

impl ShardingTableService {
    pub fn new(
        repository_manager: Arc<RepositoryManager>,
        blockchain_manager: Arc<BlockchainManager>,
        validation_manager: Arc<ValidationManager>,
    ) -> Self {
        Self {
            repository_manager,
            blockchain_manager,
            validation_manager,
        }
    }

    pub async fn find_neighborhood(
        &self,
        blockchain: BlockchainName,
        keyword: Vec<u8>,
        r2: usize,
        hash_function_id: u8,
        filter_last_seen: bool,
    ) -> Vec<PeerId> {
        let peer_models = self
            .repository_manager
            .shard_repository()
            .get_all_peer_records(blockchain.as_str(), filter_last_seen)
            .await
            .unwrap();

        let hash_function = HashFunction::from_id(hash_function_id);
        let key_hash = self
            .validation_manager
            .call_hash_function(&hash_function, keyword);

        let mut peer_distances = peer_models
            .into_iter()
            .map(|peer_model| match &hash_function {
                HashFunction::Sha256 => (
                    peer_model.peer_id.parse::<PeerId>().unwrap(),
                    Self::xor(
                        &blockchain::utils::from_hex_string(peer_model.sha256).unwrap(),
                        &key_hash,
                    )
                    .unwrap(),
                ),
            })
            .collect::<Vec<_>>();

        peer_distances.sort_unstable_by(|p1, p2| Self::compare(&p1.1, &p2.1));

        // Optionally limit to r2 closest peers and return their IDs
        peer_distances
            .into_iter()
            .take(r2)
            .map(|(peer, _)| peer)
            .collect()
    }

    pub fn xor(a: &[u8], b: &[u8]) -> Result<Vec<u8>, &'static str> {
        if a.len() != b.len() {
            return Err("Inputs should have the same length");
        }

        let result: Vec<u8> = a.iter().zip(b.iter()).map(|(&x, &y)| x ^ y).collect();

        Ok(result)
    }

    pub fn compare(a: &[u8], b: &[u8]) -> Ordering {
        for (byte_a, byte_b) in a.iter().zip(b.iter()) {
            match byte_a.cmp(byte_b) {
                std::cmp::Ordering::Equal => continue,
                x => return x,
            }
        }

        a.len().cmp(&b.len())
    }
}
