use std::collections::HashMap;
use std::time::Duration;

use dkg_domain::BlockchainId;
use dkg_network::{Multiaddr, PeerId, StreamProtocol};

use super::{PeerRegistry, config::DEFAULT_LATENCY_MS};

impl PeerRegistry {
    /// Get the average latency for a peer.
    /// Returns DEFAULT_LATENCY_MS for unknown peers.
    pub fn get_average_latency(&self, peer_id: &PeerId) -> u64 {
        self.peers
            .get(peer_id)
            .map(|entry| entry.performance.average_latency())
            .unwrap_or(DEFAULT_LATENCY_MS)
    }

    /// Sort peers by their average latency (fastest first).
    /// Unknown peers get DEFAULT_LATENCY_MS and are placed in the middle.
    pub fn sort_by_latency(&self, peers: &mut [PeerId]) {
        peers.sort_by(|a, b| {
            let latency_a = self.get_average_latency(a);
            let latency_b = self.get_average_latency(b);
            latency_a.cmp(&latency_b)
        });
    }

    /// Check if we should attempt to discover this peer.
    /// Returns true if enough time has passed since the last failed attempt.
    pub fn should_attempt_discovery(&self, peer_id: &PeerId) -> bool {
        match self.peers.get(peer_id) {
            Some(entry) => {
                if entry.discovery_failure_count == 0 {
                    return true;
                }
                match entry.last_discovery_attempt {
                    Some(last_attempt) => {
                        let backoff = self.calculate_backoff(entry.discovery_failure_count);
                        last_attempt.elapsed() >= backoff
                    }
                    None => true,
                }
            }
            None => true,
        }
    }

    /// Get the current backoff delay for a peer (for logging).
    pub fn get_discovery_backoff(&self, peer_id: &PeerId) -> Option<Duration> {
        self.peers.get(peer_id).and_then(|entry| {
            if entry.discovery_failure_count > 0 {
                Some(self.calculate_backoff(entry.discovery_failure_count))
            } else {
                None
            }
        })
    }

    pub fn discovery_backoff(&self, peer_id: &PeerId) -> Option<Duration> {
        self.get_discovery_backoff(peer_id)
    }

    pub fn peer_supports_protocol(&self, peer_id: &PeerId, protocol: &'static str) -> bool {
        let protocol = StreamProtocol::new(protocol);
        self.peers
            .get(peer_id)
            .and_then(|entry| entry.identify.as_ref().cloned())
            .map(|identify| identify.protocols.contains(&protocol))
            .unwrap_or(false)
    }

    /// Count peers in a shard (for change detection optimization).
    pub fn shard_peer_count(&self, blockchain_id: &BlockchainId) -> usize {
        self.peers
            .iter()
            .filter(|entry| entry.shard_membership.contains(blockchain_id))
            .count()
    }

    /// Count shard peers that have been identified (received identify info).
    pub fn identified_shard_peer_count(&self, blockchain_id: &BlockchainId) -> usize {
        self.peers
            .iter()
            .filter(|entry| {
                entry.shard_membership.contains(blockchain_id) && entry.identify.is_some()
            })
            .count()
    }

    /// Get shard peers that support a protocol, sorted by latency (fastest first).
    /// Excludes the local peer if provided.
    pub fn select_shard_peers(
        &self,
        blockchain_id: &BlockchainId,
        protocol: &'static str,
        exclude_peer: Option<&PeerId>,
    ) -> Vec<PeerId> {
        let protocol = StreamProtocol::new(protocol);

        let mut peers: Vec<PeerId> = self
            .peers
            .iter()
            .filter(|entry| {
                // Must be in shard
                if !entry.shard_membership.contains(blockchain_id) {
                    return false;
                }
                // Exclude self if specified
                if let Some(exclude) = exclude_peer
                    && entry.key() == exclude
                {
                    return false;
                }
                // Must support protocol (if we have identify info)
                entry
                    .identify
                    .as_ref()
                    .map(|id| id.protocols.contains(&protocol))
                    .unwrap_or(false)
            })
            .map(|entry| *entry.key())
            .collect();

        // Sort by latency
        self.sort_by_latency(&mut peers);
        peers
    }

    /// Check if a peer is in a specific shard.
    pub fn is_peer_in_shard(&self, blockchain_id: &BlockchainId, peer_id: &PeerId) -> bool {
        self.peers
            .get(peer_id)
            .map(|entry| entry.shard_membership.contains(blockchain_id))
            .unwrap_or(false)
    }

    /// Returns the first blockchain for which `peer_id` is not in shard membership.
    pub fn first_missing_shard_membership<'a, I>(
        &self,
        peer_id: &PeerId,
        blockchains: I,
    ) -> Option<BlockchainId>
    where
        I: IntoIterator<Item = &'a BlockchainId>,
    {
        blockchains.into_iter().find_map(|blockchain| {
            if self.is_peer_in_shard(blockchain, peer_id) {
                None
            } else {
                Some(blockchain.clone())
            }
        })
    }

    /// Get all unique peer IDs across all shards.
    pub fn get_all_shard_peer_ids(&self) -> Vec<PeerId> {
        self.peers
            .iter()
            .filter(|entry| !entry.shard_membership.is_empty())
            .map(|entry| *entry.key())
            .collect()
    }

    /// Get all known peer addresses from identify info.
    /// Returns peers that have identify info with non-empty listen addresses.
    pub fn get_all_addresses(&self) -> HashMap<PeerId, Vec<Multiaddr>> {
        self.peers
            .iter()
            .filter_map(|entry| {
                entry.identify.as_ref().and_then(|id| {
                    if id.listen_addrs.is_empty() {
                        None
                    } else {
                        Some((*entry.key(), id.listen_addrs.clone()))
                    }
                })
            })
            .collect()
    }
}
