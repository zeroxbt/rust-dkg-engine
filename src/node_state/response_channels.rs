use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use dkg_network::{PeerId, ResponseHandle};
use dkg_observability as observability;
use tokio::time::Instant;
use uuid::Uuid;

/// Default timeout for response channels (5 minutes).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

type ChannelKey = (String, Uuid);

struct ChannelEntry<T> {
    channel: ResponseHandle<T>,
    created_at: Instant,
}

/// Generic cache for protocol response channels.
///
/// Each protocol should have its own ResponseChannels instance with its specific response type.
/// For example:
/// - `ResponseChannels<StoreAck>` for Store protocol
/// - `ResponseChannels<GetAck>` for Get protocol
///
/// Expired channels are cleaned up by the periodic cleanup task.
pub(crate) struct ResponseChannels<T> {
    channels: Arc<DashMap<ChannelKey, ChannelEntry<T>>>,
    timeout: Duration,
    protocol: &'static str,
}

impl<T> ResponseChannels<T> {
    pub(crate) fn new(protocol: &'static str) -> Self {
        let channels = Self {
            channels: Arc::new(DashMap::new()),
            timeout: DEFAULT_TIMEOUT,
            protocol,
        };
        observability::record_network_response_channel_cache_depth(protocol, 0);
        channels
    }

    pub(crate) fn store(&self, peer_id: &PeerId, operation_id: Uuid, channel: ResponseHandle<T>) {
        let key = (peer_id.to_string(), operation_id);
        let entry = ChannelEntry {
            channel,
            created_at: Instant::now(),
        };

        tracing::trace!(
            "Storing response channel for peer: {}, operation_id: {}",
            peer_id,
            operation_id
        );

        let replaced = self.channels.insert(key, entry).is_some();
        observability::record_network_response_channel_cache_event(
            self.protocol,
            if replaced { "store_replaced" } else { "store" },
            1,
        );
        observability::record_network_response_channel_cache_depth(
            self.protocol,
            self.channels.len(),
        );
    }

    pub(crate) fn retrieve(
        &self,
        peer_id: &PeerId,
        operation_id: Uuid,
    ) -> Option<ResponseHandle<T>> {
        let key = (peer_id.to_string(), operation_id);

        tracing::trace!(
            "Retrieving response channel for peer: {}, operation_id: {}",
            peer_id,
            operation_id
        );

        let response_channel = self.channels.remove(&key).map(|(_, entry)| entry.channel);
        observability::record_network_response_channel_cache_event(
            self.protocol,
            if response_channel.is_some() {
                "retrieve_hit"
            } else {
                "retrieve_miss"
            },
            1,
        );
        observability::record_network_response_channel_cache_depth(
            self.protocol,
            self.channels.len(),
        );
        response_channel
    }

    pub(crate) fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let mut expired_keys = Vec::new();

        for entry in self.channels.iter() {
            if now.duration_since(entry.value().created_at) > self.timeout {
                expired_keys.push(entry.key().clone());
            }
        }

        let removed = expired_keys.len();

        if removed > 0 {
            tracing::debug!(removed, "Cleaning up expired response channels");
            for key in expired_keys {
                self.channels.remove(&key);
            }
            observability::record_network_response_channel_cache_event(
                self.protocol,
                "cleanup_expired",
                removed,
            );
        }
        observability::record_network_response_channel_cache_depth(
            self.protocol,
            self.channels.len(),
        );

        removed
    }
}

impl<T> Default for ResponseChannels<T> {
    fn default() -> Self {
        Self::new("unknown")
    }
}
