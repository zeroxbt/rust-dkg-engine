use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use serde::{Serialize, de::DeserializeOwned};
use tokio::time::Instant;
use uuid::Uuid;

use crate::managers::network::{PeerId, ResponseMessage, request_response};

/// Default timeout for response channels (5 minutes).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

type ChannelKey = (String, Uuid);

struct ChannelEntry<T> {
    channel: request_response::ResponseChannel<ResponseMessage<T>>,
    created_at: Instant,
}

/// Generic cache for protocol response channels.
///
/// Each protocol should have its own ResponseChannels instance with its specific response type.
/// For example:
/// - `ResponseChannels<StoreAck>` for Store protocol
/// - `ResponseChannels<GetAck>` for Get protocol
///
/// Expired channels are cleaned up opportunistically when new channels are stored.
pub(crate) struct ResponseChannels<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    channels: Arc<DashMap<ChannelKey, ChannelEntry<T>>>,
    timeout: Duration,
}

impl<T> ResponseChannels<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    pub(crate) fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    pub(crate) fn store(
        &self,
        peer_id: &PeerId,
        operation_id: Uuid,
        channel: request_response::ResponseChannel<ResponseMessage<T>>,
    ) {
        // Clean up expired entries opportunistically
        Self::cleanup_expired(&self.channels, self.timeout);

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

        self.channels.insert(key, entry);
    }

    pub(crate) fn retrieve(
        &self,
        peer_id: &PeerId,
        operation_id: Uuid,
    ) -> Option<request_response::ResponseChannel<ResponseMessage<T>>> {
        let key = (peer_id.to_string(), operation_id);

        tracing::trace!(
            "Retrieving response channel for peer: {}, operation_id: {}",
            peer_id,
            operation_id
        );

        self.channels.remove(&key).map(|(_, entry)| entry.channel)
    }

    fn cleanup_expired(channels: &DashMap<ChannelKey, ChannelEntry<T>>, timeout: Duration) {
        let now = Instant::now();
        let mut expired_keys = Vec::new();

        for entry in channels.iter() {
            if now.duration_since(entry.value().created_at) > timeout {
                expired_keys.push(entry.key().clone());
            }
        }

        if !expired_keys.is_empty() {
            tracing::debug!(
                "Cleaning up {} expired response channels",
                expired_keys.len()
            );
            for key in expired_keys {
                channels.remove(&key);
            }
        }
    }
}

impl<T> Default for ResponseChannels<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}
