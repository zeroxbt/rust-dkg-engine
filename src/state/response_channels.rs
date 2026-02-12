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
/// Expired channels are cleaned up by the periodic cleanup task.
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
        }

        removed
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
