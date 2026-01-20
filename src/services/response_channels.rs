use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use network::{PeerId, ResponseMessage, request_response};
use serde::{Serialize, de::DeserializeOwned};
use tokio::time::Instant;
use uuid::Uuid;

type ChannelKey = (String, Uuid);

struct ChannelEntry<T> {
    channel: request_response::ResponseChannel<ResponseMessage<T>>,
    created_at: Instant,
}

/// Generic cache for protocol response channels.
///
/// Each protocol should have its own ResponseChannels instance with its specific response type.
/// For example:
/// - `ResponseChannels<StoreResponseData>` for Store protocol
/// - `ResponseChannels<GetResponseData>` for Get protocol
pub struct ResponseChannels<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    channels: Arc<DashMap<ChannelKey, ChannelEntry<T>>>,
}

impl<T> ResponseChannels<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
        }
    }

    pub fn store(
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

    pub fn retrieve(
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

    pub fn remove(&self, peer_id: &PeerId, operation_id: Uuid) {
        let key = (peer_id.to_string(), operation_id);

        tracing::trace!(
            "Removing response channel for peer: {}, operation_id: {}",
            peer_id,
            operation_id
        );

        self.channels.remove(&key);
    }

    fn cleanup_expired(
        channels: &DashMap<ChannelKey, ChannelEntry<T>>,
        timeout: Duration,
    ) {
        let now = Instant::now();
        let mut expired_keys = Vec::new();

        for entry in channels.iter() {
            if now.duration_since(entry.value().created_at) > timeout {
                expired_keys.push(entry.key().clone());
            }
        }

        if !expired_keys.is_empty() {
            tracing::debug!("Cleaning up {} expired response channels", expired_keys.len());
            for key in expired_keys {
                channels.remove(&key);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.channels.len()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::rpc_controller::messages::StoreResponseData;

    #[tokio::test]
    async fn test_len() {
        let channels: ResponseChannels<StoreResponseData> = ResponseChannels::new();
        assert_eq!(channels.len(), 0);
    }
}
