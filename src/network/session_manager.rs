use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use network::{PeerId, ResponseMessage, request_response};
use serde::{Serialize, de::DeserializeOwned};
use tokio::time::Instant;
use uuid::Uuid;

type SessionKey = (String, Uuid);

struct SessionEntry<T> {
    channel: request_response::ResponseChannel<ResponseMessage<T>>,
    created_at: Instant,
}

/// Generic session manager for caching protocol response channels
///
/// Each protocol should have its own SessionManager instance with its specific response type.
/// For example:
/// - `SessionManager<StoreResponseData>` for Store protocol
/// - `SessionManager<GetResponseData>` for Get protocol
pub struct SessionManager<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    sessions: Arc<DashMap<SessionKey, SessionEntry<T>>>,
}

impl<T> SessionManager<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
        }
    }

    pub fn store_channel(
        &self,
        peer_id: &PeerId,
        operation_id: Uuid,
        channel: request_response::ResponseChannel<ResponseMessage<T>>,
    ) {
        let key = (peer_id.to_string(), operation_id);
        let entry = SessionEntry {
            channel,
            created_at: Instant::now(),
        };

        tracing::trace!(
            "Storing session for peer: {}, operation_id: {}",
            peer_id,
            operation_id
        );

        self.sessions.insert(key, entry);
    }

    pub fn retrieve_channel(
        &self,
        peer_id: &PeerId,
        operation_id: Uuid,
    ) -> Option<request_response::ResponseChannel<ResponseMessage<T>>> {
        let key = (peer_id.to_string(), operation_id);

        tracing::trace!(
            "Retrieving session for peer: {}, operation_id: {}",
            peer_id,
            operation_id
        );

        self.sessions.remove(&key).map(|(_, entry)| entry.channel)
    }

    pub fn remove_session(&self, peer_id: &PeerId, operation_id: Uuid) {
        let key = (peer_id.to_string(), operation_id);

        tracing::trace!(
            "Removing session for peer: {}, operation_id: {}",
            peer_id,
            operation_id
        );

        self.sessions.remove(&key);
    }

    fn cleanup_expired_sessions(
        sessions: &DashMap<SessionKey, SessionEntry<T>>,
        timeout: Duration,
    ) {
        let now = Instant::now();
        let mut expired_keys = Vec::new();

        for entry in sessions.iter() {
            if now.duration_since(entry.value().created_at) > timeout {
                expired_keys.push(entry.key().clone());
            }
        }

        if !expired_keys.is_empty() {
            tracing::debug!("Cleaning up {} expired sessions", expired_keys.len());
            for key in expired_keys {
                sessions.remove(&key);
            }
        }
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::protocol::StoreResponseData;

    #[tokio::test]
    async fn test_session_count() {
        let manager: SessionManager<StoreResponseData> = SessionManager::new();
        assert_eq!(manager.session_count(), 0);
    }
}
