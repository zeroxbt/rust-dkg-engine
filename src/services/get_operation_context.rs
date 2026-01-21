use std::{
    collections::HashMap,
    sync::RwLock,
    time::{Duration, Instant},
};

use blockchain::BlockchainId;
use triple_store::Visibility;
use uuid::Uuid;

/// Context stored for each get operation to enable response validation.
#[derive(Debug, Clone)]
pub struct GetOperationContext {
    /// The blockchain where the knowledge collection exists
    pub blockchain: BlockchainId,
    /// The knowledge collection ID
    pub knowledge_collection_id: u128,
    /// The specific knowledge asset ID (if requesting a single asset)
    pub knowledge_asset_id: Option<u128>,
    /// The visibility being requested (public, private, or all)
    pub visibility: Visibility,
    /// When the context was created (for TTL cleanup)
    created_at: Instant,
}

impl GetOperationContext {
    pub fn new(
        blockchain: BlockchainId,
        knowledge_collection_id: u128,
        knowledge_asset_id: Option<u128>,
        visibility: Visibility,
    ) -> Self {
        Self {
            blockchain,
            knowledge_collection_id,
            knowledge_asset_id,
            visibility,
            created_at: Instant::now(),
        }
    }

    /// Check if this is a single knowledge asset request (skip validation).
    pub fn is_single_asset(&self) -> bool {
        self.knowledge_asset_id.is_some()
    }
}

/// In-memory store for get operation contexts.
///
/// Used to store context when starting a get operation and retrieve it
/// when validating responses in the RPC controller.
pub struct GetOperationContextStore {
    contexts: RwLock<HashMap<Uuid, GetOperationContext>>,
    /// Time-to-live for contexts (cleanup stale entries)
    ttl: Duration,
}

impl GetOperationContextStore {
    /// Create a new context store with the specified TTL.
    pub fn new(ttl: Duration) -> Self {
        Self {
            contexts: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    /// Create a new context store with a default TTL of 5 minutes.
    pub fn with_default_ttl() -> Self {
        Self::new(Duration::from_secs(300))
    }

    /// Store context for an operation.
    pub fn store(&self, operation_id: Uuid, context: GetOperationContext) {
        let mut contexts = self.contexts.write().unwrap();

        // Clean up expired entries opportunistically
        self.cleanup_expired_internal(&mut contexts);

        contexts.insert(operation_id, context);
    }

    /// Get context for an operation (does not remove it).
    pub fn get(&self, operation_id: &Uuid) -> Option<GetOperationContext> {
        let contexts = self.contexts.read().unwrap();
        contexts.get(operation_id).cloned()
    }

    /// Remove and return context for an operation.
    pub fn remove(&self, operation_id: &Uuid) -> Option<GetOperationContext> {
        let mut contexts = self.contexts.write().unwrap();
        contexts.remove(operation_id)
    }

    /// Clean up expired contexts.
    fn cleanup_expired_internal(&self, contexts: &mut HashMap<Uuid, GetOperationContext>) {
        let now = Instant::now();
        contexts.retain(|_, ctx| now.duration_since(ctx.created_at) < self.ttl);
    }

    /// Get the number of stored contexts (for debugging/monitoring).
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.contexts.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_and_get() {
        let store = GetOperationContextStore::with_default_ttl();
        let op_id = Uuid::new_v4();
        let context = GetOperationContext::new(
            BlockchainId::new("hardhat1:31337"),
            1,
            None,
            Visibility::All,
        );

        store.store(op_id, context.clone());

        let retrieved = store.get(&op_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().knowledge_collection_id, 1);
    }

    #[test]
    fn test_remove() {
        let store = GetOperationContextStore::with_default_ttl();
        let op_id = Uuid::new_v4();
        let context = GetOperationContext::new(
            BlockchainId::new("hardhat1:31337"),
            1,
            Some(5_u128),
            Visibility::Public,
        );

        store.store(op_id, context);

        let removed = store.remove(&op_id);
        assert!(removed.is_some());
        assert!(removed.unwrap().is_single_asset());

        // Should be gone now
        assert!(store.get(&op_id).is_none());
    }

    #[test]
    fn test_is_single_asset() {
        let collection_ctx = GetOperationContext::new(
            BlockchainId::new("hardhat1:31337"),
            1,
            None,
            Visibility::All,
        );
        assert!(!collection_ctx.is_single_asset());

        let asset_ctx = GetOperationContext::new(
            BlockchainId::new("hardhat1:31337"),
            1,
            Some(5_u128),
            Visibility::Public,
        );
        assert!(asset_ctx.is_single_asset());
    }
}
