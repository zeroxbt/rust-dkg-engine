use std::time::{Duration, Instant};

use dashmap::DashMap;
use uuid::Uuid;

/// Entry in the context store with TTL tracking.
struct ContextEntry<S> {
    state: S,
    created_at: Instant,
}

/// Generic in-memory store for operation state/context.
///
/// This replaces operation-specific stores like `GetOperationContextStore`.
/// Uses `DashMap` for concurrent access with minimal lock contention.
pub struct ContextStore<S: Clone + Send + Sync> {
    contexts: DashMap<Uuid, ContextEntry<S>>,
    ttl: Duration,
}

impl<S: Clone + Send + Sync> ContextStore<S> {
    /// Create a new context store with the specified TTL.
    pub fn new(ttl: Duration) -> Self {
        Self {
            contexts: DashMap::new(),
            ttl,
        }
    }

    /// Create with default TTL of 5 minutes.
    pub fn with_default_ttl() -> Self {
        Self::new(Duration::from_secs(300))
    }

    /// Store context for an operation.
    pub fn store(&self, operation_id: Uuid, state: S) {
        // Clean up expired entries opportunistically (locks shards one at a time)
        self.cleanup_expired();

        self.contexts.insert(
            operation_id,
            ContextEntry {
                state,
                created_at: Instant::now(),
            },
        );
    }

    /// Get context for an operation (does not remove it).
    pub fn get(&self, operation_id: &Uuid) -> Option<S> {
        self.contexts
            .get(operation_id)
            .map(|entry| entry.state.clone())
    }

    /// Remove and return context for an operation.
    pub fn remove(&self, operation_id: &Uuid) -> Option<S> {
        self.contexts
            .remove(operation_id)
            .map(|(_, entry)| entry.state)
    }

    /// Clean up expired contexts.
    fn cleanup_expired(&self) {
        let now = Instant::now();
        self.contexts
            .retain(|_, entry| now.duration_since(entry.created_at) < self.ttl);
    }

    /// Get the number of stored contexts (for debugging/monitoring).
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.contexts.len()
    }

    /// Check if the store is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.contexts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct TestState {
        value: String,
    }

    #[test]
    fn test_store_and_get() {
        let store: ContextStore<TestState> = ContextStore::with_default_ttl();
        let op_id = Uuid::new_v4();
        let state = TestState {
            value: "test".to_string(),
        };

        store.store(op_id, state.clone());

        let retrieved = store.get(&op_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), state);
    }

    #[test]
    fn test_remove() {
        let store: ContextStore<TestState> = ContextStore::with_default_ttl();
        let op_id = Uuid::new_v4();
        let state = TestState {
            value: "test".to_string(),
        };

        store.store(op_id, state.clone());

        let removed = store.remove(&op_id);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap(), state);

        // Should be gone now
        assert!(store.get(&op_id).is_none());
    }

    #[test]
    fn test_multiple_operations() {
        let store: ContextStore<TestState> = ContextStore::with_default_ttl();
        let op1 = Uuid::new_v4();
        let op2 = Uuid::new_v4();

        store.store(
            op1,
            TestState {
                value: "one".to_string(),
            },
        );
        store.store(
            op2,
            TestState {
                value: "two".to_string(),
            },
        );

        assert_eq!(store.len(), 2);
        assert_eq!(store.get(&op1).unwrap().value, "one");
        assert_eq!(store.get(&op2).unwrap().value, "two");
    }
}
