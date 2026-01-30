#![allow(clippy::unwrap_used)]

//! Integration tests for the Triple Store Manager.
//!
//! These tests require a running Blazegraph instance for Blazegraph tests.
//! Start it with: `docker run -d -p 9999:9999 blazegraph/blazegraph:2.1.5`
//!
//! Oxigraph tests run with an in-memory backend and don't require external services.

use std::time::Duration;

use crate::managers::triple_store::{
    TripleStoreBackendType, TripleStoreManager, TripleStoreManagerConfig, config::TimeoutConfig,
};

fn blazegraph_config() -> TripleStoreManagerConfig {
    TripleStoreManagerConfig {
        backend: TripleStoreBackendType::Blazegraph,
        url: std::env::var("BLAZEGRAPH_URL")
            .unwrap_or_else(|_| "http://localhost:9999".to_string()),
        username: None,
        password: None,
        connect_max_retries: 3,
        connect_retry_frequency_ms: 1000,
        timeouts: TimeoutConfig {
            query_ms: 30000,
            insert_ms: 60000,
            ask_ms: 10000,
        },
        max_concurrent_operations: 16,
    }
}

/* async fn cleanup_repository(manager: &TripleStoreManager) {
    // Try to delete the test repository, ignore errors
    let _ = manager.update_raw("DROP ALL", None).await;
} */

#[tokio::test]
async fn test_blazegraph_health_check() {
    let config = blazegraph_config();
    let manager = match TripleStoreManager::connect(&config).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Skipping test - Blazegraph not available: {e}");
            return;
        }
    };

    let healthy = manager.health_check().await.unwrap();
    assert!(healthy);
}

#[tokio::test]
async fn test_oxigraph_in_memory() {
    // Create an in-memory Oxigraph backend
    let manager = TripleStoreManager::in_memory().unwrap();

    // Health check should pass
    let healthy = manager.health_check().await.unwrap();
    assert!(healthy);
}

#[tokio::test]
async fn test_oxigraph_insert() {
    let manager = TripleStoreManager::in_memory().unwrap();

    // Insert some test data
    let result = manager
        .update_raw(
            r#"
            INSERT DATA {
                GRAPH <did:dkg:test/1/public> {
                    <http://example.org/s1> <http://example.org/p1> "object1" .
                    <http://example.org/s2> <http://example.org/p2> "object2" .
                }
            }
            "#,
            Some(Duration::from_secs(10)),
        )
        .await;

    assert!(result.is_ok(), "Insert should succeed");
}
