#![allow(clippy::unwrap_used)]

//! Integration tests for the Triple Store Manager.
//!
//! These tests require a running Blazegraph instance for Blazegraph tests.
//! Start it with: `docker run -d -p 9999:9999 blazegraph/blazegraph:2.1.5`

use dkg_domain::KnowledgeAsset;
use tempfile::TempDir;

use crate::{
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

#[tokio::test]
async fn test_blazegraph_insert_and_query() {
    if !super::require_blazegraph() {
        return;
    }

    let config = blazegraph_config();
    let temp_dir = TempDir::new().unwrap();
    let manager = match TripleStoreManager::connect(&config, temp_dir.path()).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Skipping test - Blazegraph not available: {e}");
            return;
        }
    };

    let kc_ual = "did:dkg:test/1";
    let ka = KnowledgeAsset::new(
        format!("{}/1", kc_ual),
        vec!["<http://example.org/s1> <http://example.org/p1> \"object1\" .".to_string()],
    );

    manager
        .insert_knowledge_collection(kc_ual, &[ka], &None, None)
        .await
        .unwrap();

    let existing = manager
        .knowledge_collections_exist_by_uals(&[kc_ual.to_string()])
        .await
        .unwrap();
    assert!(existing.contains(kc_ual));
}
