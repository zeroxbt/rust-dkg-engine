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
/*
#[tokio::test]
async fn test_ensure_repository() {
    let config = test_config();
    let manager = match TripleStoreManager::connect(&config).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Skipping test - Blazegraph not available: {e}");
            return;
        }
    };

    // Should create repository if it doesn't exist
    manager.ensure_repository().await.unwrap();

    // Should succeed even if called again
    manager.ensure_repository().await.unwrap();

    cleanup_repository(&manager).await;
} */

/*
#[tokio::test]
async fn test_insert_and_get_knowledge_collection() {
    let config = test_config();
    let manager = match TripleStoreManager::connect(&config).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Skipping test - Blazegraph not available: {e}");
            return;
        }
    };

    manager.ensure_repository().await.unwrap();

    let ual = "did:dkg:hardhat1:31337/0x1234/1";
    let public_nquads = r#"<http://example.org/subject1> <http://example.org/predicate1> "value1" .
<http://example.org/subject2> <http://example.org/predicate2> "value2" ."#;

    let private_nquads =
        r#"<http://example.org/secret> <http://example.org/hasPassword> "secret123" ."#;

    let metadata = KnowledgeCollectionMetadata {
        published_by: "0xTestPublisher".to_string(),
        published_at_block: 12345,
        publish_tx: "0xabcdef1234567890".to_string(),
        publish_time: 1700000000,
        block_time: 1700000000,
    };

    // Insert knowledge collection
    manager
        .insert_knowledge_collection(ual, public_nquads, Some(private_nquads), &metadata)
        .await
        .unwrap();

    // Get from public named graph
    let result = manager
        .get_knowledge_collection_named_graphs(ual, Visibility::Public)
        .await
        .unwrap();
    assert!(!result.is_empty(), "Should return public triples");
    assert!(result.contains("subject1"), "Should contain public subject");

    // Get from private named graph
    let result = manager
        .get_knowledge_collection_named_graphs(ual, Visibility::Private)
        .await
        .unwrap();
    assert!(!result.is_empty(), "Should return private triples");
    assert!(result.contains("secret"), "Should contain private subject");

    // Get all (both public and private)
    let result = manager
        .get_knowledge_collection_named_graphs(ual, Visibility::All)
        .await
        .unwrap();
    assert!(result.contains("subject1"), "Should contain public subject");
    assert!(result.contains("secret"), "Should contain private subject");

    cleanup_repository(&manager).await;
}

#[tokio::test]
async fn test_knowledge_collection_exists() {
    let config = test_config();
    let manager = match TripleStoreManager::connect(&config).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Skipping test - Blazegraph not available: {e}");
            return;
        }
    };

    manager.ensure_repository().await.unwrap();

    let ual = "did:dkg:hardhat1:31337/0x5678/2";

    // Should not exist initially
    // Note: This checks unified graph which we haven't populated in this test
    // For named graph check, we need different logic

    let public_nquads = r#"<http://example.org/test> <http://example.org/pred> "test" ."#;

    let metadata = KnowledgeCollectionMetadata {
        published_by: "0xTest".to_string(),
        published_at_block: 1,
        publish_tx: "0x123".to_string(),
        publish_time: 1,
        block_time: 1,
    };

    // Insert
    manager
        .insert_knowledge_collection(ual, public_nquads, None, &metadata)
        .await
        .unwrap();

    // Verify we can get metadata
    let metadata_result = manager.get_metadata(ual).await.unwrap();
    assert!(!metadata_result.is_empty(), "Should have metadata");

    cleanup_repository(&manager).await;
}

#[tokio::test]
async fn test_metadata_operations() {
    let config = test_config();
    let manager = match TripleStoreManager::connect(&config).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Skipping test - Blazegraph not available: {e}");
            return;
        }
    };

    manager.ensure_repository().await.unwrap();

    let ual = "did:dkg:hardhat1:31337/0xmeta/3";
    let public_nquads = r#"<http://example.org/data> <http://example.org/value> "123" ."#;

    let metadata = KnowledgeCollectionMetadata {
        published_by: "0xMetaTest".to_string(),
        published_at_block: 999,
        publish_tx: "0xdeadbeef".to_string(),
        publish_time: 1700000000,
        block_time: 1700000001,
    };

    manager
        .insert_knowledge_collection(ual, public_nquads, None, &metadata)
        .await
        .unwrap();

    // Get metadata
    let result = manager.get_metadata(ual).await.unwrap();
    assert!(
        result.contains("did:dkg:publisherKey/0xMetaTest"),
        "Should contain publisher as URI"
    );
    assert!(result.contains("999"), "Should contain block number");
    assert!(result.contains("0xdeadbeef"), "Should contain tx hash");
    // Check datetime format is ISO 8601
    assert!(
        result.contains("2023-11-14T") || result.contains("dateTime"),
        "Should contain datetime values"
    );

    // Delete metadata
    manager.delete_metadata(ual).await.unwrap();

    // Verify deleted
    let result = manager.get_metadata(ual).await.unwrap();
    assert!(
        result.is_empty() || !result.contains("0xMetaTest"),
        "Metadata should be deleted"
    );

    cleanup_repository(&manager).await;
}

#[tokio::test]
async fn test_drop_named_graph() {
    let config = test_config();
    let manager = match TripleStoreManager::connect(&config).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Skipping test - Blazegraph not available: {e}");
            return;
        }
    };

    manager.ensure_repository().await.unwrap();

    let ual = "did:dkg:hardhat1:31337/0xdrop/4";
    let public_nquads = r#"<http://example.org/drop> <http://example.org/test> "dropme" ."#;

    let metadata = KnowledgeCollectionMetadata {
        published_by: "0xDrop".to_string(),
        published_at_block: 1,
        publish_tx: "0x1".to_string(),
        publish_time: 1,
        block_time: 1,
    };

    manager
        .insert_knowledge_collection(ual, public_nquads, None, &metadata)
        .await
        .unwrap();

    // Verify data exists
    let result = manager
        .get_knowledge_collection_named_graphs(ual, Visibility::Public)
        .await
        .unwrap();
    assert!(!result.is_empty(), "Data should exist before drop");

    // Drop the named graph
    let public_graph = format!("{ual}/public");
    manager.drop_named_graph(&public_graph).await.unwrap();

    // Verify data is gone
    let result = manager
        .get_knowledge_collection_named_graphs(ual, Visibility::Public)
        .await
        .unwrap();
    assert!(result.is_empty(), "Data should be gone after drop");

    cleanup_repository(&manager).await;
}

#[tokio::test]
async fn test_raw_sparql_queries() {
    let config = test_config();
    let manager = match TripleStoreManager::connect(&config).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Skipping test - Blazegraph not available: {e}");
            return;
        }
    };

    manager.ensure_repository().await.unwrap();

    // Insert some test data using raw update
    let insert_query = r#"
        INSERT DATA {
            GRAPH <test:graph> {
                <http://example.org/s1> <http://example.org/p1> "object1" .
                <http://example.org/s2> <http://example.org/p2> "object2" .
            }
        }
    "#;
    manager.update_raw(insert_query, None).await.unwrap();

    // Test SELECT query
    let select_query = r#"
        SELECT ?s ?p ?o
        WHERE {
            GRAPH <test:graph> {
                ?s ?p ?o
            }
        }
    "#;
    let result = manager.select_raw(select_query, None).await.unwrap();
    assert_eq!(result.rows.len(), 2, "Should have 2 results");

    // Test CONSTRUCT query
    let construct_query = r#"
        CONSTRUCT { ?s ?p ?o }
        WHERE {
            GRAPH <test:graph> {
                ?s ?p ?o
            }
        }
    "#;
    let result = manager.construct_raw(construct_query, None).await.unwrap();
    assert!(result.contains("object1"), "Should contain first object");
    assert!(result.contains("object2"), "Should contain second object");

    // Test ASK query
    let ask_query = r#"
        ASK {
            GRAPH <test:graph> {
                <http://example.org/s1> ?p ?o
            }
        }
    "#;
    let exists = manager.ask_raw(ask_query, None).await.unwrap();
    assert!(exists, "Subject should exist");

    let ask_query_false = r#"
        ASK {
            GRAPH <test:graph> {
                <http://example.org/nonexistent> ?p ?o
            }
        }
    "#;
    let exists = manager.ask_raw(ask_query_false, None).await.unwrap();
    assert!(!exists, "Nonexistent subject should not exist");

    cleanup_repository(&manager).await;
}
 */
