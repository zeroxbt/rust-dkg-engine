#![allow(clippy::unwrap_used)]

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use dkg_domain::{KnowledgeAsset, KnowledgeCollectionMetadata};
use tempfile::TempDir;

use crate::{
    GraphVisibility, TripleStoreBackend, TripleStoreBackendType, TripleStoreManager,
    TripleStoreManagerConfig,
    config::{OxigraphStoreConfig, TimeoutConfig},
    query::predicates,
};

#[derive(Clone, Copy)]
enum TestBackendKind {
    Oxigraph,
    Blazegraph,
}

fn selected_backend() -> TestBackendKind {
    match std::env::var("TRIPLE_STORE_TEST_BACKEND")
        .ok()
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("blazegraph") => TestBackendKind::Blazegraph,
        Some("oxigraph") | None => TestBackendKind::Oxigraph,
        Some(other) => panic!(
            "Unknown TRIPLE_STORE_TEST_BACKEND value: {other}. Use 'oxigraph' or 'blazegraph'."
        ),
    }
}

fn test_config_for_backend(
    backend: TestBackendKind,
    max_concurrent_operations: usize,
) -> TripleStoreManagerConfig {
    TripleStoreManagerConfig {
        backend: match backend {
            TestBackendKind::Oxigraph => TripleStoreBackendType::Oxigraph,
            TestBackendKind::Blazegraph => TripleStoreBackendType::Blazegraph,
        },
        url: std::env::var("BLAZEGRAPH_URL")
            .unwrap_or_else(|_| "http://localhost:9999".to_string()),
        username: None,
        password: None,
        connect_max_retries: if matches!(backend, TestBackendKind::Blazegraph) {
            3
        } else {
            1
        },
        connect_retry_frequency_ms: if matches!(backend, TestBackendKind::Blazegraph) {
            1000
        } else {
            10
        },
        timeouts: TimeoutConfig {
            query_ms: 10_000,
            insert_ms: 10_000,
            ask_ms: 10_000,
        },
        max_concurrent_operations,
        oxigraph: OxigraphStoreConfig::default(),
    }
}

fn test_config() -> TripleStoreManagerConfig {
    test_config_for_backend(selected_backend(), 4)
}

async fn setup_manager() -> (TripleStoreManager, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = test_config();
    let manager = match TripleStoreManager::connect(&config, temp_dir.path()).await {
        Ok(manager) => manager,
        Err(error) => {
            if matches!(selected_backend(), TestBackendKind::Blazegraph) {
                panic!(
                    "Failed to connect to Blazegraph: {error}. Ensure Blazegraph is running and BLAZEGRAPH_URL is correct."
                );
            }
            panic!("Failed to initialize Oxigraph backend: {error}");
        }
    };
    (manager, temp_dir)
}

#[tokio::test]
async fn insert_and_query_knowledge_collection() {
    let (manager, _temp_dir) = setup_manager().await;

    let kc_ual = "did:dkg:kc/test";
    let mut ka1 = KnowledgeAsset::new(
        format!("{}/1", kc_ual),
        vec!["<http://example.org/s1> <http://example.org/p1> \"o1\" .".to_string()],
    );
    ka1.set_private_triples(vec![
        "<http://example.org/s1> <http://example.org/p2> \"secret\" .".to_string(),
    ]);

    let ka2 = KnowledgeAsset::new(
        format!("{}/2", kc_ual),
        vec!["<http://example.org/s2> <http://example.org/p1> \"o2\" .".to_string()],
    );

    let metadata = KnowledgeCollectionMetadata::new(
        "0xabc".to_string(),
        42,
        "0xdeadbeef".to_string(),
        1_700_000_000,
    );

    let inserted = manager
        .insert_knowledge_collection(kc_ual, &[ka1.clone(), ka2.clone()], &Some(metadata), None)
        .await
        .unwrap();
    assert!(inserted > 0);

    let ka1_public = format!("{}/public", ka1.ual());
    let ka1_private = format!("{}/private", ka1.ual());
    let ka2_public = format!("{}/public", ka2.ual());
    let ka2_private = format!("{}/private", ka2.ual());

    assert!(manager.knowledge_asset_exists(&ka1_public).await.unwrap());
    assert!(manager.knowledge_asset_exists(&ka2_public).await.unwrap());
    assert!(manager.knowledge_asset_exists(&ka1_private).await.unwrap());
    assert!(!manager.knowledge_asset_exists(&ka2_private).await.unwrap());

    let ka1_public_triples = manager
        .get_knowledge_asset_named_graph(ka1.ual(), GraphVisibility::Public)
        .await
        .unwrap();
    assert!(
        ka1_public_triples
            .iter()
            .any(|line| line.contains("\"o1\""))
    );

    let kc_public_triples = manager
        .get_knowledge_collection_named_graphs(kc_ual, 1, 2, &[2], GraphVisibility::Public)
        .await
        .unwrap();
    assert!(kc_public_triples.iter().any(|line| line.contains("\"o1\"")));
    assert!(!kc_public_triples.iter().any(|line| line.contains("\"o2\"")));

    let existing = manager
        .knowledge_collections_exist_by_uals(&[
            kc_ual.to_string(),
            "did:dkg:kc/missing".to_string(),
        ])
        .await
        .unwrap();
    let expected: HashSet<String> = [kc_ual.to_string()].into_iter().collect();
    assert_eq!(existing, expected);

    let metadata_triples = manager.get_metadata(kc_ual).await.unwrap();
    assert!(metadata_triples.contains(predicates::PUBLISHED_BY));
    assert!(metadata_triples.contains(predicates::PUBLISHED_AT_BLOCK));
    assert!(metadata_triples.contains(predicates::PUBLISH_TX));
    assert!(metadata_triples.contains(predicates::BLOCK_TIME));
    assert!(metadata_triples.contains("http://schema.org/states"));
    let has_ka_count = metadata_triples
        .lines()
        .filter(|line| line.contains(predicates::HAS_KNOWLEDGE_ASSET))
        .count();
    assert_eq!(has_ka_count, 2);
    let states_count = metadata_triples
        .lines()
        .filter(|line| line.contains("http://schema.org/states"))
        .count();
    assert_eq!(states_count, 2);
}

#[tokio::test]
async fn knowledge_collections_exist_empty_input() {
    let (manager, _temp_dir) = setup_manager().await;
    let existing = manager
        .knowledge_collections_exist_by_uals(&[])
        .await
        .unwrap();
    assert!(existing.is_empty());
}

#[tokio::test]
async fn knowledge_asset_missing_returns_false() {
    let (manager, _temp_dir) = setup_manager().await;
    let exists = manager
        .knowledge_asset_exists("did:dkg:kc/missing/1/public")
        .await
        .unwrap();
    assert!(!exists);
}

#[tokio::test]
async fn get_named_graph_missing_returns_empty() {
    let (manager, _temp_dir) = setup_manager().await;
    let triples = manager
        .get_knowledge_asset_named_graph("did:dkg:kc/missing/1", GraphVisibility::Public)
        .await
        .unwrap();
    assert!(triples.is_empty());
}

#[tokio::test]
async fn private_named_graph_only_contains_private_triples() {
    let (manager, _temp_dir) = setup_manager().await;

    let kc_ual = "did:dkg:kc/private-only";
    let mut ka = KnowledgeAsset::new(
        format!("{}/1", kc_ual),
        vec!["<http://example.org/s1> <http://example.org/p1> \"public\" .".to_string()],
    );
    ka.set_private_triples(vec![
        "<http://example.org/s1> <http://example.org/p2> \"private\" .".to_string(),
    ]);

    manager
        .insert_knowledge_collection(kc_ual, std::slice::from_ref(&ka), &None, None)
        .await
        .unwrap();

    let private_triples = manager
        .get_knowledge_asset_named_graph(ka.ual(), GraphVisibility::Private)
        .await
        .unwrap();

    assert!(
        private_triples
            .iter()
            .any(|line| line.contains("\"private\""))
    );
    assert!(
        !private_triples
            .iter()
            .any(|line| line.contains("\"public\""))
    );
}

#[tokio::test]
async fn get_metadata_without_metadata_is_empty() {
    let (manager, _temp_dir) = setup_manager().await;

    let kc_ual = "did:dkg:kc/metadata-empty";
    let ka = KnowledgeAsset::new(
        format!("{}/1", kc_ual),
        vec!["<http://example.org/s1> <http://example.org/p1> \"o1\" .".to_string()],
    );

    manager
        .insert_knowledge_collection(kc_ual, &[ka], &None, None)
        .await
        .unwrap();

    let metadata = manager.get_metadata(kc_ual).await.unwrap();
    assert!(metadata.contains(predicates::HAS_KNOWLEDGE_ASSET));
    assert!(metadata.contains(predicates::HAS_NAMED_GRAPH));
    assert!(metadata.contains("http://schema.org/states"));
    assert!(!metadata.contains(predicates::PUBLISHED_BY));
    assert!(!metadata.contains(predicates::PUBLISHED_AT_BLOCK));
    assert!(!metadata.contains(predicates::PUBLISH_TX));
    assert!(!metadata.contains(predicates::BLOCK_TIME));
}

#[tokio::test]
async fn oxigraph_persists_across_reopen() {
    if matches!(selected_backend(), TestBackendKind::Blazegraph) {
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let config = test_config_for_backend(TestBackendKind::Oxigraph, 4);

    let kc_ual = "did:dkg:kc/persist";
    let ka = KnowledgeAsset::new(
        format!("{}/1", kc_ual),
        vec!["<http://example.org/s1> <http://example.org/p1> \"o1\" .".to_string()],
    );

    let manager = TripleStoreManager::connect(&config, temp_dir.path())
        .await
        .unwrap();
    manager
        .insert_knowledge_collection(kc_ual, &[ka], &None, None)
        .await
        .unwrap();

    drop(manager);

    let reopened = TripleStoreManager::connect(&config, temp_dir.path())
        .await
        .unwrap();
    let exists = reopened
        .knowledge_collections_exist_by_uals(&[kc_ual.to_string()])
        .await
        .unwrap();
    assert!(exists.contains(kc_ual));
}

#[tokio::test]
async fn metadata_contains_expected_values() {
    let (manager, _temp_dir) = setup_manager().await;

    let kc_ual = "did:dkg:kc/metadata-values";
    let ka = KnowledgeAsset::new(
        format!("{}/1", kc_ual),
        vec!["<http://example.org/s1> <http://example.org/p1> \"o1\" .".to_string()],
    );
    let meta = KnowledgeCollectionMetadata::new(
        "0xabc".to_string(),
        777,
        "0xdeadbeef".to_string(),
        1_700_000_000,
    );

    manager
        .insert_knowledge_collection(kc_ual, &[ka], &Some(meta), None)
        .await
        .unwrap();

    let metadata = manager.get_metadata(kc_ual).await.unwrap();
    assert!(metadata.contains("did:dkg:publisherKey/0xabc"));
    assert!(metadata.contains("777"));
    assert!(metadata.contains("0xdeadbeef"));
    assert!(metadata.contains("blockTime"));
}

#[tokio::test]
async fn public_queries_do_not_return_private_triples() {
    let (manager, _temp_dir) = setup_manager().await;

    let kc_ual = "did:dkg:kc/private-filter";
    let mut ka = KnowledgeAsset::new(
        format!("{}/1", kc_ual),
        vec!["<http://example.org/s1> <http://example.org/p1> \"public\" .".to_string()],
    );
    ka.set_private_triples(vec![
        "<http://example.org/s1> <http://example.org/p2> \"private\" .".to_string(),
    ]);

    manager
        .insert_knowledge_collection(kc_ual, &[ka], &None, None)
        .await
        .unwrap();

    let public_triples = manager
        .get_knowledge_collection_named_graphs(kc_ual, 1, 1, &[], GraphVisibility::Public)
        .await
        .unwrap();

    assert!(
        public_triples
            .iter()
            .any(|line| line.contains("\"public\""))
    );
    assert!(
        !public_triples
            .iter()
            .any(|line| line.contains("\"private\""))
    );
}

struct TimeoutBackend;

#[async_trait]
impl TripleStoreBackend for TimeoutBackend {
    fn name(&self) -> &'static str {
        "timeout-backend"
    }

    async fn health_check(&self) -> crate::error::Result<bool> {
        Ok(true)
    }

    async fn repository_exists(&self) -> crate::error::Result<bool> {
        Ok(true)
    }

    async fn create_repository(&self) -> crate::error::Result<()> {
        Ok(())
    }

    #[cfg(test)]
    async fn delete_repository(&self) -> crate::error::Result<()> {
        Ok(())
    }

    async fn update(&self, _query: &str, _timeout: Duration) -> crate::error::Result<()> {
        Err(crate::error::TripleStoreError::ConnectionFailed { attempts: 1 })
    }

    async fn construct(&self, _query: &str, _timeout: Duration) -> crate::error::Result<String> {
        Err(crate::error::TripleStoreError::ConnectionFailed { attempts: 1 })
    }

    async fn ask(&self, _query: &str, _timeout: Duration) -> crate::error::Result<bool> {
        Err(crate::error::TripleStoreError::ConnectionFailed { attempts: 1 })
    }

    async fn select(&self, _query: &str, _timeout: Duration) -> crate::error::Result<String> {
        Err(crate::error::TripleStoreError::ConnectionFailed { attempts: 1 })
    }
}

#[tokio::test]
async fn backend_error_propagates_from_operations() {
    let config = test_config_for_backend(TestBackendKind::Oxigraph, 1);
    let manager = TripleStoreManager::from_backend_for_tests(Box::new(TimeoutBackend), config);

    let result = manager.get_metadata("did:dkg:kc/fail").await;
    assert!(result.is_err());
}
#[tokio::test]
async fn knowledge_collections_exist_batches_over_200() {
    let (manager, _temp_dir) = setup_manager().await;

    let kc_ual = "did:dkg:kc/batch";
    let ka = KnowledgeAsset::new(
        format!("{}/1", kc_ual),
        vec!["<http://example.org/s1> <http://example.org/p1> \"o1\" .".to_string()],
    );

    manager
        .insert_knowledge_collection(kc_ual, &[ka], &None, None)
        .await
        .unwrap();

    let mut uals = Vec::with_capacity(201);
    uals.push(kc_ual.to_string());
    for i in 0..200 {
        uals.push(format!("did:dkg:kc/missing-{}", i));
    }

    let existing = manager
        .knowledge_collections_exist_by_uals(&uals)
        .await
        .unwrap();
    let expected: HashSet<String> = [kc_ual.to_string()].into_iter().collect();
    assert_eq!(existing, expected);
}

#[tokio::test]
async fn pagination_across_multiple_pages() {
    let (manager, _temp_dir) = setup_manager().await;

    let kc_ual = "did:dkg:kc/paginate";
    let mut assets = Vec::new();
    for id in 1..=60 {
        let ka = KnowledgeAsset::new(
            format!("{}/{}", kc_ual, id),
            vec![format!(
                "<http://example.org/s{0}> <http://example.org/p> \"o{0}\" .",
                id
            )],
        );
        assets.push(ka);
    }

    manager
        .insert_knowledge_collection(kc_ual, &assets, &None, None)
        .await
        .unwrap();

    let triples = manager
        .get_knowledge_collection_named_graphs(kc_ual, 1, 60, &[], GraphVisibility::Public)
        .await
        .unwrap();

    let mut found = HashSet::new();
    for id in 1..=60 {
        let needle = format!("\"o{id}\"");
        if triples.iter().any(|line| line.contains(&needle)) {
            found.insert(id);
        }
    }

    assert_eq!(found.len(), 60);
}

#[tokio::test]
async fn get_named_graphs_all_burned_returns_empty() {
    let (manager, _temp_dir) = setup_manager().await;

    let kc_ual = "did:dkg:kc/burned";
    let assets = (1..=3)
        .map(|id| {
            KnowledgeAsset::new(
                format!("{}/{}", kc_ual, id),
                vec![format!(
                    "<http://example.org/s{0}> <http://example.org/p> \"o{0}\" .",
                    id
                )],
            )
        })
        .collect::<Vec<_>>();

    manager
        .insert_knowledge_collection(kc_ual, &assets, &None, None)
        .await
        .unwrap();

    let burned = vec![1, 2, 3];
    let triples = manager
        .get_knowledge_collection_named_graphs(kc_ual, 1, 3, &burned, GraphVisibility::Public)
        .await
        .unwrap();

    assert!(triples.is_empty());
}

#[tokio::test]
async fn insert_invalid_triples_returns_error() {
    let (manager, _temp_dir) = setup_manager().await;

    let kc_ual = "did:dkg:kc/invalid";
    let ka = KnowledgeAsset::new(
        format!("{}/1", kc_ual),
        vec!["<http://example.org/s1> \"o1\" .".to_string()],
    );

    let result = manager
        .insert_knowledge_collection(kc_ual, &[ka], &None, None)
        .await;
    assert!(result.is_err());
}

struct TestBackend {
    current: Arc<AtomicUsize>,
    max_observed: Arc<AtomicUsize>,
    hold: Duration,
}

impl TestBackend {
    fn new(hold: Duration, current: Arc<AtomicUsize>, max_observed: Arc<AtomicUsize>) -> Self {
        Self {
            current,
            max_observed,
            hold,
        }
    }
}

#[async_trait]
impl TripleStoreBackend for TestBackend {
    fn name(&self) -> &'static str {
        "test"
    }

    async fn health_check(&self) -> crate::error::Result<bool> {
        Ok(true)
    }

    async fn repository_exists(&self) -> crate::error::Result<bool> {
        Ok(true)
    }

    async fn create_repository(&self) -> crate::error::Result<()> {
        Ok(())
    }

    #[cfg(test)]
    async fn delete_repository(&self) -> crate::error::Result<()> {
        Ok(())
    }

    async fn update(&self, _query: &str, _timeout: Duration) -> crate::error::Result<()> {
        let current = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_observed.fetch_max(current, Ordering::SeqCst);
        tokio::time::sleep(self.hold).await;
        self.current.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    async fn construct(&self, _query: &str, _timeout: Duration) -> crate::error::Result<String> {
        Ok(String::new())
    }

    async fn ask(&self, _query: &str, _timeout: Duration) -> crate::error::Result<bool> {
        Ok(false)
    }

    async fn select(&self, _query: &str, _timeout: Duration) -> crate::error::Result<String> {
        Ok("{\"results\":{\"bindings\":[]}}".to_string())
    }
}

struct InvalidSelectBackend;

#[async_trait]
impl TripleStoreBackend for InvalidSelectBackend {
    fn name(&self) -> &'static str {
        "invalid-select"
    }

    async fn health_check(&self) -> crate::error::Result<bool> {
        Ok(true)
    }

    async fn repository_exists(&self) -> crate::error::Result<bool> {
        Ok(true)
    }

    async fn create_repository(&self) -> crate::error::Result<()> {
        Ok(())
    }

    #[cfg(test)]
    async fn delete_repository(&self) -> crate::error::Result<()> {
        Ok(())
    }

    async fn update(&self, _query: &str, _timeout: Duration) -> crate::error::Result<()> {
        Ok(())
    }

    async fn construct(&self, _query: &str, _timeout: Duration) -> crate::error::Result<String> {
        Ok(String::new())
    }

    async fn ask(&self, _query: &str, _timeout: Duration) -> crate::error::Result<bool> {
        Ok(false)
    }

    async fn select(&self, _query: &str, _timeout: Duration) -> crate::error::Result<String> {
        Ok("not-json".to_string())
    }
}

#[tokio::test]
async fn knowledge_collections_exist_invalid_select_returns_error() {
    let config = test_config_for_backend(TestBackendKind::Oxigraph, 1);
    let manager =
        TripleStoreManager::from_backend_for_tests(Box::new(InvalidSelectBackend), config);

    let result = manager
        .knowledge_collections_exist_by_uals(&["did:dkg:kc/1".to_string()])
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn concurrency_limiter_serializes_updates() {
    let current = Arc::new(AtomicUsize::new(0));
    let max_observed = Arc::new(AtomicUsize::new(0));
    let backend = TestBackend::new(
        Duration::from_millis(50),
        Arc::clone(&current),
        Arc::clone(&max_observed),
    );

    let config = test_config_for_backend(TestBackendKind::Oxigraph, 1);
    let manager = TripleStoreManager::from_backend_for_tests(Box::new(backend), config);
    let manager = Arc::new(manager);

    let kc_ual = "did:dkg:kc/concurrency";
    let ka = KnowledgeAsset::new(
        format!("{}/1", kc_ual),
        vec!["<http://example.org/s1> <http://example.org/p> \"o1\" .".to_string()],
    );

    let mut tasks = Vec::new();
    for _ in 0..5 {
        let manager = Arc::clone(&manager);
        let ka = ka.clone();
        tasks.push(tokio::spawn(async move {
            manager
                .insert_knowledge_collection(kc_ual, &[ka], &None, None)
                .await
                .unwrap();
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(max_observed.load(Ordering::SeqCst), 1);
}
