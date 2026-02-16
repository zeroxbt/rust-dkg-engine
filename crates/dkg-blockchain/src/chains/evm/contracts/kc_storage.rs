// Generated bindings include methods with many parameters; allow for this module.
#[allow(clippy::too_many_arguments)]
pub mod knowledge_collection_storage {
    use alloy::sol;

    sol!(
        #[derive(Debug)]
        #[sol(rpc)]
        KnowledgeCollectionStorage,
        "abi/KnowledgeCollectionStorage.json"
    );
}

pub use knowledge_collection_storage::KnowledgeCollectionStorage;
