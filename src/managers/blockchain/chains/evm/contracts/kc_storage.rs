// Generated bindings include methods with many parameters; allow for this module.
#[allow(clippy::too_many_arguments)]
pub(crate) mod knowledge_collection_storage {
    use alloy::sol;

    sol!(
        #[derive(Debug)]
        #[sol(rpc)]
        KnowledgeCollectionStorage,
        "abi/KnowledgeCollectionStorage.json"
    );
}

pub(crate) use knowledge_collection_storage::KnowledgeCollectionStorage;
