use std::sync::Arc;

use dkg_domain::{Assertion, KnowledgeCollectionMetadata, parse_ual};
use dkg_repository::{KcChainMetadataRepository, KcProjectionRepository};
use dkg_triple_store::error::TripleStoreError;

use super::{
    TripleStoreAssertions, assertions::build_assets::build_knowledge_assets,
    state_metadata::encode_private_graph_presence,
};

pub(crate) struct KcMaterializationService {
    triple_store_assertions: Arc<TripleStoreAssertions>,
    kc_chain_metadata_repository: KcChainMetadataRepository,
    kc_projection_repository: KcProjectionRepository,
}

impl KcMaterializationService {
    pub(crate) fn new(
        triple_store_assertions: Arc<TripleStoreAssertions>,
        kc_chain_metadata_repository: KcChainMetadataRepository,
        kc_projection_repository: KcProjectionRepository,
    ) -> Self {
        Self {
            triple_store_assertions,
            kc_chain_metadata_repository,
            kc_projection_repository,
        }
    }

    pub(crate) async fn insert_knowledge_collection(
        &self,
        knowledge_collection_ual: &str,
        dataset: &Assertion,
        metadata: &Option<KnowledgeCollectionMetadata>,
        paranet_ual: Option<&str>,
    ) -> Result<usize, TripleStoreError> {
        let projection_key = projection_key_from_ual(knowledge_collection_ual);
        if let Some((blockchain_id, contract_address, kc_id)) = projection_key.as_ref()
            && let Err(error) = self
                .kc_projection_repository
                .ensure_desired_present(blockchain_id, contract_address, &[*kc_id])
                .await
        {
            tracing::warn!(
                blockchain_id = blockchain_id,
                contract_address = contract_address,
                kc_id = *kc_id,
                error = %error,
                "Failed to upsert projection desired state before materialization"
            );
        }

        let inserted = match self
            .triple_store_assertions
            .insert_knowledge_collection(knowledge_collection_ual, dataset, metadata, paranet_ual)
            .await
        {
            Ok(inserted) => inserted,
            Err(error) => {
                if let Some((blockchain_id, contract_address, kc_id)) = projection_key.as_ref()
                    && let Err(mark_error) = self
                        .kc_projection_repository
                        .mark_failed(
                            blockchain_id,
                            contract_address,
                            &[*kc_id],
                            "triple_store_insert_failed",
                        )
                        .await
                {
                    tracing::warn!(
                        blockchain_id = blockchain_id,
                        contract_address = contract_address,
                        kc_id = *kc_id,
                        error = %mark_error,
                        "Failed to mark projection row as failed after insert error"
                    );
                }
                return Err(error);
            }
        };

        if let Err(error) = self
            .persist_private_graph_encoding(knowledge_collection_ual, dataset, metadata)
            .await
        {
            tracing::warn!(
                ual = %knowledge_collection_ual,
                error = %error,
                "Failed to persist private graph encoding after successful triple store insert"
            );
        }

        if let Some((blockchain_id, contract_address, kc_id)) = projection_key.as_ref()
            && let Err(error) = self
                .kc_projection_repository
                .mark_present(blockchain_id, contract_address, &[*kc_id])
                .await
        {
            tracing::warn!(
                blockchain_id = blockchain_id,
                contract_address = contract_address,
                kc_id = *kc_id,
                error = %error,
                "Failed to mark projection row as present after materialization"
            );
        }

        Ok(inserted)
    }

    async fn persist_private_graph_encoding(
        &self,
        knowledge_collection_ual: &str,
        dataset: &Assertion,
        metadata: &Option<KnowledgeCollectionMetadata>,
    ) -> Result<(), TripleStoreError> {
        let parsed_ual = match parse_ual(knowledge_collection_ual) {
            Ok(parsed) => parsed,
            Err(error) => {
                if metadata.is_some() {
                    return Err(TripleStoreError::Other(format!(
                        "Failed to parse KC UAL '{knowledge_collection_ual}' for private graph metadata persistence: {error}"
                    )));
                }
                return Ok(());
            }
        };

        let Ok(kc_id) = u64::try_from(parsed_ual.knowledge_collection_id) else {
            if metadata.is_some() {
                return Err(TripleStoreError::Other(format!(
                    "KC id out of range for private graph metadata persistence: ual={knowledge_collection_ual}, kc_id={}",
                    parsed_ual.knowledge_collection_id
                )));
            }
            return Ok(());
        };

        let knowledge_assets = build_knowledge_assets(knowledge_collection_ual, dataset)?;
        let private_graph_encoding = encode_private_graph_presence(&knowledge_assets);
        let contract_address = format!("{:?}", parsed_ual.contract);

        let persist_result = self
            .kc_chain_metadata_repository
            .upsert_private_graph_encoding(
                parsed_ual.blockchain.as_str(),
                &contract_address,
                kc_id,
                Some(private_graph_encoding.mode as u32),
                private_graph_encoding.payload.as_deref(),
                Some("triple_store_insert"),
            )
            .await;

        if metadata.is_some() {
            if let Err(error) = persist_result {
                return Err(TripleStoreError::Other(format!(
                    "Failed to persist private graph metadata in kc_chain_state_metadata for ual={knowledge_collection_ual}: {error}"
                )));
            }
        } else if let Err(error) = persist_result {
            tracing::warn!(
                blockchain_id = %parsed_ual.blockchain,
                contract_address = %contract_address,
                kc_id = kc_id,
                error = %error,
                "Failed to persist private graph encoding"
            );
        }

        Ok(())
    }
}

fn projection_key_from_ual(ual: &str) -> Option<(String, String, u64)> {
    let parsed = parse_ual(ual).ok()?;
    let kc_id = u64::try_from(parsed.knowledge_collection_id).ok()?;
    Some((
        parsed.blockchain.as_str().to_string(),
        format!("{:?}", parsed.contract),
        kc_id,
    ))
}
