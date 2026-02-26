use std::collections::HashSet;

use dkg_domain::{KnowledgeCollectionMetadata, ParsedUal, TokenIds};
use dkg_repository::KcChainMetadataEntry;
use dkg_triple_store::{MetadataAsset, MetadataTriples};

use crate::application::state_metadata::PrivateGraphPresence;

pub(super) fn reconstruct_metadata_triples(
    parsed_ual: &ParsedUal,
    token_ids: &TokenIds,
    metadata: &KcChainMetadataEntry,
    private_presence: &PrivateGraphPresence,
) -> Vec<String> {
    let kc_ual = parsed_ual.knowledge_collection_ual();
    let burned: HashSet<u64> = token_ids.burned().iter().copied().collect();

    let mut metadata_assets = Vec::new();
    for token_id in token_ids.start_token_id()..=token_ids.end_token_id() {
        if burned.contains(&token_id) {
            continue;
        }
        metadata_assets.push(MetadataAsset {
            ka_ual: format!("{kc_ual}/{token_id}"),
            has_private_graph: private_presence.has_private_graph(token_id),
        });
    }

    let kc_metadata = KnowledgeCollectionMetadata::new(
        metadata.publisher_address.clone(),
        metadata.block_number,
        metadata.transaction_hash.clone(),
        metadata.block_timestamp,
    );
    let built = MetadataTriples::build(&kc_ual, &metadata_assets, Some(&kc_metadata));
    built.all_triples()
}
