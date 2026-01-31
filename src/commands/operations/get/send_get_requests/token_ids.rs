use uuid::Uuid;

use super::handler::SendGetRequestsCommandHandler;
use crate::{
    managers::triple_store::{MAX_TOKENS_PER_KC, TokenIds},
    types::ParsedUal,
};

impl SendGetRequestsCommandHandler {
    pub(crate) async fn resolve_token_ids(
        &self,
        operation_id: Uuid,
        parsed_ual: &ParsedUal,
    ) -> TokenIds {
        if let Some(token_id) = parsed_ual.knowledge_asset_id {
            return TokenIds::single(token_id as u64);
        }

        // Get token IDs range from blockchain
        let chain_range = match self
            .blockchain_manager
            .get_knowledge_assets_range(
                &parsed_ual.blockchain,
                parsed_ual.contract,
                parsed_ual.knowledge_collection_id,
            )
            .await
        {
            Ok(range) => {
                if let Some((start, end, ref burned)) = range {
                    tracing::debug!(
                        operation_id = %operation_id,
                        start_token_id = start,
                        end_token_id = end,
                        burned_count = burned.len(),
                        "Retrieved knowledge assets range from chain"
                    );
                }
                range
            }
            Err(e) => {
                // Fallback for old ContentAssetStorage contracts
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to get knowledge assets range, using fallback"
                );
                None
            }
        };

        // Use on-chain data if available
        match chain_range {
            Some((global_start, global_end, global_burned)) => {
                // Convert global token IDs to local 1-based indices
                // Global: (kc_id - 1) * 1_000_000 + local_id
                // Local: global - (kc_id - 1) * 1_000_000
                let offset = (parsed_ual.knowledge_collection_id as u64 - 1) * MAX_TOKENS_PER_KC;
                let local_start = global_start.saturating_sub(offset);
                let local_end = global_end.saturating_sub(offset);
                let local_burned: Vec<u64> = global_burned
                    .into_iter()
                    .map(|b| b.saturating_sub(offset))
                    .collect();
                TokenIds::new(local_start, local_end, local_burned)
            }
            None => {
                // Fallback for old ContentAssetStorage contracts
                TokenIds::single(1)
            }
        }
    }
}
