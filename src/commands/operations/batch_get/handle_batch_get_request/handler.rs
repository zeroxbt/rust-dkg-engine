use std::{collections::HashMap, sync::Arc};

use libp2p::PeerId;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        network::{NetworkManager, messages::BatchGetAck},
        triple_store::{Assertion, TokenIds},
    },
    operations::protocols::batch_get,
    services::{ResponseChannels, TripleStoreService},
    types::{ParsedUal, Visibility, parse_ual},
};

/// Command data for handling incoming batch get requests.
#[derive(Clone)]
pub(crate) struct HandleBatchGetRequestCommandData {
    pub operation_id: Uuid,
    pub uals: Vec<String>,
    pub token_ids: HashMap<String, TokenIds>,
    pub include_metadata: bool,
    pub remote_peer_id: PeerId,
}

impl HandleBatchGetRequestCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        uals: Vec<String>,
        token_ids: HashMap<String, TokenIds>,
        include_metadata: bool,
        remote_peer_id: PeerId,
    ) -> Self {
        Self {
            operation_id,
            uals,
            token_ids,
            include_metadata,
            remote_peer_id,
        }
    }
}

pub(crate) struct HandleBatchGetRequestCommandHandler {
    pub(super) network_manager: Arc<NetworkManager>,
    triple_store_service: Arc<TripleStoreService>,
    response_channels: Arc<ResponseChannels<BatchGetAck>>,
}

impl HandleBatchGetRequestCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            network_manager: Arc::clone(context.network_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            response_channels: Arc::clone(context.batch_get_response_channels()),
        }
    }
}

impl CommandHandler<HandleBatchGetRequestCommandData> for HandleBatchGetRequestCommandHandler {
    #[instrument(
        name = "op.batch_get.recv",
        skip(self, data),
        fields(
            operation_id = %data.operation_id,
            protocol = "batch_get",
            direction = "recv",
            remote_peer = %data.remote_peer_id,
            include_metadata = data.include_metadata,
            ual_count = tracing::field::Empty,
        )
    )]
    async fn execute(&self, data: &HandleBatchGetRequestCommandData) -> CommandExecutionResult {
        let operation_id = data.operation_id;
        let remote_peer_id = &data.remote_peer_id;

        // Retrieve the response channel
        let Some(channel) = self
            .response_channels
            .retrieve(remote_peer_id, operation_id)
        else {
            tracing::warn!(
                operation_id = %operation_id,
                peer = %remote_peer_id,
                "Response channel not found; request may have expired"
            );
            return CommandExecutionResult::Completed;
        };

        // Apply UAL limit
        let uals: Vec<String> = data
            .uals
            .iter()
            .take(batch_get::UAL_MAX_LIMIT)
            .cloned()
            .collect();
        tracing::Span::current().record("ual_count", &tracing::field::display(uals.len()));

        // Parse UALs and pair with token IDs
        let mut uals_with_token_ids: Vec<(ParsedUal, TokenIds)> = Vec::new();

        for ual in &uals {
            let parsed_ual = match parse_ual(ual) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(
                        operation_id = %operation_id,
                        ual = %ual,
                        error = %e,
                        "Failed to parse UAL, skipping"
                    );
                    continue;
                }
            };

            // Get token IDs from request or use default
            let token_ids = data
                .token_ids
                .get(ual)
                .cloned()
                .unwrap_or_else(|| TokenIds::single(1));

            uals_with_token_ids.push((parsed_ual, token_ids));
        }

        // Query local triple store in batch
        // Always query public visibility for remote requests
        let query_results = self
            .triple_store_service
            .query_assertions_batch(
                &uals_with_token_ids,
                Visibility::Public,
                data.include_metadata,
            )
            .await;

        let query_results = match query_results {
            Ok(results) => results,
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Batch get query failed"
                );
                self.send_nack(
                    channel,
                    operation_id,
                    format!("Triple store query failed: {}", e),
                )
                .await;
                return CommandExecutionResult::Completed;
            }
        };

        // Build response maps
        let mut assertions: HashMap<String, Assertion> = HashMap::new();
        let mut metadata: HashMap<String, Vec<String>> = HashMap::new();

        for (ual, result) in query_results {
            let assertion = result.assertion.clone();
            assertions.insert(ual.clone(), assertion);

            if let Some(meta) = result.metadata {
                metadata.insert(ual, meta);
            }
        }

        tracing::debug!(
            operation_id = %operation_id,
            assertions_count = assertions.len(),
            metadata_count = metadata.len(),
            "Sending batch get response"
        );

        self.send_ack(channel, operation_id, assertions, metadata)
            .await;

        CommandExecutionResult::Completed
    }
}
