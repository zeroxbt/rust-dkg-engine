use std::{collections::HashMap, sync::Arc};

use libp2p::PeerId;
use uuid::Uuid;

use super::BATCH_GET_UAL_MAX_LIMIT;
use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        network::{
            NetworkManager, ResponseMessage,
            message::{ResponseBody, ResponseMessageHeader, ResponseMessageType},
            messages::BatchGetAck,
            request_response::ResponseChannel,
        },
        triple_store::{Assertion, TokenIds},
    },
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
    network_manager: Arc<NetworkManager>,
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

    async fn send_response(
        &self,
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        operation_id: Uuid,
        message: ResponseMessage<BatchGetAck>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_batch_get_response(channel, message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send batch get response"
            );
        }
    }

    async fn send_ack(
        &self,
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        operation_id: Uuid,
        assertions: HashMap<String, Assertion>,
        metadata: HashMap<String, Vec<String>>,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Ack),
            data: ResponseBody::ack(BatchGetAck {
                assertions,
                metadata,
            }),
        };
        self.send_response(channel, operation_id, message).await;
    }

    async fn send_nack(
        &self,
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        operation_id: Uuid,
        message: impl Into<String>,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Nack),
            data: ResponseBody::error(message),
        };
        self.send_response(channel, operation_id, message).await;
    }
}

impl CommandHandler<HandleBatchGetRequestCommandData> for HandleBatchGetRequestCommandHandler {
    async fn execute(&self, data: &HandleBatchGetRequestCommandData) -> CommandExecutionResult {
        let operation_id = data.operation_id;
        let remote_peer_id = &data.remote_peer_id;

        tracing::info!(
            operation_id = %operation_id,
            remote_peer_id = %remote_peer_id,
            ual_count = data.uals.len(),
            include_metadata = data.include_metadata,
            "Starting HandleBatchGetRequest command"
        );

        // Retrieve the response channel
        let Some(channel) = self
            .response_channels
            .retrieve(remote_peer_id, operation_id)
        else {
            tracing::error!(
                operation_id = %operation_id,
                peer = %remote_peer_id,
                "No cached response channel found. Channel may have expired."
            );
            return CommandExecutionResult::Completed;
        };

        // Apply UAL limit
        let uals: Vec<String> = data
            .uals
            .iter()
            .take(BATCH_GET_UAL_MAX_LIMIT)
            .cloned()
            .collect();

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

        tracing::info!(
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
