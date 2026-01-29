use std::{collections::HashSet, sync::Arc};

use libp2p::PeerId;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::{AccessPolicy, BlockchainManager},
        network::{
            NetworkManager, ResponseMessage,
            message::{ResponseMessageHeader, ResponseMessageType},
            messages::GetResponseData,
            request_response::ResponseChannel,
        },
        triple_store::{Assertion, TokenIds, Visibility},
    },
    services::{ResponseChannels, TripleStoreService},
    utils::{
        paranet::{construct_knowledge_collection_onchain_id, construct_paranet_id},
        ual::{ParsedUal, parse_ual},
    },
};

/// Command data for handling incoming get requests.
#[derive(Clone)]
pub(crate) struct HandleGetRequestCommandData {
    pub operation_id: Uuid,
    pub ual: String,
    pub token_ids: TokenIds,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
    pub remote_peer_id: PeerId,
}

impl HandleGetRequestCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        ual: String,
        token_ids: TokenIds,
        include_metadata: bool,
        paranet_ual: Option<String>,
        remote_peer_id: PeerId,
    ) -> Self {
        Self {
            operation_id,
            ual,
            token_ids,
            include_metadata,
            paranet_ual,
            remote_peer_id,
        }
    }
}

pub(crate) struct HandleGetRequestCommandHandler {
    network_manager: Arc<NetworkManager>,
    triple_store_service: Arc<TripleStoreService>,
    response_channels: Arc<ResponseChannels<GetResponseData>>,
    blockchain_manager: Arc<BlockchainManager>,
}

impl HandleGetRequestCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            network_manager: Arc::clone(context.network_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            response_channels: Arc::clone(context.get_response_channels()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
        }
    }

    async fn send_response(
        &self,
        channel: ResponseChannel<ResponseMessage<GetResponseData>>,
        operation_id: Uuid,
        message: ResponseMessage<GetResponseData>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_get_response(channel, message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send get response"
            );
        }
    }

    async fn send_nack(
        &self,
        channel: ResponseChannel<ResponseMessage<GetResponseData>>,
        operation_id: Uuid,
        error_message: &str,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Nack),
            data: GetResponseData::nack(error_message),
        };
        self.send_response(channel, operation_id, message).await;
    }

    async fn send_ack(
        &self,
        channel: ResponseChannel<ResponseMessage<GetResponseData>>,
        operation_id: Uuid,
        assertion: Assertion,
        metadata: Option<Vec<String>>,
    ) {
        let message = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Ack),
            data: GetResponseData::ack(assertion, metadata),
        };
        self.send_response(channel, operation_id, message).await;
    }

    /// Determine effective visibility based on paranet authorization.
    ///
    /// For PERMISSIONED paranets where both remote and local peers are authorized,
    /// returns `Visibility::All` (public + private data).
    /// Otherwise returns `Visibility::Public`.
    async fn determine_visibility_for_paranet(
        &self,
        target_ual: &ParsedUal,
        paranet_ual: Option<&str>,
        remote_peer_id: &PeerId,
    ) -> Visibility {
        let Some(paranet_ual) = paranet_ual else {
            return Visibility::Public;
        };

        // Parse paranet UAL
        let Ok(paranet_parsed) = parse_ual(paranet_ual) else {
            tracing::debug!(
                paranet_ual = %paranet_ual,
                "Failed to parse paranet UAL, using Public visibility"
            );
            return Visibility::Public;
        };

        let Some(ka_id) = paranet_parsed.knowledge_asset_id else {
            tracing::debug!(
                paranet_ual = %paranet_ual,
                "Paranet UAL missing knowledge_asset_id, using Public visibility"
            );
            return Visibility::Public;
        };

        // Construct paranet ID
        let paranet_id = construct_paranet_id(
            paranet_parsed.contract,
            paranet_parsed.knowledge_collection_id,
            ka_id,
        );

        // Get access policy
        let Ok(policy) = self
            .blockchain_manager
            .get_nodes_access_policy(&target_ual.blockchain, paranet_id)
            .await
        else {
            tracing::debug!(
                paranet_id = %paranet_id,
                "Failed to get access policy, using Public visibility"
            );
            return Visibility::Public;
        };

        if policy != AccessPolicy::Permissioned {
            tracing::debug!(
                paranet_id = %paranet_id,
                policy = ?policy,
                "Paranet is not PERMISSIONED, using Public visibility"
            );
            return Visibility::Public;
        }

        // For PERMISSIONED: verify KC is registered
        let kc_onchain_id = construct_knowledge_collection_onchain_id(
            target_ual.contract,
            target_ual.knowledge_collection_id,
        );

        let Ok(kc_registered) = self
            .blockchain_manager
            .is_knowledge_collection_registered(&target_ual.blockchain, paranet_id, kc_onchain_id)
            .await
        else {
            tracing::debug!(
                paranet_id = %paranet_id,
                "Failed to check KC registration, using Public visibility"
            );
            return Visibility::Public;
        };

        if !kc_registered {
            tracing::debug!(
                paranet_id = %paranet_id,
                kc_id = target_ual.knowledge_collection_id,
                "Knowledge collection not registered in paranet, using Public visibility"
            );
            return Visibility::Public;
        }

        // Get permissioned nodes and check both peers
        let Ok(permissioned_nodes) = self
            .blockchain_manager
            .get_permissioned_nodes(&target_ual.blockchain, paranet_id)
            .await
        else {
            tracing::debug!(
                paranet_id = %paranet_id,
                "Failed to get permissioned nodes, using Public visibility"
            );
            return Visibility::Public;
        };

        let permissioned_peer_ids: HashSet<PeerId> = permissioned_nodes
            .iter()
            .filter_map(|node| {
                String::from_utf8(node.node_id.clone())
                    .ok()
                    .and_then(|s| s.parse::<PeerId>().ok())
            })
            .collect();

        // Check remote peer is authorized
        if !permissioned_peer_ids.contains(remote_peer_id) {
            tracing::debug!(
                peer = %remote_peer_id,
                paranet_id = %paranet_id,
                "Remote peer not in permissioned list, using Public visibility"
            );
            return Visibility::Public;
        }

        // Check local node is authorized
        let my_peer_id = self.network_manager.peer_id();
        if !permissioned_peer_ids.contains(my_peer_id) {
            tracing::debug!(
                paranet_id = %paranet_id,
                "Local node not in permissioned list, using Public visibility"
            );
            return Visibility::Public;
        }

        // Both peers authorized - return ALL data (public + private)
        tracing::info!(
            paranet_id = %paranet_id,
            remote_peer = %remote_peer_id,
            "Both peers authorized for permissioned paranet, using All visibility"
        );
        Visibility::All
    }
}

impl CommandHandler<HandleGetRequestCommandData> for HandleGetRequestCommandHandler {
    async fn execute(&self, data: &HandleGetRequestCommandData) -> CommandExecutionResult {
        let operation_id = data.operation_id;
        let ual = &data.ual;
        let remote_peer_id = &data.remote_peer_id;

        tracing::info!(
            operation_id = %operation_id,
            ual = %ual,
            remote_peer_id = %remote_peer_id,
            include_metadata = data.include_metadata,
            has_paranet = data.paranet_ual.is_some(),
            "Starting HandleGetRequest command"
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

        // Parse the UAL
        let parsed_ual = match parse_ual(ual) {
            Ok(parsed) => parsed,
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    ual = %ual,
                    error = %e,
                    "Failed to parse UAL - sending NACK"
                );
                self.send_nack(channel, operation_id, &format!("Invalid UAL: {}", e))
                    .await;
                return CommandExecutionResult::Completed;
            }
        };

        // Determine effective visibility based on paranet authorization
        // For PERMISSIONED paranets where both peers are authorized, returns All visibility
        // Otherwise returns Public visibility
        let effective_visibility = self
            .determine_visibility_for_paranet(
                &parsed_ual,
                data.paranet_ual.as_deref(),
                remote_peer_id,
            )
            .await;

        tracing::debug!(
            operation_id = %operation_id,
            effective_visibility = ?effective_visibility,
            "Determined effective visibility for query"
        );

        // Query the triple store using the shared service
        let query_result = self
            .triple_store_service
            .query_assertion(
                &parsed_ual,
                &data.token_ids,
                effective_visibility,
                data.include_metadata,
            )
            .await;

        match query_result {
            Some(result) if result.assertion.has_data() => {
                tracing::info!(
                    operation_id = %operation_id,
                    ual = %ual,
                    public_count = result.assertion.public.len(),
                    has_private = result.assertion.private.is_some(),
                    has_metadata = result.metadata.is_some(),
                    "Found assertion data - sending ACK"
                );

                self.send_ack(channel, operation_id, result.assertion, result.metadata)
                    .await;
            }
            _ => {
                tracing::debug!(
                    operation_id = %operation_id,
                    ual = %ual,
                    "No assertion data found - sending NACK"
                );
                self.send_nack(
                    channel,
                    operation_id,
                    &format!("Unable to find assertion {}", ual),
                )
                .await;
            }
        }

        CommandExecutionResult::Completed
    }
}
