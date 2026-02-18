use std::sync::Arc;

use dkg_domain::{ParsedUal, Visibility};
use dkg_network::{GetAck, GetRequestData, GetResponseData, NetworkError, NetworkManager, PeerId};
use futures::{StreamExt, stream::FuturesUnordered};
use uuid::Uuid;

use crate::application::AssertionValidation;

use super::config::NETWORK_CONCURRENT_PEERS;

pub(super) async fn fetch_first_valid_ack_from_peers(
    network_manager: Arc<NetworkManager>,
    assertion_validation: Arc<AssertionValidation>,
    peers: Vec<PeerId>,
    operation_id: Uuid,
    request_data: GetRequestData,
    parsed_ual: ParsedUal,
    visibility: Visibility,
) -> Option<GetAck> {
    let mut futures = FuturesUnordered::new();
    let mut peers_iter = peers.iter().cloned();
    let limit = NETWORK_CONCURRENT_PEERS.max(1).min(peers.len());

    for _ in 0..limit {
        if let Some(peer) = peers_iter.next() {
            futures.push(send_get_request_to_peer(
                Arc::clone(&network_manager),
                peer,
                operation_id,
                request_data.clone(),
            ));
        }
    }

    while let Some((peer, response)) = futures.next().await {
        if let Some(ack) = validate_get_ack(
            &peer,
            response,
            assertion_validation.as_ref(),
            &parsed_ual,
            visibility,
        )
        .await
        {
            return Some(ack);
        }

        if let Some(peer) = peers_iter.next() {
            futures.push(send_get_request_to_peer(
                Arc::clone(&network_manager),
                peer,
                operation_id,
                request_data.clone(),
            ));
        }
    }

    None
}

async fn validate_get_ack(
    peer: &PeerId,
    response: Result<GetResponseData, NetworkError>,
    assertion_validation: &AssertionValidation,
    parsed_ual: &ParsedUal,
    visibility: Visibility,
) -> Option<GetAck> {
    let response = match response {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(peer = %peer, error = %e, "Get request failed");
            return None;
        }
    };

    match response {
        GetResponseData::Ack(ack) => {
            let is_valid = assertion_validation
                .validate_response(&ack.assertion, parsed_ual, visibility)
                .await;
            if !is_valid {
                tracing::debug!(peer = %peer, "Ack failed assertion validation");
                return None;
            }
            Some(ack)
        }
        GetResponseData::Error(err) => {
            tracing::debug!(peer = %peer, error = %err.error_message, "Peer returned NACK");
            None
        }
    }
}

async fn send_get_request_to_peer(
    network_manager: Arc<NetworkManager>,
    peer: PeerId,
    operation_id: Uuid,
    request_data: GetRequestData,
) -> (PeerId, Result<GetResponseData, NetworkError>) {
    let result = network_manager
        .send_get_request(peer, operation_id, request_data)
        .await;
    (peer, result)
}
