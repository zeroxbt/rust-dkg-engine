use dkg_network::{PeerId, StoreResponseData};
use uuid::Uuid;

use super::handler::SendPublishStoreRequestsCommandHandler;
use crate::operations::{PublishStoreOperationResult, PublishStoreSignatureData};

impl SendPublishStoreRequestsCommandHandler {
    /// Process a store response and store the signature if valid.
    ///
    /// Returns true if the response is a valid ACK with signature data.
    pub(crate) async fn process_store_response(
        &self,
        operation_id: Uuid,
        peer: &PeerId,
        response: &StoreResponseData,
    ) -> bool {
        match response {
            StoreResponseData::Ack(ack) => {
                let identity_id = ack.identity_id;
                let signature = &ack.signature;
                let sig_data = PublishStoreSignatureData::new(
                    identity_id.to_string(),
                    signature.v,
                    signature.r.clone(),
                    signature.s.clone(),
                    signature.vs.clone(),
                );

                // Store signature incrementally to redb
                if let Err(e) = self
                    .publish_store_operation_tracking
                    .update_result(
                        operation_id,
                        PublishStoreOperationResult::new(None, Vec::new()),
                        |result| {
                            result.network_signatures.push(sig_data);
                        },
                    )
                    .await
                {
                    tracing::error!(
                        operation_id = %operation_id,
                        peer = %peer,
                        error = %e,
                        "Failed to store network signature"
                    );
                    return false;
                }

                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %peer,
                    identity_id = %identity_id,
                    "Signature stored successfully"
                );
                true
            }
            StoreResponseData::Error(err) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %peer,
                    error = %err.error_message,
                    "Peer returned error response"
                );
                false
            }
        }
    }
}
