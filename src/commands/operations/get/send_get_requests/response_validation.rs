use libp2p::PeerId;
use uuid::Uuid;

use super::handler::SendGetRequestsCommandHandler;
use crate::{
    managers::{
        network::{message::ResponseBody, messages::GetResponseData},
        triple_store::Assertion,
    },
    operations::GetOperationResult,
    types::{ParsedUal, Visibility},
};

impl SendGetRequestsCommandHandler {
    /// Validate a get response and store the result if valid.
    ///
    /// Returns true if the response is valid and was stored successfully.
    pub(crate) async fn validate_and_store_response(
        &self,
        operation_id: Uuid,
        peer: &PeerId,
        response: &GetResponseData,
        parsed_ual: &ParsedUal,
        visibility: Visibility,
    ) -> bool {
        match response {
            ResponseBody::Ack(ack) => {
                let assertion = &ack.assertion;
                let metadata = &ack.metadata;
                // Validate the assertion
                let is_valid = self
                    .get_validation_service
                    .validate_response(assertion, parsed_ual, visibility)
                    .await;

                if !is_valid {
                    tracing::debug!(
                        operation_id = %operation_id,
                        peer = %peer,
                        "Response validation failed"
                    );
                    return false;
                }

                // Build and store the result
                let get_result = GetOperationResult::new(
                    Assertion::new(assertion.public.clone(), assertion.private.clone()),
                    metadata.clone(),
                );

                match self
                    .get_operation_status_service
                    .store_result(operation_id, &get_result)
                {
                    Ok(()) => {
                        tracing::debug!(
                            operation_id = %operation_id,
                            peer = %peer,
                            public_count = assertion.public.len(),
                            has_private = assertion.private.is_some(),
                            "Response validated and stored"
                        );
                        true
                    }
                    Err(e) => {
                        tracing::error!(
                            operation_id = %operation_id,
                            peer = %peer,
                            error = %e,
                            "Failed to store result"
                        );
                        false
                    }
                }
            }
            ResponseBody::Error(err) => {
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
