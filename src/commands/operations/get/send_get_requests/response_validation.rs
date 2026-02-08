use libp2p::PeerId;
use uuid::Uuid;

use super::handler::SendGetRequestsCommandHandler;
use crate::{
    managers::network::{message::ResponseBody, messages::GetResponseData},
    operations::GetOperationResult,
    types::{Assertion, ParsedUal, Visibility},
};

impl SendGetRequestsCommandHandler {
    /// Validate a get response and return the result if valid.
    ///
    /// Returns a result if the response is valid.
    pub(crate) async fn validate_response(
        &self,
        operation_id: Uuid,
        peer: &PeerId,
        response: &GetResponseData,
        parsed_ual: &ParsedUal,
        visibility: Visibility,
    ) -> Option<GetOperationResult> {
        match response {
            ResponseBody::Ack(ack) => {
                let assertion = &ack.assertion;
                let metadata = &ack.metadata;
                // Validate the assertion
                let is_valid = self
                    .assertion_validation_service
                    .validate_response(assertion, parsed_ual, visibility)
                    .await;

                if !is_valid {
                    tracing::debug!(
                        operation_id = %operation_id,
                        peer = %peer,
                        "Response validation failed"
                    );
                    return None;
                }

                // Build the result
                let get_result = GetOperationResult::new(
                    Assertion::new(assertion.public.clone(), assertion.private.clone()),
                    metadata.clone(),
                );

                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %peer,
                    public_count = assertion.public.len(),
                    has_private = assertion.private.is_some(),
                    "Response validated"
                );
                Some(get_result)
            }
            ResponseBody::Error(err) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %peer,
                    error = %err.error_message,
                    "Peer returned error response"
                );
                None
            }
        }
    }
}
