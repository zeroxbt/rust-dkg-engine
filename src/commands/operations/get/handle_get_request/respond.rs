use dkg_domain::Assertion;
use dkg_network::{ResponseHandle, messages::GetAck};
use uuid::Uuid;

use crate::commands::operations::get::handle_get_request::HandleGetRequestCommandHandler;

impl HandleGetRequestCommandHandler {
    pub(crate) async fn send_nack(
        &self,
        channel: ResponseHandle<GetAck>,
        operation_id: Uuid,
        error_message: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_get_nack(channel, operation_id, error_message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send get NACK response"
            );
        }
    }

    pub(crate) async fn send_ack(
        &self,
        channel: ResponseHandle<GetAck>,
        operation_id: Uuid,
        assertion: Assertion,
        metadata: Option<Vec<String>>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_get_ack(
                channel,
                operation_id,
                GetAck {
                    assertion,
                    metadata,
                },
            )
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send get ACK response"
            );
        }
    }
}
