use dkg_network::{ResponseHandle, messages::FinalityAck};
use uuid::Uuid;

use super::handler::HandlePublishFinalityRequestCommandHandler;

impl HandlePublishFinalityRequestCommandHandler {
    pub(crate) async fn send_ack(
        &self,
        channel: ResponseHandle<FinalityAck>,
        operation_id: Uuid,
        ual: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_finality_ack(
                channel,
                operation_id,
                FinalityAck {
                    message: format!("Acknowledged storing of {}", ual),
                },
            )
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send finality ACK response"
            );
        }
    }

    pub(crate) async fn send_nack(
        &self,
        channel: ResponseHandle<FinalityAck>,
        operation_id: Uuid,
        ual: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_finality_nack(
                channel,
                operation_id,
                format!("Failed to acknowledge storing of {}", ual),
            )
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send finality NACK response"
            );
        }
    }
}
