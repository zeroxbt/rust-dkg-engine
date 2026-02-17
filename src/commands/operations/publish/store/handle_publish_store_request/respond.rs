use dkg_network::{ResponseHandle, messages::StoreAck};
use uuid::Uuid;

use super::handler::HandlePublishStoreRequestCommandHandler;

impl HandlePublishStoreRequestCommandHandler {
    pub(crate) async fn send_nack(
        &self,
        channel: ResponseHandle<StoreAck>,
        operation_id: Uuid,
        error_message: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_store_nack(channel, operation_id, error_message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send store NACK response"
            );
        }
    }

    pub(crate) async fn send_ack(
        &self,
        channel: ResponseHandle<StoreAck>,
        operation_id: Uuid,
        identity_id: u128,
        signature: dkg_domain::SignatureComponents,
    ) {
        if let Err(e) = self
            .network_manager
            .send_store_ack(
                channel,
                operation_id,
                StoreAck {
                    identity_id,
                    signature,
                },
            )
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send store ACK response"
            );
        }
    }
}
