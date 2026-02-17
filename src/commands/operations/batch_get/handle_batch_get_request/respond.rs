use std::collections::HashMap;

use dkg_domain::Assertion;
use dkg_network::{BatchGetAck, ResponseHandle};
use uuid::Uuid;

use crate::commands::operations::batch_get::handle_batch_get_request::HandleBatchGetRequestCommandHandler;

impl HandleBatchGetRequestCommandHandler {
    pub(crate) async fn send_ack(
        &self,
        channel: ResponseHandle<BatchGetAck>,
        operation_id: Uuid,
        assertions: HashMap<String, Assertion>,
        metadata: HashMap<String, Vec<String>>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_batch_get_ack(
                channel,
                operation_id,
                BatchGetAck {
                    assertions,
                    metadata,
                },
            )
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send batch-get ACK response"
            );
        }
    }

    pub(crate) async fn send_nack(
        &self,
        channel: ResponseHandle<BatchGetAck>,
        operation_id: Uuid,
        message: impl Into<String>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_batch_get_nack(channel, operation_id, message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send batch-get NACK response"
            );
        }
    }
}
