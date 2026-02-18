use dkg_network::PeerId;
use dkg_repository::FinalityStatusRepository;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct ServePublishFinalityInput {
    pub operation_id: Uuid,
    pub ual: String,
    pub publish_store_operation_id: String,
    pub remote_peer_id: PeerId,
}

#[derive(Debug, Clone)]
pub(crate) enum ServePublishFinalityOutcome {
    Ack,
    Nack,
}

pub(crate) struct ServePublishFinalityWorkflow {
    finality_status_repository: FinalityStatusRepository,
}

impl ServePublishFinalityWorkflow {
    pub(crate) fn new(finality_status_repository: FinalityStatusRepository) -> Self {
        Self {
            finality_status_repository,
        }
    }

    pub(crate) async fn execute(
        &self,
        input: &ServePublishFinalityInput,
    ) -> ServePublishFinalityOutcome {
        let publish_store_operation_id = match Uuid::parse_str(&input.publish_store_operation_id) {
            Ok(uuid) => uuid,
            Err(e) => {
                tracing::error!(
                    operation_id = %input.operation_id,
                    publish_store_operation_id = %input.publish_store_operation_id,
                    error = %e,
                    "Failed to parse publish_operation_id as UUID"
                );
                return ServePublishFinalityOutcome::Nack;
            }
        };

        if let Err(e) = self
            .finality_status_repository
            .save_finality_ack(
                publish_store_operation_id,
                &input.ual,
                &input.remote_peer_id.to_base58(),
            )
            .await
        {
            tracing::error!(
                operation_id = %input.operation_id,
                publish_operation_id = %publish_store_operation_id,
                ual = %input.ual,
                peer = %input.remote_peer_id,
                error = %e,
                "Failed to save finality ack"
            );
            return ServePublishFinalityOutcome::Nack;
        }

        tracing::debug!(
            operation_id = %input.operation_id,
            publish_operation_id = %publish_store_operation_id,
            ual = %input.ual,
            peer = %input.remote_peer_id,
            "Finality ack saved successfully"
        );

        ServePublishFinalityOutcome::Ack
    }
}
