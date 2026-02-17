use std::sync::Arc;

use dkg_network::{
    ImmediateResponse, InboundDecision, InboundRequest, NetworkEventHandler, ResponseHandle,
    messages::{
        BatchGetAck, BatchGetRequestData, FinalityAck, FinalityRequestData, GetAck, GetRequestData,
        StoreAck, StoreRequestData,
    },
};

use super::{PeerRateLimiter, RpcConfig};
use crate::{
    context::Context,
    controllers::rpc_controller::v1::{
        batch_get::BatchGetRpcController, get::GetRpcController,
        publish_finality::PublishFinalityRpcController, publish_store::PublishStoreRpcController,
    },
};

pub(crate) struct RpcRouter {
    store_controller: Arc<PublishStoreRpcController>,
    get_controller: Arc<GetRpcController>,
    finality_controller: Arc<PublishFinalityRpcController>,
    batch_get_controller: Arc<BatchGetRpcController>,
    peer_rate_limiter: Arc<PeerRateLimiter>,
}

impl RpcRouter {
    pub(crate) fn new(context: Arc<Context>, config: &RpcConfig) -> Self {
        RpcRouter {
            store_controller: Arc::new(PublishStoreRpcController::new(Arc::clone(&context))),
            get_controller: Arc::new(GetRpcController::new(Arc::clone(&context))),
            finality_controller: Arc::new(PublishFinalityRpcController::new(Arc::clone(&context))),
            batch_get_controller: Arc::new(BatchGetRpcController::new(Arc::clone(&context))),
            peer_rate_limiter: Arc::new(PeerRateLimiter::new(config.rate_limiter.clone())),
        }
    }

    fn store_busy_response(
        channel: ResponseHandle<StoreAck>,
        operation_id: uuid::Uuid,
    ) -> ImmediateResponse<StoreAck> {
        ImmediateResponse::busy(channel, operation_id, "Rate limited")
    }

    fn get_busy_response(
        channel: ResponseHandle<GetAck>,
        operation_id: uuid::Uuid,
    ) -> ImmediateResponse<GetAck> {
        ImmediateResponse::busy(channel, operation_id, "Rate limited")
    }

    fn finality_busy_response(
        channel: ResponseHandle<FinalityAck>,
        operation_id: uuid::Uuid,
    ) -> ImmediateResponse<FinalityAck> {
        ImmediateResponse::busy(channel, operation_id, "Rate limited")
    }

    fn batch_get_busy_response(
        channel: ResponseHandle<BatchGetAck>,
        operation_id: uuid::Uuid,
    ) -> ImmediateResponse<BatchGetAck> {
        ImmediateResponse::busy(channel, operation_id, "Rate limited")
    }
}

impl NetworkEventHandler for RpcRouter {
    // ─────────────────────────────────────────────────────────────────────────
    // Protocol inbound requests
    // ─────────────────────────────────────────────────────────────────────────

    fn on_store_request(
        &self,
        request: InboundRequest<StoreRequestData>,
        channel: ResponseHandle<StoreAck>,
    ) -> InboundDecision<StoreAck> {
        let operation_id = request.operation_id();
        if !self.peer_rate_limiter.check(&request.peer_id()) {
            return InboundDecision::RespondNow(Self::store_busy_response(channel, operation_id));
        }
        if let Some(channel) = self.store_controller.handle_request(request, channel) {
            return InboundDecision::RespondNow(Self::store_busy_response(channel, operation_id));
        }
        InboundDecision::Scheduled
    }

    fn on_get_request(
        &self,
        request: InboundRequest<GetRequestData>,
        channel: ResponseHandle<GetAck>,
    ) -> InboundDecision<GetAck> {
        let operation_id = request.operation_id();
        if !self.peer_rate_limiter.check(&request.peer_id()) {
            return InboundDecision::RespondNow(Self::get_busy_response(channel, operation_id));
        }
        if let Some(channel) = self.get_controller.handle_request(request, channel) {
            return InboundDecision::RespondNow(Self::get_busy_response(channel, operation_id));
        }
        InboundDecision::Scheduled
    }

    fn on_finality_request(
        &self,
        request: InboundRequest<FinalityRequestData>,
        channel: ResponseHandle<FinalityAck>,
    ) -> InboundDecision<FinalityAck> {
        let operation_id = request.operation_id();
        if !self.peer_rate_limiter.check(&request.peer_id()) {
            return InboundDecision::RespondNow(Self::finality_busy_response(
                channel,
                operation_id,
            ));
        }
        if let Some(channel) = self.finality_controller.handle_request(request, channel) {
            return InboundDecision::RespondNow(Self::finality_busy_response(
                channel,
                operation_id,
            ));
        }
        InboundDecision::Scheduled
    }

    fn on_batch_get_request(
        &self,
        request: InboundRequest<BatchGetRequestData>,
        channel: ResponseHandle<BatchGetAck>,
    ) -> InboundDecision<BatchGetAck> {
        let operation_id = request.operation_id();
        if !self.peer_rate_limiter.check(&request.peer_id()) {
            return InboundDecision::RespondNow(Self::batch_get_busy_response(
                channel,
                operation_id,
            ));
        }
        if let Some(channel) = self.batch_get_controller.handle_request(request, channel) {
            return InboundDecision::RespondNow(Self::batch_get_busy_response(
                channel,
                operation_id,
            ));
        }
        InboundDecision::Scheduled
    }
}
