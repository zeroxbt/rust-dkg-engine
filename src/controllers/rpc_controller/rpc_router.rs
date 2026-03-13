use std::sync::Arc;

use dkg_network::{
    BatchGetAck, BatchGetRequestData, FinalityAck, FinalityRequestData, GetAck, GetRequestData,
    ImmediateResponse, InboundDecision, InboundRequest, NetworkEventHandler,
    PROTOCOL_NAME_BATCH_GET, PROTOCOL_NAME_FINALITY, PROTOCOL_NAME_GET, PROTOCOL_NAME_STORE,
    ResponseHandle, StoreAck, StoreRequestData,
};
use dkg_observability as observability;

use super::{PeerRateLimiter, RpcConfig, deps::RpcRouterDeps};
use crate::controllers::rpc_controller::v1::{
    batch_get::BatchGetRpcController, get::GetRpcController,
    publish_finality::PublishFinalityRpcController, publish_store::PublishStoreRpcController,
};

pub(crate) struct RpcRouter {
    store_controller: Arc<PublishStoreRpcController>,
    get_controller: Arc<GetRpcController>,
    finality_controller: Arc<PublishFinalityRpcController>,
    batch_get_controller: Arc<BatchGetRpcController>,
    peer_rate_limiter: Arc<PeerRateLimiter>,
}

impl RpcRouter {
    pub(crate) fn new(deps: RpcRouterDeps, config: &RpcConfig) -> Self {
        RpcRouter {
            store_controller: Arc::new(PublishStoreRpcController::new(deps.publish_store)),
            get_controller: Arc::new(GetRpcController::new(deps.get)),
            finality_controller: Arc::new(PublishFinalityRpcController::new(deps.publish_finality)),
            batch_get_controller: Arc::new(BatchGetRpcController::new(deps.batch_get)),
            peer_rate_limiter: Arc::new(PeerRateLimiter::new(config.rate_limiter.clone())),
        }
    }

    fn store_busy_response(
        response_handle: ResponseHandle<StoreAck>,
        operation_id: uuid::Uuid,
    ) -> ImmediateResponse<StoreAck> {
        ImmediateResponse::busy(response_handle, operation_id, "Rate limited")
    }

    fn get_busy_response(
        response_handle: ResponseHandle<GetAck>,
        operation_id: uuid::Uuid,
    ) -> ImmediateResponse<GetAck> {
        ImmediateResponse::busy(response_handle, operation_id, "Rate limited")
    }

    fn finality_busy_response(
        response_handle: ResponseHandle<FinalityAck>,
        operation_id: uuid::Uuid,
    ) -> ImmediateResponse<FinalityAck> {
        ImmediateResponse::busy(response_handle, operation_id, "Rate limited")
    }

    fn batch_get_busy_response(
        response_handle: ResponseHandle<BatchGetAck>,
        operation_id: uuid::Uuid,
    ) -> ImmediateResponse<BatchGetAck> {
        ImmediateResponse::busy(response_handle, operation_id, "Rate limited")
    }
}

impl NetworkEventHandler for RpcRouter {
    // ─────────────────────────────────────────────────────────────────────────
    // Protocol inbound requests
    // ─────────────────────────────────────────────────────────────────────────

    fn on_store_request(
        &self,
        request: InboundRequest<StoreRequestData>,
        response_handle: ResponseHandle<StoreAck>,
    ) -> InboundDecision<StoreAck> {
        let operation_id = request.operation_id();
        if !self.peer_rate_limiter.check(request.peer_id()) {
            observability::record_network_inbound_request(PROTOCOL_NAME_STORE, "rate_limited");
            return InboundDecision::RespondNow(Self::store_busy_response(
                response_handle,
                operation_id,
            ));
        }
        if let Some(response_handle) = self
            .store_controller
            .handle_request(request, response_handle)
        {
            observability::record_network_inbound_request(PROTOCOL_NAME_STORE, "controller_busy");
            return InboundDecision::RespondNow(Self::store_busy_response(
                response_handle,
                operation_id,
            ));
        }
        observability::record_network_inbound_request(PROTOCOL_NAME_STORE, "scheduled");
        InboundDecision::Scheduled
    }

    fn on_get_request(
        &self,
        request: InboundRequest<GetRequestData>,
        response_handle: ResponseHandle<GetAck>,
    ) -> InboundDecision<GetAck> {
        let operation_id = request.operation_id();
        if !self.peer_rate_limiter.check(request.peer_id()) {
            observability::record_network_inbound_request(PROTOCOL_NAME_GET, "rate_limited");
            return InboundDecision::RespondNow(Self::get_busy_response(
                response_handle,
                operation_id,
            ));
        }
        if let Some(response_handle) = self.get_controller.handle_request(request, response_handle)
        {
            observability::record_network_inbound_request(PROTOCOL_NAME_GET, "controller_busy");
            return InboundDecision::RespondNow(Self::get_busy_response(
                response_handle,
                operation_id,
            ));
        }
        observability::record_network_inbound_request(PROTOCOL_NAME_GET, "scheduled");
        InboundDecision::Scheduled
    }

    fn on_finality_request(
        &self,
        request: InboundRequest<FinalityRequestData>,
        response_handle: ResponseHandle<FinalityAck>,
    ) -> InboundDecision<FinalityAck> {
        let operation_id = request.operation_id();
        if !self.peer_rate_limiter.check(request.peer_id()) {
            observability::record_network_inbound_request(PROTOCOL_NAME_FINALITY, "rate_limited");
            return InboundDecision::RespondNow(Self::finality_busy_response(
                response_handle,
                operation_id,
            ));
        }
        if let Some(response_handle) = self
            .finality_controller
            .handle_request(request, response_handle)
        {
            observability::record_network_inbound_request(
                PROTOCOL_NAME_FINALITY,
                "controller_busy",
            );
            return InboundDecision::RespondNow(Self::finality_busy_response(
                response_handle,
                operation_id,
            ));
        }
        observability::record_network_inbound_request(PROTOCOL_NAME_FINALITY, "scheduled");
        InboundDecision::Scheduled
    }

    fn on_batch_get_request(
        &self,
        request: InboundRequest<BatchGetRequestData>,
        response_handle: ResponseHandle<BatchGetAck>,
    ) -> InboundDecision<BatchGetAck> {
        let operation_id = request.operation_id();
        if !self.peer_rate_limiter.check(request.peer_id()) {
            observability::record_network_inbound_request(PROTOCOL_NAME_BATCH_GET, "rate_limited");
            return InboundDecision::RespondNow(Self::batch_get_busy_response(
                response_handle,
                operation_id,
            ));
        }
        if let Some(response_handle) = self
            .batch_get_controller
            .handle_request(request, response_handle)
        {
            observability::record_network_inbound_request(
                PROTOCOL_NAME_BATCH_GET,
                "controller_busy",
            );
            return InboundDecision::RespondNow(Self::batch_get_busy_response(
                response_handle,
                operation_id,
            ));
        }
        observability::record_network_inbound_request(PROTOCOL_NAME_BATCH_GET, "scheduled");
        InboundDecision::Scheduled
    }
}
