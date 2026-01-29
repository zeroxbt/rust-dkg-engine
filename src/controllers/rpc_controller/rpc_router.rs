use std::sync::Arc;

use uuid::Uuid;

use crate::{
    context::Context,
    controllers::rpc_controller::v1::{
        batch_get_rpc_controller::BatchGetRpcController,
        finality_rpc_controller::FinalityRpcController, get_rpc_controller::GetRpcController,
        store_rpc_controller::StoreRpcController,
    },
    managers::network::{
        Multiaddr, NetworkEventHandler, NetworkManager, PeerId,
        message::{RequestMessage, ResponseMessage, ResponseMessageHeader, ResponseMessageType},
        messages::{
            BatchGetRequestData, BatchGetResponseData, FinalityRequestData, FinalityResponseData,
            GetRequestData, GetResponseData, StoreRequestData, StoreResponseData,
        },
        request_response::ResponseChannel,
    },
    services::{PeerDiscoveryTracker, PeerRateLimiter},
};

pub(crate) struct RpcRouter {
    network_manager: Arc<NetworkManager>,
    store_controller: Arc<StoreRpcController>,
    get_controller: Arc<GetRpcController>,
    finality_controller: Arc<FinalityRpcController>,
    batch_get_controller: Arc<BatchGetRpcController>,
    peer_discovery_tracker: Arc<PeerDiscoveryTracker>,
    peer_rate_limiter: Arc<PeerRateLimiter>,
}

impl RpcRouter {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        RpcRouter {
            network_manager: Arc::clone(context.network_manager()),
            store_controller: Arc::new(StoreRpcController::new(Arc::clone(&context))),
            get_controller: Arc::new(GetRpcController::new(Arc::clone(&context))),
            finality_controller: Arc::new(FinalityRpcController::new(Arc::clone(&context))),
            batch_get_controller: Arc::new(BatchGetRpcController::new(Arc::clone(&context))),
            peer_discovery_tracker: Arc::clone(context.peer_discovery_tracker()),
            peer_rate_limiter: Arc::clone(context.peer_rate_limiter()),
        }
    }

    async fn send_store_busy(
        &self,
        channel: ResponseChannel<ResponseMessage<StoreResponseData>>,
        operation_id: Uuid,
    ) {
        let response = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: StoreResponseData::nack("Rate limited"),
        };
        let _ = self
            .network_manager
            .send_store_response(channel, response)
            .await;
    }

    async fn send_get_busy(
        &self,
        channel: ResponseChannel<ResponseMessage<GetResponseData>>,
        operation_id: Uuid,
    ) {
        let response = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: GetResponseData::nack("Rate limited"),
        };
        let _ = self
            .network_manager
            .send_get_response(channel, response)
            .await;
    }

    async fn send_finality_busy(
        &self,
        channel: ResponseChannel<ResponseMessage<FinalityResponseData>>,
        operation_id: Uuid,
    ) {
        let response = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: FinalityResponseData::nack("Rate limited"),
        };
        let _ = self
            .network_manager
            .send_finality_response(channel, response)
            .await;
    }

    async fn send_batch_get_busy(
        &self,
        channel: ResponseChannel<ResponseMessage<BatchGetResponseData>>,
        operation_id: Uuid,
    ) {
        let response = ResponseMessage {
            header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
            data: BatchGetResponseData::nack("Rate limited"),
        };
        let _ = self
            .network_manager
            .send_batch_get_response(channel, response)
            .await;
    }
}

impl NetworkEventHandler for RpcRouter {
    // ─────────────────────────────────────────────────────────────────────────
    // Protocol inbound requests
    // ─────────────────────────────────────────────────────────────────────────

    async fn on_store_request(
        &self,
        request: RequestMessage<StoreRequestData>,
        channel: ResponseChannel<ResponseMessage<StoreResponseData>>,
        peer: PeerId,
    ) {
        if !self.peer_rate_limiter.check(&peer) {
            self.send_store_busy(channel, request.header.operation_id())
                .await;
            return;
        }
        self.store_controller
            .handle_request(request, channel, peer)
            .await;
    }

    async fn on_get_request(
        &self,
        request: RequestMessage<GetRequestData>,
        channel: ResponseChannel<ResponseMessage<GetResponseData>>,
        peer: PeerId,
    ) {
        if !self.peer_rate_limiter.check(&peer) {
            self.send_get_busy(channel, request.header.operation_id())
                .await;
            return;
        }
        self.get_controller
            .handle_request(request, channel, peer)
            .await;
    }

    async fn on_finality_request(
        &self,
        request: RequestMessage<FinalityRequestData>,
        channel: ResponseChannel<ResponseMessage<FinalityResponseData>>,
        peer: PeerId,
    ) {
        if !self.peer_rate_limiter.check(&peer) {
            self.send_finality_busy(channel, request.header.operation_id())
                .await;
            return;
        }
        self.finality_controller
            .handle_request(request, channel, peer)
            .await;
    }

    async fn on_batch_get_request(
        &self,
        request: RequestMessage<BatchGetRequestData>,
        channel: ResponseChannel<ResponseMessage<BatchGetResponseData>>,
        peer: PeerId,
    ) {
        if !self.peer_rate_limiter.check(&peer) {
            self.send_batch_get_busy(channel, request.header.operation_id())
                .await;
            return;
        }
        self.batch_get_controller
            .handle_request(request, channel, peer)
            .await;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Infrastructure events
    // ─────────────────────────────────────────────────────────────────────────

    async fn on_identify_received(&self, peer_id: PeerId, listen_addrs: Vec<Multiaddr>) {
        if let Err(e) = self
            .network_manager
            .add_kad_addresses(peer_id, listen_addrs)
            .await
        {
            tracing::error!(%peer_id, %e, "failed to add kad addresses");
        }
    }

    async fn on_kad_peer_found(&self, target: PeerId) {
        tracing::debug!(%target, "DHT found peer, dialing");
        if let Err(e) = self.network_manager.dial_peer(target).await {
            tracing::debug!(%target, %e, "failed to dial peer");
        }
    }

    async fn on_kad_peer_not_found(&self, target: PeerId) {
        tracing::debug!(%target, "DHT lookup did not find target peer");
        self.peer_discovery_tracker.record_failure(target);
    }

    async fn on_connection_established(&self, peer_id: PeerId) {
        self.peer_discovery_tracker.record_success(&peer_id);
    }

    async fn on_connection_closed(&self, _peer_id: PeerId) {
        // Could track disconnections if needed
    }

    fn on_new_listen_addr(&self, address: Multiaddr) {
        tracing::info!("Listening on {}", address);
    }
}
