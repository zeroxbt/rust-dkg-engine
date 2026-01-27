pub(crate) use libp2p::swarm::derive_prelude::*;

use super::{
    super::messages::{
        BatchGetRequestData, BatchGetResponseData, FinalityRequestData, FinalityResponseData,
        GetRequestData, GetResponseData, StoreRequestData, StoreResponseData,
    },
    JsCompatCodec,
    constants::ProtocolTimeouts,
};
use crate::managers::network::{
    ProtocolSupport, RequestMessage, ResponseMessage, StreamProtocol, request_response,
};

/// Application-specific protocols
///
/// Only contains app-specific protocols (store, get, finality, batch_get).
/// Base protocols (kad, identify, ping) are provided by NetworkManager via NetworkBehaviour.
/// Uses JsCompatCodec to handle the JS node's chunked message format.
#[derive(libp2p::swarm::NetworkBehaviour)]
pub(crate) struct NetworkProtocols {
    pub store: request_response::Behaviour<
        JsCompatCodec<RequestMessage<StoreRequestData>, ResponseMessage<StoreResponseData>>,
    >,
    pub get: request_response::Behaviour<
        JsCompatCodec<RequestMessage<GetRequestData>, ResponseMessage<GetResponseData>>,
    >,
    pub finality: request_response::Behaviour<
        JsCompatCodec<RequestMessage<FinalityRequestData>, ResponseMessage<FinalityResponseData>>,
    >,
    pub batch_get: request_response::Behaviour<
        JsCompatCodec<RequestMessage<BatchGetRequestData>, ResponseMessage<BatchGetResponseData>>,
    >,
}

impl NetworkProtocols {
    /// Creates a new instance of application protocols with configured timeouts.
    pub(crate) fn new() -> Self {
        let store_config =
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::STORE);

        let store = request_response::Behaviour::with_codec(
            JsCompatCodec::new(),
            [(StreamProtocol::new("/store/1.0.0"), ProtocolSupport::Full)],
            store_config,
        );

        let get_config =
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::GET);

        let get = request_response::Behaviour::with_codec(
            JsCompatCodec::new(),
            [(StreamProtocol::new("/get/1.0.0"), ProtocolSupport::Full)],
            get_config,
        );

        let finality_config =
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::FINALITY);

        let finality = request_response::Behaviour::with_codec(
            JsCompatCodec::new(),
            [(
                StreamProtocol::new("/finality/1.0.0"),
                // TODO: verify with js nodes if this can be outbound only
                ProtocolSupport::Full,
            )],
            finality_config,
        );

        let batch_get_config =
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::BATCH_GET);

        let batch_get = request_response::Behaviour::with_codec(
            JsCompatCodec::new(),
            [(
                StreamProtocol::new("/batch-get/1.0.0"),
                ProtocolSupport::Full,
            )],
            batch_get_config,
        );

        Self {
            store,
            get,
            finality,
            batch_get,
        }
    }
}

impl Default for NetworkProtocols {
    fn default() -> Self {
        Self::new()
    }
}
