use super::constants::ProtocolTimeouts;
use crate::{
    controllers::rpc_controller::messages::{
        BatchGetRequestData, BatchGetResponseData, FinalityRequestData, FinalityResponseData,
        GetRequestData, GetResponseData, StoreRequestData, StoreResponseData,
    },
    managers::network::{
        NetworkBehaviour, ProtocolSupport, RequestMessage, ResponseMessage, StreamProtocol,
        request_response,
    },
};

/// Application-specific protocols
///
/// Only contains app-specific protocols (store, get, finality, batch_get).
/// Base protocols (kad, identify, ping) are provided by NetworkManager via NetworkBehaviour.
#[derive(NetworkBehaviour)]
pub(crate) struct NetworkProtocols {
    pub store: request_response::json::Behaviour<
        RequestMessage<StoreRequestData>,
        ResponseMessage<StoreResponseData>,
    >,
    pub get: request_response::json::Behaviour<
        RequestMessage<GetRequestData>,
        ResponseMessage<GetResponseData>,
    >,
    pub finality: request_response::json::Behaviour<
        RequestMessage<FinalityRequestData>,
        ResponseMessage<FinalityResponseData>,
    >,
    pub batch_get: request_response::json::Behaviour<
        RequestMessage<BatchGetRequestData>,
        ResponseMessage<BatchGetResponseData>,
    >,
}

impl NetworkProtocols {
    /// Creates a new instance of application protocols with configured timeouts.
    ///
    /// Timeouts match the JS implementation:
    /// - Store: 15 seconds
    /// - Get: 15 seconds
    /// - Finality: 60 seconds
    pub(crate) fn new() -> Self {
        let store_config =
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::STORE);

        let store = request_response::json::Behaviour::<
            RequestMessage<StoreRequestData>,
            ResponseMessage<StoreResponseData>,
        >::new(
            [(StreamProtocol::new("/store/1.0.0"), ProtocolSupport::Full)],
            store_config,
        );

        let get_config =
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::GET);

        let get = request_response::json::Behaviour::<
            RequestMessage<GetRequestData>,
            ResponseMessage<GetResponseData>,
        >::new(
            [(StreamProtocol::new("/get/1.0.0"), ProtocolSupport::Full)],
            get_config,
        );

        let finality_config =
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::FINALITY);

        let finality = request_response::json::Behaviour::<
            RequestMessage<FinalityRequestData>,
            ResponseMessage<FinalityResponseData>,
        >::new(
            [(
                StreamProtocol::new("/finality/1.0.0"),
                // TODO: verify with js nodes if this can be outbound only
                ProtocolSupport::Full,
            )],
            finality_config,
        );

        let batch_get_config =
            request_response::Config::default().with_request_timeout(ProtocolTimeouts::BATCH_GET);

        let batch_get = request_response::json::Behaviour::<
            RequestMessage<BatchGetRequestData>,
            ResponseMessage<BatchGetResponseData>,
        >::new(
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
