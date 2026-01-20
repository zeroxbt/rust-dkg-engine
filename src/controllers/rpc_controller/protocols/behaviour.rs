use network::{
    NetworkBehaviour, ProtocolSupport, RequestMessage, ResponseMessage, StreamProtocol,
    request_response,
};

use super::constants::ProtocolTimeouts;
use crate::controllers::rpc_controller::messages::{
    FinalityRequestData, FinalityResponseData, GetRequestData, GetResponseData, StoreRequestData,
    StoreResponseData,
};

/// Application-specific protocols
///
/// Only contains app-specific protocols (store, get, finality).
/// Base protocols (kad, identify, ping) are provided by NetworkManager via NetworkBehaviour.
#[derive(NetworkBehaviour)]
pub struct NetworkProtocols {
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
}

impl NetworkProtocols {
    /// Creates a new instance of application protocols with configured timeouts.
    ///
    /// Timeouts match the JS implementation:
    /// - Store: 15 seconds
    /// - Get: 15 seconds
    /// - Finality: 60 seconds
    pub fn new() -> Self {
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

        Self {
            store,
            get,
            finality,
        }
    }
}

impl Default for NetworkProtocols {
    fn default() -> Self {
        Self::new()
    }
}
