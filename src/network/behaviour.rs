use network::{
    NetworkBehaviour, ProtocolSupport, RequestMessage, ResponseMessage, StreamProtocol,
    request_response,
};

use crate::types::protocol::{
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
    /// Creates a new instance of application protocols
    pub fn new() -> Self {
        let store = request_response::json::Behaviour::<
            RequestMessage<StoreRequestData>,
            ResponseMessage<StoreResponseData>,
        >::new(
            [(StreamProtocol::new("/store/1.0.0"), ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        let get = request_response::json::Behaviour::<
            RequestMessage<GetRequestData>,
            ResponseMessage<GetResponseData>,
        >::new(
            [(StreamProtocol::new("/get/1.0.0"), ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        let finality = request_response::json::Behaviour::<
            RequestMessage<FinalityRequestData>,
            ResponseMessage<FinalityResponseData>,
        >::new(
            [(
                StreamProtocol::new("/finality/1.0.0"),
                ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        );

        Self {
            store,
            get,
            finality,
        }
    }
}

// No type alias needed - use NetworkManager<NetworkProtocols> directly
// The NetworkBehaviour wrapping is done inside NetworkManager
