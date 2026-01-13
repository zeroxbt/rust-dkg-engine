pub mod behaviour;
pub mod protocol;

pub use behaviour::{NetworkProtocols, NetworkProtocolsEvent};
pub use protocol::{ProtocolRequest, ProtocolResponse};

// Type alias for convenience - the complete behaviour type
pub type Behaviour = network::NestedBehaviour<NetworkProtocols>;
