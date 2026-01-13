pub mod behaviour;
pub mod protocol;
pub mod session_manager;

pub use behaviour::{NetworkProtocols, NetworkProtocolsEvent};
pub use protocol::{ProtocolRequest, ProtocolResponse};
pub use session_manager::SessionManager;

// Type alias for convenience - the complete behaviour type
pub type Behaviour = network::NestedBehaviour<NetworkProtocols>;
