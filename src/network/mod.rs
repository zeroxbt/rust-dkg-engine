pub mod behaviour;
pub mod constants;
pub mod protocol;
pub mod request_tracker;
pub mod session_manager;

pub use behaviour::{NetworkProtocols, NetworkProtocolsEvent};
pub use constants::ProtocolTimeouts;
pub use protocol::{ProtocolRequest, ProtocolResponse};
pub use request_tracker::RequestTracker;
pub use session_manager::SessionManager;

// Type alias for convenience - the complete behaviour type
pub type Behaviour = network::NestedBehaviour<NetworkProtocols>;
