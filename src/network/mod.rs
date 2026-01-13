pub mod actions;
pub mod behaviour;

pub use actions::NetworkHandle;
pub use behaviour::NetworkProtocols;

// Type alias for convenience - the complete behaviour type
pub type Behaviour = network::NestedBehaviour<NetworkProtocols>;
