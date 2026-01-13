use std::convert::Infallible;

use thiserror::Error;

/// Error types for network operations
#[derive(Error, Debug)]
pub enum NetworkError {
    /// Error reading/writing peer ID from/to disk
    #[error("Peer ID I/O error: {0}")]
    PeerIdIo(#[from] std::io::Error),

    /// Error parsing peer ID
    #[error("Invalid peer ID parsed. Parsed: {parsed}")]
    InvalidPeerId {
        parsed: String,
        #[source]
        #[allow(unused)]
        source: libp2p::identity::ParseError,
    },

    /// Error parsing multiaddr
    #[error("Invalid multiaddress parsed. Parsed: {parsed}")]
    InvalidMultiaddr {
        parsed: String,
        #[source]
        #[allow(unused)]
        source: libp2p::multiaddr::Error,
    },

    /// Error creating libp2p transport
    #[error("Transport creation failed: {0}")]
    TransportCreation(#[from] libp2p::noise::Error),

    /// Error starting listener
    #[error("Failed to start listener on {address}.")]
    ListenerFailed {
        address: String,
        #[source]
        #[allow(unused)]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error sending message to peer
    #[error("Failed to send message to {peer_id}: {reason}")]
    SendFailed { peer_id: String, reason: String },

    /// Error sending response
    #[error("Failed to send {protocol} response; channel closed or peer disconnected.")]
    ResponseFailed { protocol: &'static str },

    /// Dial failure
    #[error("Failed to dial peer {peer_id}: {reason}")]
    DialFailed { peer_id: String, reason: String },

    /// Swarm already taken for event loop
    #[error("Swarm already taken for event loop.")]
    SwarmAlreadyTaken,

    /// Key management error
    #[error("Key conversion failed: {0}")]
    KeyConversion(#[from] libp2p::identity::OtherVariantError),

    /// Bootstrap node parsing error
    #[error("Invalid bootstrap node format. Expected: {expected}, got: {received}")]
    InvalidBootstrapNode { expected: String, received: String },
}

impl From<Infallible> for NetworkError {
    fn from(e: Infallible) -> Self {
        match e {}
    }
}

/// Convenient Result type alias for NetworkError
pub type Result<T> = std::result::Result<T, NetworkError>;
