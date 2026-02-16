use std::convert::Infallible;

use libp2p::request_response::OutboundFailure;
use thiserror::Error;

/// Unified error type for all network operations.
///
/// This includes both infrastructure errors (swarm creation, listening) and
/// request-level errors (timeouts, connection failures).
#[derive(Error, Debug)]
pub enum NetworkError {
    // ===== Infrastructure errors =====
    /// Error reading/writing peer ID from/to disk
    #[error("Peer ID I/O error: {0}")]
    PeerIdIo(#[from] std::io::Error),

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

    /// Swarm action channel closed
    #[error("Swarm action channel closed")]
    ActionChannelClosed,

    /// Response channel closed (event loop shut down before returning response)
    #[error("Response channel closed before receiving response")]
    ResponseChannelClosed,

    /// Key management error
    #[error("Key conversion failed: {0}")]
    KeyConversion(#[from] libp2p::identity::OtherVariantError),

    /// Bootstrap node parsing error
    #[error("Invalid bootstrap node format. Expected: {expected}, got: {received}")]
    InvalidBootstrapNode { expected: String, received: String },

    // ===== Request-level errors =====
    /// Request timed out waiting for response
    #[error("Request timed out")]
    RequestTimeout,

    /// Failed to establish connection to peer
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Peer is not connected and dial failed
    #[error("Peer not connected")]
    PeerNotConnected,

    /// Protocol not supported by remote peer
    #[error("Protocol not supported by remote peer")]
    UnsupportedProtocol,
}

impl From<Infallible> for NetworkError {
    fn from(e: Infallible) -> Self {
        match e {}
    }
}

impl From<&OutboundFailure> for NetworkError {
    fn from(error: &OutboundFailure) -> Self {
        match error {
            OutboundFailure::Timeout => NetworkError::RequestTimeout,
            OutboundFailure::ConnectionClosed => {
                NetworkError::ConnectionFailed("Connection closed".to_string())
            }
            OutboundFailure::DialFailure => NetworkError::PeerNotConnected,
            OutboundFailure::UnsupportedProtocols => NetworkError::UnsupportedProtocol,
            _ => NetworkError::ConnectionFailed(error.to_string()),
        }
    }
}

/// Convenient Result type alias for NetworkError
pub type Result<T> = std::result::Result<T, NetworkError>;
