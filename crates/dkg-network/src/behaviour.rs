//! Node behaviour and swarm building.

use std::collections::HashSet;

use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder, identify, identity,
    kad::{self, BucketInserts, Config as KademliaConfig, Mode, store::MemoryStore},
    multiaddr::Protocol,
    noise,
    request_response::{self, ProtocolSupport},
    swarm::NetworkBehaviour,
    tcp,
};
use tracing::info;

use super::{
    NetworkError, NetworkManagerConfig,
    message::{RequestMessage, ResponseMessage},
    protocols::{
        BatchGetAck, BatchGetProtocol, BatchGetRequestData, FinalityAck, FinalityProtocol,
        FinalityRequestData, GetAck, GetProtocol, GetRequestData, JsCompatCodec, ProtocolSpec,
        StoreAck, StoreProtocol, StoreRequestData,
    },
};

/// Flattened network behaviour combining all protocols used by the node.
///
/// This includes both infrastructure protocols (kad, identify) and
/// application-specific request-response protocols (store, get, finality, batch_get).
#[derive(NetworkBehaviour)]
pub struct NodeBehaviour {
    pub kad: kad::Behaviour<MemoryStore>,
    pub identify: identify::Behaviour,
    // Application protocols using JsCompatCodec for JS node interoperability
    pub store: request_response::Behaviour<
        JsCompatCodec<RequestMessage<StoreRequestData>, ResponseMessage<StoreAck>>,
    >,
    pub get: request_response::Behaviour<
        JsCompatCodec<RequestMessage<GetRequestData>, ResponseMessage<GetAck>>,
    >,
    pub finality: request_response::Behaviour<
        JsCompatCodec<RequestMessage<FinalityRequestData>, ResponseMessage<FinalityAck>>,
    >,
    pub batch_get: request_response::Behaviour<
        JsCompatCodec<RequestMessage<BatchGetRequestData>, ResponseMessage<BatchGetAck>>,
    >,
}

/// Build a configured swarm with all protocols.
///
/// # Arguments
/// * `config` - Network configuration (port, bootstrap nodes, timeouts)
/// * `key` - Pre-loaded libp2p identity keypair
///
/// # Returns
/// A tuple of (Swarm, PeerId) on success.
///
/// # Errors
/// Returns `NetworkError` if:
/// - Bootstrap node parsing fails
/// - Transport creation fails
/// - Swarm building fails
pub fn build_swarm(
    config: &NetworkManagerConfig,
    key: identity::Keypair,
) -> Result<(Swarm<NodeBehaviour>, PeerId), NetworkError> {
    let public_key = key.public();
    let local_peer_id = PeerId::from(&public_key);

    info!("Network ID is {}", local_peer_id.to_base58());

    // Create base protocols
    // 1. Kademlia DHT
    let mut kad_config = KademliaConfig::default();
    kad_config.set_kbucket_inserts(BucketInserts::OnConnected);
    let mut seen_bootstrap = HashSet::new();
    for bootstrap in &config.bootstrap {
        // Parse as a full multiaddr first to extract and validate bootstrap peer ids.
        let full_addr: Multiaddr =
            bootstrap
                .parse()
                .map_err(|e| NetworkError::InvalidMultiaddr {
                    parsed: bootstrap.clone(),
                    source: e,
                })?;

        // Extract the peer ID from the /p2p/ component.
        let peer_id = full_addr
            .iter()
            .find_map(|proto| {
                if let Protocol::P2p(peer_id) = proto {
                    Some(peer_id)
                } else {
                    None
                }
            })
            .ok_or_else(|| NetworkError::InvalidBootstrapNode {
                expected: "multiaddr with /p2p/<peer_id> component".to_string(),
                received: bootstrap.clone(),
            })?;

        seen_bootstrap.insert(peer_id);
    }
    seen_bootstrap.remove(&local_peer_id);
    kad_config.set_query_peer_allowlist(seen_bootstrap.iter().copied());

    let memory_store = MemoryStore::new(local_peer_id);
    let mut kad = kad::Behaviour::with_config(local_peer_id, memory_store, kad_config);

    kad.set_mode(Some(Mode::Server));

    // Add bootstrap nodes to kad
    for bootstrap in &config.bootstrap {
        // Parse as a full multiaddr first
        let full_addr: Multiaddr =
            bootstrap
                .parse()
                .map_err(|e| NetworkError::InvalidMultiaddr {
                    parsed: bootstrap.clone(),
                    source: e,
                })?;

        // Extract the peer ID from the /p2p/ component
        let peer_id = full_addr
            .iter()
            .find_map(|proto| {
                if let Protocol::P2p(peer_id) = proto {
                    Some(peer_id)
                } else {
                    None
                }
            })
            .ok_or_else(|| NetworkError::InvalidBootstrapNode {
                expected: "multiaddr with /p2p/<peer_id> component".to_string(),
                received: bootstrap.clone(),
            })?;

        if !seen_bootstrap.insert(peer_id) {
            continue;
        }

        // Build the address without the /p2p/ component for kad
        let addr_without_peer: Multiaddr = full_addr
            .iter()
            .filter(|proto| !matches!(proto, Protocol::P2p(_)))
            .collect();

        kad.add_address(&peer_id, addr_without_peer);
    }

    // 2. Identify protocol
    let identify = identify::Behaviour::new(identify::Config::new(
        "/ipfs/id/1.0.0".to_string(),
        public_key.clone(),
    ));

    // 3. Application protocols (store, get, finality, batch_get)
    // Timeouts are defined in the ProtocolSpec implementations
    let store = request_response::Behaviour::with_codec(
        JsCompatCodec::new(),
        [(
            StreamProtocol::new(StoreProtocol::STREAM_PROTOCOL),
            ProtocolSupport::Full,
        )],
        request_response::Config::default().with_request_timeout(StoreProtocol::TIMEOUT),
    );
    let get = request_response::Behaviour::with_codec(
        JsCompatCodec::new(),
        [(
            StreamProtocol::new(GetProtocol::STREAM_PROTOCOL),
            ProtocolSupport::Full,
        )],
        request_response::Config::default().with_request_timeout(GetProtocol::TIMEOUT),
    );
    let finality = request_response::Behaviour::with_codec(
        JsCompatCodec::new(),
        [(
            StreamProtocol::new(FinalityProtocol::STREAM_PROTOCOL),
            ProtocolSupport::Full,
        )],
        request_response::Config::default().with_request_timeout(FinalityProtocol::TIMEOUT),
    );
    let batch_get = request_response::Behaviour::with_codec(
        JsCompatCodec::new(),
        [(
            StreamProtocol::new(BatchGetProtocol::STREAM_PROTOCOL),
            ProtocolSupport::Full,
        )],
        request_response::Config::default().with_request_timeout(BatchGetProtocol::TIMEOUT),
    );

    let behaviour = NodeBehaviour {
        kad,
        identify,
        store,
        get,
        finality,
        batch_get,
    };

    // Build the swarm with configurable idle connection timeout.
    // Default libp2p timeout is 10 seconds, which causes connections to drop quickly.
    // We use a configurable timeout (default 5 minutes) to maintain shard table connections.
    let idle_timeout = config.idle_connection_timeout();
    // Use yamux as the preferred multiplexer (better flow control, no head-of-line blocking)
    // with mplex as fallback for compatibility with older JS libp2p nodes.
    // Protocol negotiation selects the best common option automatically.
    let mut swarm = SwarmBuilder::with_existing_identity(key.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            (
                libp2p::yamux::Config::default,
                libp2p_mplex::Config::default,
            ),
        )
        .map_err(NetworkError::TransportCreation)?
        .with_behaviour(|_| behaviour)?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(idle_timeout))
        .build();

    // Add external address for NAT traversal if configured.
    // This allows peers behind NAT to be reachable by advertising their public IP.
    if let Some(ref external_ip) = config.external_ip {
        // Validate that it's a valid public IPv4 address
        match external_ip.parse::<std::net::Ipv4Addr>() {
            Ok(ip) if !ip.is_private() && !ip.is_loopback() && !ip.is_link_local() => {
                let external_addr: Multiaddr = format!("/ip4/{}/tcp/{}", external_ip, config.port)
                    .parse()
                    .map_err(|e| NetworkError::InvalidMultiaddr {
                        parsed: format!("/ip4/{}/tcp/{}", external_ip, config.port),
                        source: e,
                    })?;
                swarm.add_external_address(external_addr.clone());
                info!(
                    "Added external address for NAT traversal: {}",
                    external_addr
                );
            }
            Ok(_) => {
                tracing::warn!(
                    external_ip,
                    "external_ip must be a public IPv4 address, ignoring"
                );
            }
            Err(e) => {
                tracing::warn!(
                    external_ip,
                    error = %e,
                    "invalid external_ip format, must be a valid IPv4 address"
                );
            }
        }
    }

    Ok((swarm, local_peer_id))
}
