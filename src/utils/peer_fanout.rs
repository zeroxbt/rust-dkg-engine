use std::{future::Future, ops::ControlFlow};

use futures::{StreamExt, stream::FuturesUnordered};

/// Limit a peer list to a maximum count, preserving order.
pub(crate) fn limit_peers<T>(mut peers: Vec<T>, max_peers: usize) -> Vec<T> {
    if max_peers != usize::MAX && peers.len() > max_peers {
        peers.truncate(max_peers);
    }
    peers
}

/// Execute peer requests with a fixed concurrency limit.
///
/// Results are processed as each request completes. Returning `Break`
/// stops scheduling new peers.
pub(crate) async fn for_each_peer_concurrently<P, F, Fut, T, C>(
    peers: &[P],
    concurrent: usize,
    mut make_future: F,
    mut on_result: C,
) where
    P: Clone,
    F: FnMut(P) -> Fut,
    Fut: Future<Output = T>,
    C: FnMut(T) -> ControlFlow<()>,
{
    if peers.is_empty() {
        return;
    }

    let mut futures = FuturesUnordered::new();
    let mut peers_iter = peers.iter().cloned();
    let limit = concurrent.max(1).min(peers.len());

    for _ in 0..limit {
        if let Some(peer) = peers_iter.next() {
            futures.push(make_future(peer));
        }
    }

    while let Some(result) = futures.next().await {
        if matches!(on_result(result), ControlFlow::Break(())) {
            break;
        }

        if let Some(peer) = peers_iter.next() {
            futures.push(make_future(peer));
        }
    }
}
