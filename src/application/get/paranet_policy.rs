use dkg_blockchain::BlockchainManager;
use dkg_domain::{ParsedUal, Visibility};
use dkg_network::PeerId;

use crate::application::paranet::{
    ParanetAccessError, ParanetAccessResolution, resolve_paranet_access,
};

pub(crate) async fn filter_shard_peers_for_paranet(
    blockchain_manager: &BlockchainManager,
    target_ual: &ParsedUal,
    paranet_ual: &str,
    all_shard_peers: Vec<PeerId>,
) -> Result<Vec<PeerId>, ParanetAccessError> {
    let resolution =
        resolve_paranet_access(blockchain_manager, target_ual, paranet_ual, true).await?;
    Ok(filter_shard_peers_by_resolution(
        resolution,
        all_shard_peers,
    ))
}

pub(crate) fn filter_shard_peers_by_resolution(
    resolution: ParanetAccessResolution,
    all_shard_peers: Vec<PeerId>,
) -> Vec<PeerId> {
    match resolution {
        ParanetAccessResolution::Permissioned {
            permissioned_peer_ids,
            ..
        } => all_shard_peers
            .into_iter()
            .filter(|peer_id| permissioned_peer_ids.contains(peer_id))
            .collect(),
        ParanetAccessResolution::Open => all_shard_peers,
    }
}

pub(crate) async fn resolve_effective_visibility_for_paranet(
    blockchain_manager: &BlockchainManager,
    target_ual: &ParsedUal,
    paranet_ual: Option<&str>,
    local_peer_id: &PeerId,
    remote_peer_id: &PeerId,
) -> Result<Visibility, ParanetAccessError> {
    let Some(paranet_ual) = paranet_ual else {
        return Ok(Visibility::Public);
    };

    let resolution =
        resolve_paranet_access(blockchain_manager, target_ual, paranet_ual, false).await?;
    Ok(visibility_for_resolution(
        resolution,
        local_peer_id,
        remote_peer_id,
    ))
}

pub(crate) fn visibility_for_resolution(
    resolution: ParanetAccessResolution,
    local_peer_id: &PeerId,
    remote_peer_id: &PeerId,
) -> Visibility {
    match resolution {
        ParanetAccessResolution::Open => Visibility::Public,
        ParanetAccessResolution::Permissioned {
            permissioned_peer_ids,
            ..
        } => {
            if permissioned_peer_ids.contains(local_peer_id)
                && permissioned_peer_ids.contains(remote_peer_id)
            {
                Visibility::All
            } else {
                Visibility::Public
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{filter_shard_peers_by_resolution, visibility_for_resolution};
    use crate::application::paranet::ParanetAccessResolution;
    use dkg_network::PeerId;
    use std::collections::HashSet;

    #[test]
    fn permissioned_resolution_filters_non_permissioned_peers() {
        let peer_a = PeerId::random();
        let peer_b = PeerId::random();
        let peer_c = PeerId::random();
        let resolution = ParanetAccessResolution::Permissioned {
            permissioned_peer_ids: HashSet::from([peer_a, peer_b]),
        };

        let filtered = filter_shard_peers_by_resolution(resolution, vec![peer_a, peer_b, peer_c]);
        assert_eq!(filtered, vec![peer_a, peer_b]);
    }

    #[test]
    fn visibility_is_all_only_when_both_peers_are_permissioned() {
        let peer_a = PeerId::random();
        let peer_b = PeerId::random();
        let peer_c = PeerId::random();
        let all_resolution = ParanetAccessResolution::Permissioned {
            permissioned_peer_ids: HashSet::from([peer_a, peer_b]),
        };
        assert_eq!(
            visibility_for_resolution(all_resolution, &peer_a, &peer_b),
            dkg_domain::Visibility::All
        );

        let public_resolution = ParanetAccessResolution::Permissioned {
            permissioned_peer_ids: HashSet::from([peer_a, peer_b]),
        };
        assert_eq!(
            visibility_for_resolution(public_resolution, &peer_a, &peer_c),
            dkg_domain::Visibility::Public
        );
    }
}
