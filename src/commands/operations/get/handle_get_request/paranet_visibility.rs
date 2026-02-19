use dkg_domain::{
    AccessPolicy, ParsedUal, Visibility, construct_knowledge_collection_onchain_id,
    construct_paranet_id, parse_ual,
};
use dkg_network::PeerId;

use super::handler::HandleGetRequestCommandHandler;

impl HandleGetRequestCommandHandler {
    /// Determine effective visibility based on paranet authorization.
    ///
    /// For PERMISSIONED paranets where both remote and local peers are authorized,
    /// returns `Visibility::All` (public + private data).
    /// Otherwise returns `Visibility::Public`.
    pub(crate) async fn determine_visibility_for_paranet(
        &self,
        target_ual: &ParsedUal,
        paranet_ual: Option<&str>,
        remote_peer_id: &PeerId,
    ) -> Visibility {
        let Some(paranet_ual) = paranet_ual else {
            return Visibility::Public;
        };

        // Parse paranet UAL
        let Ok(paranet_parsed) = parse_ual(paranet_ual) else {
            tracing::debug!(
                paranet_ual = %paranet_ual,
                "Failed to parse paranet UAL, using Public visibility"
            );
            return Visibility::Public;
        };

        let Some(ka_id) = paranet_parsed.knowledge_asset_id else {
            tracing::debug!(
                paranet_ual = %paranet_ual,
                "Paranet UAL missing knowledge_asset_id, using Public visibility"
            );
            return Visibility::Public;
        };

        // Construct paranet ID
        let paranet_id = construct_paranet_id(
            paranet_parsed.contract,
            paranet_parsed.knowledge_collection_id,
            ka_id,
        );

        // Get access policy
        let Ok(policy) = self
            .blockchain_manager
            .get_nodes_access_policy(&target_ual.blockchain, paranet_id)
            .await
        else {
            tracing::debug!(
                paranet_id = %paranet_id,
                "Failed to get access policy, using Public visibility"
            );
            return Visibility::Public;
        };

        if policy != AccessPolicy::Permissioned {
            tracing::debug!(
                paranet_id = %paranet_id,
                policy = ?policy,
                "Paranet is not PERMISSIONED, using Public visibility"
            );
            return Visibility::Public;
        }

        // For PERMISSIONED: verify KC is registered
        let kc_onchain_id = construct_knowledge_collection_onchain_id(
            target_ual.contract,
            target_ual.knowledge_collection_id,
        );

        let Ok(kc_registered) = self
            .blockchain_manager
            .is_knowledge_collection_registered(&target_ual.blockchain, paranet_id, kc_onchain_id)
            .await
        else {
            tracing::debug!(
                paranet_id = %paranet_id,
                "Failed to check KC registration, using Public visibility"
            );
            return Visibility::Public;
        };

        if !kc_registered {
            tracing::debug!(
                paranet_id = %paranet_id,
                kc_id = target_ual.knowledge_collection_id,
                "Knowledge collection not registered in paranet, using Public visibility"
            );
            return Visibility::Public;
        }

        // Get permissioned nodes and check both peers
        let Ok(permissioned_nodes) = self
            .blockchain_manager
            .get_permissioned_nodes(&target_ual.blockchain, paranet_id)
            .await
        else {
            tracing::debug!(
                paranet_id = %paranet_id,
                "Failed to get permissioned nodes, using Public visibility"
            );
            return Visibility::Public;
        };

        let permissioned_peer_ids: std::collections::HashSet<PeerId> = permissioned_nodes
            .iter()
            .filter_map(|node| {
                String::from_utf8(node.nodeId.to_vec())
                    .ok()
                    .and_then(|s| s.parse::<PeerId>().ok())
            })
            .collect();

        let local_peer_id = *self.network_manager.peer_id();

        if permissioned_peer_ids.contains(&local_peer_id)
            && permissioned_peer_ids.contains(remote_peer_id)
        {
            tracing::debug!(
                paranet_id = %paranet_id,
                local_peer = %local_peer_id,
                remote_peer = %remote_peer_id,
                "Both peers are permissioned, using All visibility"
            );
            Visibility::All
        } else {
            tracing::debug!(
                paranet_id = %paranet_id,
                local_peer = %local_peer_id,
                remote_peer = %remote_peer_id,
                "One or both peers not permissioned, using Public visibility"
            );
            Visibility::Public
        }
    }
}
