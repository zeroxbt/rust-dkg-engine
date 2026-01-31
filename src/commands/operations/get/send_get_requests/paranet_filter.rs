use libp2p::PeerId;
use uuid::Uuid;

use super::handler::SendGetRequestsCommandHandler;
use crate::{
    types::{AccessPolicy, ParsedUal, parse_ual},
    utils::paranet::{construct_knowledge_collection_onchain_id, construct_paranet_id},
};

impl SendGetRequestsCommandHandler {
    /// Handle paranet validation and return filtered peers based on access policy.
    ///
    /// For PERMISSIONED paranets, only returns nodes that are in the permissioned list.
    /// For OPEN paranets, returns all provided peers.
    ///
    /// Returns Ok(peers) for valid paranets, or Err(()) if validation fails
    /// (operation is marked as failed in that case).
    pub(crate) async fn handle_paranet_node_selection(
        &self,
        operation_id: Uuid,
        paranet_ual: &str,
        target_ual: &ParsedUal,
        all_shard_peers: Vec<PeerId>,
    ) -> Result<Vec<PeerId>, ()> {
        // 1. Parse paranet UAL
        let paranet_parsed = match parse_ual(paranet_ual) {
            Ok(p) => p,
            Err(e) => {
                let error_message = format!("Invalid paranet UAL: {}", e);
                tracing::error!(
                    operation_id = %operation_id,
                    %error_message,
                    "Paranet UAL parsing failed"
                );
                self.get_operation_status_service
                    .mark_failed(operation_id, error_message)
                    .await;
                return Err(());
            }
        };

        // 2. Validate paranet UAL has knowledge_asset_id
        let Some(ka_id) = paranet_parsed.knowledge_asset_id else {
            let error_message = "Paranet UAL must include knowledge asset ID".to_string();
            tracing::error!(
                operation_id = %operation_id,
                %error_message,
                "Paranet UAL missing knowledge asset ID"
            );
            self.get_operation_status_service
                .mark_failed(operation_id, error_message)
                .await;
            return Err(());
        };

        // 3. Construct paranet ID
        let paranet_id = construct_paranet_id(
            paranet_parsed.contract,
            paranet_parsed.knowledge_collection_id,
            ka_id,
        );

        tracing::debug!(
            operation_id = %operation_id,
            paranet_id = %paranet_id,
            "Constructed paranet ID"
        );

        // 4. Check paranet exists
        let exists = self
            .blockchain_manager
            .paranet_exists(&target_ual.blockchain, paranet_id)
            .await
            .unwrap_or(false);

        if !exists {
            let error_message = format!("Paranet does not exist: {}", paranet_ual);
            tracing::error!(
                operation_id = %operation_id,
                %error_message,
                "Paranet not found"
            );
            self.get_operation_status_service
                .mark_failed(operation_id, error_message)
                .await;
            return Err(());
        }

        // 5. Get access policy
        let policy = match self
            .blockchain_manager
            .get_nodes_access_policy(&target_ual.blockchain, paranet_id)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                let error_message = format!("Failed to get access policy: {}", e);
                tracing::error!(
                    operation_id = %operation_id,
                    %error_message,
                    "Failed to load paranet access policy"
                );
                self.get_operation_status_service
                    .mark_failed(operation_id, error_message)
                    .await;
                return Err(());
            }
        };

        tracing::debug!(
            operation_id = %operation_id,
            policy = ?policy,
            "Retrieved paranet access policy"
        );

        // 6. Check KC is registered in paranet
        let kc_onchain_id = construct_knowledge_collection_onchain_id(
            target_ual.contract,
            target_ual.knowledge_collection_id,
        );
        let kc_registered = self
            .blockchain_manager
            .is_knowledge_collection_registered(&target_ual.blockchain, paranet_id, kc_onchain_id)
            .await
            .unwrap_or(false);

        if !kc_registered {
            let error_message = "Knowledge collection not registered in paranet".to_string();
            tracing::error!(
                operation_id = %operation_id,
                %error_message,
                "Knowledge collection not registered in paranet"
            );
            self.get_operation_status_service
                .mark_failed(operation_id, error_message)
                .await;
            return Err(());
        }

        // 7. Filter peers based on policy
        match policy {
            AccessPolicy::Permissioned => {
                let permissioned_nodes = match self
                    .blockchain_manager
                    .get_permissioned_nodes(&target_ual.blockchain, paranet_id)
                    .await
                {
                    Ok(nodes) => nodes,
                    Err(e) => {
                        let error_message = format!("Failed to get permissioned nodes: {}", e);
                        tracing::error!(
                            operation_id = %operation_id,
                            %error_message,
                            "Failed to fetch permissioned nodes"
                        );
                        self.get_operation_status_service
                            .mark_failed(operation_id, error_message)
                            .await;
                        return Err(());
                    }
                };

                // Convert node_id bytes to peer IDs
                let permissioned_peer_ids: std::collections::HashSet<PeerId> = permissioned_nodes
                    .iter()
                    .filter_map(|node| {
                        String::from_utf8(node.nodeId.to_vec())
                            .ok()
                            .and_then(|s| s.parse::<PeerId>().ok())
                    })
                    .collect();

                tracing::debug!(
                    operation_id = %operation_id,
                    permissioned_count = permissioned_peer_ids.len(),
                    "Retrieved permissioned nodes for paranet"
                );

                let my_peer_id = *self.network_manager.peer_id();
                let filtered: Vec<PeerId> = all_shard_peers
                    .into_iter()
                    .filter(|peer_id| {
                        *peer_id != my_peer_id && permissioned_peer_ids.contains(peer_id)
                    })
                    .collect();

                tracing::debug!(
                    operation_id = %operation_id,
                    filtered_count = filtered.len(),
                    "Filtered peers to permissioned set"
                );

                Ok(filtered)
            }
            AccessPolicy::Open => {
                tracing::debug!(
                    operation_id = %operation_id,
                    "Paranet is OPEN, using all shard nodes"
                );
                Ok(all_shard_peers)
            }
        }
    }
}
