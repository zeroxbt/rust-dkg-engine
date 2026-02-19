use std::collections::HashSet;

use dkg_blockchain::{B256, BlockchainManager};
use dkg_domain::{
    AccessPolicy, ParsedUal, Visibility, construct_knowledge_collection_onchain_id,
    construct_paranet_id, parse_ual,
};
use dkg_network::PeerId;

pub(crate) async fn select_shard_peers_for_paranet(
    blockchain_manager: &BlockchainManager,
    target_ual: &ParsedUal,
    paranet_ual: &str,
    all_shard_peers: Vec<PeerId>,
) -> Result<Vec<PeerId>, String> {
    let paranet_id = parse_paranet_id(paranet_ual)?;

    let exists = blockchain_manager
        .paranet_exists(&target_ual.blockchain, paranet_id)
        .await
        .map_err(|e| format!("Failed to check paranet existence: {}", e))?;
    if !exists {
        return Err(format!("Paranet does not exist: {}", paranet_ual));
    }

    let policy = blockchain_manager
        .get_nodes_access_policy(&target_ual.blockchain, paranet_id)
        .await
        .map_err(|e| format!("Failed to get access policy: {}", e))?;

    let kc_registered = is_target_kc_registered(blockchain_manager, target_ual, paranet_id)
        .await
        .map_err(|e| format!("Failed to check KC registration in paranet: {}", e))?;
    if !kc_registered {
        return Err("Knowledge collection not registered in paranet".to_string());
    }

    match policy {
        AccessPolicy::Permissioned => {
            let permissioned_peer_ids =
                permissioned_peer_ids(blockchain_manager, &target_ual.blockchain, paranet_id)
                    .await
                    .map_err(|e| format!("Failed to get permissioned nodes: {}", e))?;

            Ok(all_shard_peers
                .into_iter()
                .filter(|peer_id| permissioned_peer_ids.contains(peer_id))
                .collect())
        }
        AccessPolicy::Open => Ok(all_shard_peers),
    }
}

pub(crate) async fn determine_get_visibility_for_paranet(
    blockchain_manager: &BlockchainManager,
    target_ual: &ParsedUal,
    paranet_ual: Option<&str>,
    local_peer_id: &PeerId,
    remote_peer_id: &PeerId,
) -> Visibility {
    let Some(paranet_ual) = paranet_ual else {
        return Visibility::Public;
    };

    let paranet_id = match parse_paranet_id(paranet_ual) {
        Ok(paranet_id) => paranet_id,
        Err(e) if e.starts_with("Invalid paranet UAL:") => {
            tracing::debug!(
                paranet_ual = %paranet_ual,
                "Failed to parse paranet UAL, using Public visibility"
            );
            return Visibility::Public;
        }
        Err(_) => {
            tracing::debug!(
                paranet_ual = %paranet_ual,
                "Paranet UAL missing knowledge_asset_id, using Public visibility"
            );
            return Visibility::Public;
        }
    };

    let Ok(policy) = blockchain_manager
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

    let Ok(kc_registered) =
        is_target_kc_registered(blockchain_manager, target_ual, paranet_id).await
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

    let Ok(permissioned_peer_ids) =
        permissioned_peer_ids(blockchain_manager, &target_ual.blockchain, paranet_id).await
    else {
        tracing::debug!(
        paranet_id = %paranet_id,
        "Failed to get permissioned nodes, using Public visibility"
        );
        return Visibility::Public;
    };

    if permissioned_peer_ids.contains(local_peer_id)
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

fn parse_paranet_id(paranet_ual: &str) -> Result<B256, String> {
    let paranet_parsed =
        parse_ual(paranet_ual).map_err(|e| format!("Invalid paranet UAL: {}", e))?;
    let ka_id = paranet_parsed
        .knowledge_asset_id
        .ok_or_else(|| "Paranet UAL must include knowledge asset ID".to_string())?;

    Ok(construct_paranet_id(
        paranet_parsed.contract,
        paranet_parsed.knowledge_collection_id,
        ka_id,
    ))
}

async fn is_target_kc_registered(
    blockchain_manager: &BlockchainManager,
    target_ual: &ParsedUal,
    paranet_id: B256,
) -> Result<bool, String> {
    let kc_onchain_id = construct_knowledge_collection_onchain_id(
        target_ual.contract,
        target_ual.knowledge_collection_id,
    );

    blockchain_manager
        .is_knowledge_collection_registered(&target_ual.blockchain, paranet_id, kc_onchain_id)
        .await
        .map_err(|e| e.to_string())
}

async fn permissioned_peer_ids(
    blockchain_manager: &BlockchainManager,
    blockchain_id: &dkg_blockchain::BlockchainId,
    paranet_id: B256,
) -> Result<HashSet<PeerId>, String> {
    let permissioned_nodes = blockchain_manager
        .get_permissioned_nodes(blockchain_id, paranet_id)
        .await
        .map_err(|e| e.to_string())?;

    Ok(permissioned_nodes
        .iter()
        .filter_map(|node| {
            String::from_utf8(node.nodeId.to_vec())
                .ok()
                .and_then(|s| s.parse::<PeerId>().ok())
        })
        .collect())
}
