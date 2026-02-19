use std::collections::HashSet;

use dkg_blockchain::{B256, BlockchainManager, U256, keccak256_encode_packed};
use dkg_domain::{AccessPolicy, ParsedUal, parse_ual};
use dkg_network::PeerId;

#[derive(Debug, Clone)]
pub(crate) struct ParsedParanet {
    pub(crate) parsed_ual: ParsedUal,
    pub(crate) paranet_id: B256,
}

#[derive(Debug, Clone)]
pub(crate) enum ParanetAccessResolution {
    Open,
    Permissioned {
        permissioned_peer_ids: HashSet<PeerId>,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum ParanetAccessError {
    InvalidParanetUal(String),
    MissingKnowledgeAssetId,
    FailedToCheckParanetExistence(String),
    ParanetDoesNotExist(String),
    FailedToGetAccessPolicy(String),
    FailedToCheckKnowledgeCollectionRegistration(String),
    KnowledgeCollectionNotRegistered,
    FailedToGetPermissionedNodes(String),
}

impl std::fmt::Display for ParanetAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidParanetUal(error) => write!(f, "Invalid paranet UAL: {}", error),
            Self::MissingKnowledgeAssetId => {
                write!(f, "Paranet UAL must include knowledge asset ID")
            }
            Self::FailedToCheckParanetExistence(error) => {
                write!(f, "Failed to check paranet existence: {}", error)
            }
            Self::ParanetDoesNotExist(paranet_ual) => {
                write!(f, "Paranet does not exist: {}", paranet_ual)
            }
            Self::FailedToGetAccessPolicy(error) => {
                write!(f, "Failed to get access policy: {}", error)
            }
            Self::FailedToCheckKnowledgeCollectionRegistration(error) => {
                write!(f, "Failed to check KC registration in paranet: {}", error)
            }
            Self::KnowledgeCollectionNotRegistered => {
                write!(f, "Knowledge collection not registered in paranet")
            }
            Self::FailedToGetPermissionedNodes(error) => {
                write!(f, "Failed to get permissioned nodes: {}", error)
            }
        }
    }
}

pub(crate) fn parse_paranet_ual_with_id(
    paranet_ual: &str,
) -> Result<ParsedParanet, ParanetAccessError> {
    let parsed_ual =
        parse_ual(paranet_ual).map_err(|e| ParanetAccessError::InvalidParanetUal(e.to_string()))?;
    let knowledge_asset_id = parsed_ual
        .knowledge_asset_id
        .ok_or(ParanetAccessError::MissingKnowledgeAssetId)?;

    let paranet_id = construct_paranet_id(
        parsed_ual.contract,
        parsed_ual.knowledge_collection_id,
        knowledge_asset_id,
    );

    Ok(ParsedParanet {
        parsed_ual,
        paranet_id,
    })
}

pub(crate) fn target_kc_onchain_id(target_ual: &ParsedUal) -> B256 {
    construct_knowledge_collection_onchain_id(
        target_ual.contract,
        target_ual.knowledge_collection_id,
    )
}

pub(crate) async fn resolve_paranet_access(
    blockchain_manager: &BlockchainManager,
    target_ual: &ParsedUal,
    paranet_ual: &str,
    require_paranet_exists: bool,
) -> Result<ParanetAccessResolution, ParanetAccessError> {
    let parsed_paranet = parse_paranet_ual_with_id(paranet_ual)?;
    let paranet_id = parsed_paranet.paranet_id;

    if require_paranet_exists {
        let exists = blockchain_manager
            .paranet_exists(&target_ual.blockchain, paranet_id)
            .await
            .map_err(|e| ParanetAccessError::FailedToCheckParanetExistence(e.to_string()))?;

        if !exists {
            return Err(ParanetAccessError::ParanetDoesNotExist(
                paranet_ual.to_string(),
            ));
        }
    }

    let policy = blockchain_manager
        .get_nodes_access_policy(&target_ual.blockchain, paranet_id)
        .await
        .map_err(|e| ParanetAccessError::FailedToGetAccessPolicy(e.to_string()))?;

    let kc_registered = blockchain_manager
        .is_knowledge_collection_registered(
            &target_ual.blockchain,
            paranet_id,
            target_kc_onchain_id(target_ual),
        )
        .await
        .map_err(|e| {
            ParanetAccessError::FailedToCheckKnowledgeCollectionRegistration(e.to_string())
        })?;

    if !kc_registered {
        return Err(ParanetAccessError::KnowledgeCollectionNotRegistered);
    }

    if policy != AccessPolicy::Permissioned {
        return Ok(ParanetAccessResolution::Open);
    }

    let permissioned_nodes = blockchain_manager
        .get_permissioned_nodes(&target_ual.blockchain, paranet_id)
        .await
        .map_err(|e| ParanetAccessError::FailedToGetPermissionedNodes(e.to_string()))?;

    Ok(ParanetAccessResolution::Permissioned {
        permissioned_peer_ids: permissioned_peer_ids_from_nodes(
            permissioned_nodes.iter().map(|node| node.nodeId.to_vec()),
        ),
    })
}

fn construct_paranet_id(
    contract: dkg_blockchain::Address,
    knowledge_collection_id: u128,
    knowledge_asset_id: u128,
) -> B256 {
    let knowledge_collection_id = U256::from(knowledge_collection_id).to_be_bytes::<32>();
    let knowledge_asset_id = U256::from(knowledge_asset_id).to_be_bytes::<32>();
    B256::from(keccak256_encode_packed(&[
        contract.as_slice(),
        &knowledge_collection_id,
        &knowledge_asset_id,
    ]))
}

fn construct_knowledge_collection_onchain_id(
    contract: dkg_blockchain::Address,
    knowledge_collection_id: u128,
) -> B256 {
    let knowledge_collection_id = U256::from(knowledge_collection_id).to_be_bytes::<32>();
    B256::from(keccak256_encode_packed(&[
        contract.as_slice(),
        &knowledge_collection_id,
    ]))
}

fn permissioned_peer_ids_from_nodes<I>(node_ids: I) -> HashSet<PeerId>
where
    I: IntoIterator<Item = Vec<u8>>,
{
    node_ids
        .into_iter()
        .filter_map(|node_id| {
            String::from_utf8(node_id)
                .ok()
                .and_then(|peer_id_str| peer_id_str.parse::<PeerId>().ok())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        construct_knowledge_collection_onchain_id, construct_paranet_id,
        permissioned_peer_ids_from_nodes,
    };
    use dkg_blockchain::Address;

    #[test]
    fn construct_paranet_id_is_deterministic() {
        let contract = Address::ZERO;
        let kc_id = 1u128;
        let ka_id = 1u128;

        let id1 = construct_paranet_id(contract, kc_id, ka_id);
        let id2 = construct_paranet_id(contract, kc_id, ka_id);
        assert_eq!(id1, id2);

        let id3 = construct_paranet_id(contract, kc_id, 2u128);
        assert_ne!(id1, id3);
    }

    #[test]
    fn construct_knowledge_collection_onchain_id_is_deterministic() {
        let contract = Address::ZERO;
        let kc_id = 1u128;

        let id1 = construct_knowledge_collection_onchain_id(contract, kc_id);
        let id2 = construct_knowledge_collection_onchain_id(contract, kc_id);
        assert_eq!(id1, id2);

        let id3 = construct_knowledge_collection_onchain_id(contract, 2u128);
        assert_ne!(id1, id3);
    }

    #[test]
    fn filters_invalid_permissioned_node_ids() {
        let valid = b"12D3KooWJ6b8uMzkA4q7hCkC4DKf8YqthpF9q2Mce3rLrFK2dP8V".to_vec();
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let invalid_peer_id = b"not-a-peer-id".to_vec();

        let permissioned_ids =
            permissioned_peer_ids_from_nodes(vec![valid, invalid_utf8, invalid_peer_id]);

        assert_eq!(permissioned_ids.len(), 1);
    }
}
