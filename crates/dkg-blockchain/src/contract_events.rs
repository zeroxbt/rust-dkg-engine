use std::collections::HashMap;

use alloy::{
    primitives::B256,
    rpc::types::Log,
    sol_types::{SolEvent, SolEventInterface},
};

use crate::{
    AssetStorageChangedFilter, ContractChangedFilter, ContractName,
    KnowledgeCollectionCreatedFilter, NewAssetStorageFilter, NewContractFilter,
    ParameterChangedFilter,
    chains::evm::{Hub, KnowledgeCollectionStorage, ParametersStorage},
};

/// Blockchain-level event representation for monitored contracts.
#[derive(Debug, Clone)]
pub enum ContractEvent {
    NewContract(NewContractFilter),
    ContractChanged(ContractChangedFilter),
    NewAssetStorage(NewAssetStorageFilter),
    AssetStorageChanged(AssetStorageChangedFilter),
    ParameterChanged(ParameterChangedFilter),
    KnowledgeCollectionCreated(KnowledgeCollectionCreatedFilter),
}

fn decode_event<E: SolEventInterface>(log: &Log) -> Option<E> {
    E::decode_log(log.as_ref()).ok().map(|decoded| decoded.data)
}

/// Contracts and events to monitor (aligned with JS implementation).
pub fn monitored_contract_events() -> HashMap<ContractName, Vec<B256>> {
    let mut map = HashMap::new();
    map.insert(
        ContractName::Hub,
        vec![
            NewContractFilter::SIGNATURE_HASH,
            ContractChangedFilter::SIGNATURE_HASH,
            NewAssetStorageFilter::SIGNATURE_HASH,
            AssetStorageChangedFilter::SIGNATURE_HASH,
        ],
    );
    map.insert(
        ContractName::ParametersStorage,
        vec![ParameterChangedFilter::SIGNATURE_HASH],
    );
    map.insert(
        ContractName::KnowledgeCollectionStorage,
        vec![KnowledgeCollectionCreatedFilter::SIGNATURE_HASH],
    );
    map
}

/// Decode a contract log into a monitored blockchain event.
pub fn decode_contract_event(contract_name: &ContractName, log: &Log) -> Option<ContractEvent> {
    match contract_name {
        ContractName::Hub => decode_event::<Hub::HubEvents>(log).and_then(|event| match event {
            Hub::HubEvents::NewContract(filter) => Some(ContractEvent::NewContract(filter)),
            Hub::HubEvents::ContractChanged(filter) => Some(ContractEvent::ContractChanged(filter)),
            Hub::HubEvents::NewAssetStorage(filter) => Some(ContractEvent::NewAssetStorage(filter)),
            Hub::HubEvents::AssetStorageChanged(filter) => {
                Some(ContractEvent::AssetStorageChanged(filter))
            }
            _ => None,
        }),
        ContractName::ParametersStorage => {
            decode_event::<ParametersStorage::ParametersStorageEvents>(log).map(|event| {
                match event {
                    ParametersStorage::ParametersStorageEvents::ParameterChanged(filter) => {
                    ContractEvent::ParameterChanged(filter)
                    }
                }
            })
        }
        ContractName::KnowledgeCollectionStorage => {
            decode_event::<KnowledgeCollectionStorage::KnowledgeCollectionStorageEvents>(log)
                .and_then(|event| match event {
                KnowledgeCollectionStorage::KnowledgeCollectionStorageEvents::KnowledgeCollectionCreated(filter) => {
                    Some(ContractEvent::KnowledgeCollectionCreated(filter))
                }
                _ => None,
            })
        }
        _ => None,
    }
}
