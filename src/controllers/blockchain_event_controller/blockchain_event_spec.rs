use std::collections::HashMap;

use alloy::{
    primitives::B256,
    sol_types::{SolEvent, SolEventInterface},
};
use blockchain::{
    AssetStorageChangedFilter, ContractChangedFilter, ContractName, Hub,
    KnowledgeCollectionCreatedFilter, KnowledgeCollectionStorage, NewAssetStorageFilter,
    NewContractFilter, ParameterChangedFilter, ParametersStorage,
};

pub enum ContractEvent {
    Hub(Hub::HubEvents),
    ParametersStorage(ParametersStorage::ParametersStorageEvents),
    KnowledgeCollectionStorage(KnowledgeCollectionStorage::KnowledgeCollectionStorageEvents),
}

fn decode_event<E: SolEventInterface>(log: &alloy::rpc::types::Log) -> Option<E> {
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

pub fn decode_contract_event(
    contract_name: &ContractName,
    log: &alloy::rpc::types::Log,
) -> Option<ContractEvent> {
    match contract_name {
        ContractName::Hub => decode_event::<Hub::HubEvents>(log).map(ContractEvent::Hub),
        ContractName::ParametersStorage => {
            decode_event::<ParametersStorage::ParametersStorageEvents>(log)
                .map(ContractEvent::ParametersStorage)
        }
        ContractName::KnowledgeCollectionStorage => {
            decode_event::<KnowledgeCollectionStorage::KnowledgeCollectionStorageEvents>(log)
                .map(ContractEvent::KnowledgeCollectionStorage)
        }
        _ => None,
    }
}
