use std::collections::HashMap;

use crate::managers::blockchain::{
    AssetStorageChangedFilter, B256, ContractChangedFilter, ContractName, HubEvents,
    KnowledgeCollectionCreatedFilter, KnowledgeCollectionStorageEvents, Log, NewAssetStorageFilter,
    NewContractFilter, ParameterChangedFilter, ParametersStorageEvents, SolEvent,
    SolEventInterface,
};

pub(crate) enum ContractEvent {
    Hub(HubEvents),
    ParametersStorage(ParametersStorageEvents),
    KnowledgeCollectionStorage(KnowledgeCollectionStorageEvents),
}

fn decode_event<E: SolEventInterface>(log: &Log) -> Option<E> {
    E::decode_log(log.as_ref()).ok().map(|decoded| decoded.data)
}

/// Contracts and events to monitor (aligned with JS implementation).
pub(crate) fn monitored_contract_events() -> HashMap<ContractName, Vec<B256>> {
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

pub(crate) fn decode_contract_event(
    contract_name: &ContractName,
    log: &Log,
) -> Option<ContractEvent> {
    match contract_name {
        ContractName::Hub => decode_event::<HubEvents>(log).map(ContractEvent::Hub),
        ContractName::ParametersStorage => {
            decode_event::<ParametersStorageEvents>(log).map(ContractEvent::ParametersStorage)
        }
        ContractName::KnowledgeCollectionStorage => {
            decode_event::<KnowledgeCollectionStorageEvents>(log)
                .map(ContractEvent::KnowledgeCollectionStorage)
        }
        _ => None,
    }
}
