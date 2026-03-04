use std::collections::HashMap;

use alloy::{
    primitives::{Address, B256},
    rpc::types::Log,
    sol_types::{SolEvent, SolEventInterface},
};

use crate::{
    ContractName,
    chains::evm::{Hub, KnowledgeCollectionStorage, ParametersStorage},
};

/// Hub + ParametersStorage governance/admin events.
#[derive(Debug, Clone)]
pub enum AdminContractEvent {
    NewContract(Hub::NewContract),
    ContractChanged(Hub::ContractChanged),
    NewAssetStorage(Hub::NewAssetStorage),
    AssetStorageChanged(Hub::AssetStorageChanged),
    ParameterChanged(ParametersStorage::ParameterChanged),
}

/// KnowledgeCollectionStorage events.
#[derive(Debug, Clone)]
pub enum KcStorageEvent {
    KnowledgeCollectionCreated {
        event: KnowledgeCollectionStorage::KnowledgeCollectionCreated,
        contract_address: Address,
        transaction_hash: Option<B256>,
        block_number: u64,
        block_timestamp: u64,
    },
}

fn decode_event<E: SolEventInterface>(log: &Log) -> Option<E> {
    E::decode_log(log.as_ref()).ok().map(|decoded| decoded.data)
}

/// Admin contract event signatures to monitor (Hub + ParametersStorage).
pub fn monitored_admin_events() -> HashMap<ContractName, Vec<B256>> {
    let mut map = HashMap::new();
    map.insert(
        ContractName::Hub,
        vec![
            Hub::NewContract::SIGNATURE_HASH,
            Hub::ContractChanged::SIGNATURE_HASH,
            Hub::NewAssetStorage::SIGNATURE_HASH,
            Hub::AssetStorageChanged::SIGNATURE_HASH,
        ],
    );
    map.insert(
        ContractName::ParametersStorage,
        vec![ParametersStorage::ParameterChanged::SIGNATURE_HASH],
    );
    map
}

/// KnowledgeCollectionStorage event signatures.
pub fn kc_storage_event_signatures() -> Vec<B256> {
    vec![KnowledgeCollectionStorage::KnowledgeCollectionCreated::SIGNATURE_HASH]
}

/// Decode a log from a Hub or ParametersStorage contract.
pub fn decode_admin_event(contract_name: &ContractName, log: &Log) -> Option<AdminContractEvent> {
    match contract_name {
        ContractName::Hub => decode_event::<Hub::HubEvents>(log).and_then(|event| match event {
            Hub::HubEvents::NewContract(e) => Some(AdminContractEvent::NewContract(e)),
            Hub::HubEvents::ContractChanged(e) => Some(AdminContractEvent::ContractChanged(e)),
            Hub::HubEvents::NewAssetStorage(e) => Some(AdminContractEvent::NewAssetStorage(e)),
            Hub::HubEvents::AssetStorageChanged(e) => {
                Some(AdminContractEvent::AssetStorageChanged(e))
            }
            _ => None,
        }),
        ContractName::ParametersStorage => {
            decode_event::<ParametersStorage::ParametersStorageEvents>(log).map(|event| match event
            {
                ParametersStorage::ParametersStorageEvents::ParameterChanged(e) => {
                    AdminContractEvent::ParameterChanged(e)
                }
            })
        }
        _ => None,
    }
}

/// Decode a log from a KnowledgeCollectionStorage contract.
pub fn decode_kc_storage_event(log: &Log) -> Option<KcStorageEvent> {
    decode_event::<KnowledgeCollectionStorage::KnowledgeCollectionStorageEvents>(log).and_then(
        |event| match event {
            KnowledgeCollectionStorage::KnowledgeCollectionStorageEvents::KnowledgeCollectionCreated(e) => {
                Some(KcStorageEvent::KnowledgeCollectionCreated {
                    event: e,
                    contract_address: log.address(),
                    transaction_hash: log.transaction_hash,
                    block_number: log.block_number.unwrap_or_default(),
                    block_timestamp: log.block_timestamp.unwrap_or_default(),
                })
            }
            _ => None,
        },
    )
}
