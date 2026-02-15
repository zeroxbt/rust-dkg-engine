use alloy::{
    contract::Error as ContractError,
    hex,
    primitives::Bytes,
    sol_types::{SolInterface, decode_revert_reason},
};

fn spelunk_hex_revert(value: &serde_json::Value) -> Option<Bytes> {
    match value {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Object(o) => o.values().find_map(spelunk_hex_revert),
        _ => None,
    }
}

fn extract_revert_data_lossy(err: &ContractError) -> Option<Bytes> {
    // Happy path: Alloy successfully extracted revert data.
    if let Some(data) = err.as_revert_data() {
        return Some(data);
    }

    // Fallback: some providers put selector-only revert data into the JSON-RPC error payload
    // in a non-standard way (or even as invalid JSON). Try to extract it anyway.
    let ContractError::TransportError(transport) = err else {
        return None;
    };

    let payload = transport.as_error_resp()?;
    let raw = payload.data.as_ref()?;
    let s = raw.get().trim();

    // Best-effort JSON parse first (works for the common `{ "data": "0x..." }` shapes).
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(s) {
        if let Some(bytes) = spelunk_hex_revert(&value) {
            return Some(bytes);
        }
    }

    // Selector-only / non-JSON `data` (we've seen `0xdeadbeef`).
    let s = s.trim_matches('"');
    s.parse().ok()
}

use super::contracts::{
    Chronos, DelegatorsInfo, Hub, Identity, IdentityStorage, KnowledgeCollectionStorage,
    ParametersStorage, Paranet, ParanetsRegistry, Profile, RandomSampling, RandomSamplingStorage,
    ShardingTable, ShardingTableStorage, Staking, Token,
};

pub(crate) fn decode_contract_error(err: &ContractError) -> Option<String> {
    macro_rules! decode_interface {
        ($iface:ty, $name:expr, $data:expr) => {
            if let Ok(decoded) = <$iface as SolInterface>::abi_decode($data) {
                return Some(format!("{}::{:?}", $name, decoded));
            }
        };
    }

    let revert_data = extract_revert_data_lossy(err)?;

    decode_interface!(Chronos::ChronosErrors, "Chronos", &revert_data);
    decode_interface!(
        DelegatorsInfo::DelegatorsInfoErrors,
        "DelegatorsInfo",
        &revert_data
    );
    decode_interface!(Hub::HubErrors, "Hub", &revert_data);
    decode_interface!(Identity::IdentityErrors, "Identity", &revert_data);
    decode_interface!(
        IdentityStorage::IdentityStorageErrors,
        "IdentityStorage",
        &revert_data
    );
    decode_interface!(
        KnowledgeCollectionStorage::KnowledgeCollectionStorageErrors,
        "KnowledgeCollectionStorage",
        &revert_data
    );
    decode_interface!(
        ParametersStorage::ParametersStorageErrors,
        "ParametersStorage",
        &revert_data
    );
    decode_interface!(Paranet::ParanetErrors, "Paranet", &revert_data);
    decode_interface!(
        ParanetsRegistry::ParanetsRegistryErrors,
        "ParanetsRegistry",
        &revert_data
    );
    decode_interface!(Profile::ProfileErrors, "Profile", &revert_data);
    decode_interface!(
        RandomSampling::RandomSamplingErrors,
        "RandomSampling",
        &revert_data
    );
    decode_interface!(
        RandomSamplingStorage::RandomSamplingStorageErrors,
        "RandomSamplingStorage",
        &revert_data
    );
    decode_interface!(
        ShardingTable::ShardingTableErrors,
        "ShardingTable",
        &revert_data
    );
    decode_interface!(
        ShardingTableStorage::ShardingTableStorageErrors,
        "ShardingTableStorage",
        &revert_data
    );
    decode_interface!(Staking::StakingErrors, "Staking", &revert_data);
    decode_interface!(Token::TokenErrors, "Token", &revert_data);

    if let Some(reason) = decode_revert_reason(&revert_data) {
        return Some(reason);
    }

    if revert_data.len() >= 4 {
        return Some(format!(
            "Unknown custom error selector 0x{}",
            hex::encode(&revert_data[..4])
        ));
    }

    Some(format!(
        "Unknown revert data 0x{}",
        hex::encode(revert_data)
    ))
}
