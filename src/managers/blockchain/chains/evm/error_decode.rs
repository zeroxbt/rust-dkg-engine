use alloy::{contract::Error as ContractError, hex, sol_types::decode_revert_reason};

use super::contracts::{
    Chronos, DelegatorsInfo, Hub, Identity, IdentityStorage, KnowledgeCollectionStorage,
    ParametersStorage, Paranet, ParanetsRegistry, Profile, RandomSampling, RandomSamplingStorage,
    ShardingTable, ShardingTableStorage, Staking, Token,
};

pub(crate) fn decode_contract_error(err: &ContractError) -> Option<String> {
    macro_rules! decode_interface {
        ($iface:ty, $name:expr) => {
            if let Some(decoded) = err.as_decoded_interface_error::<$iface>() {
                return Some(format!("{}::{:?}", $name, decoded));
            }
        };
    }

    decode_interface!(Chronos::ChronosErrors, "Chronos");
    decode_interface!(DelegatorsInfo::DelegatorsInfoErrors, "DelegatorsInfo");
    decode_interface!(Hub::HubErrors, "Hub");
    decode_interface!(Identity::IdentityErrors, "Identity");
    decode_interface!(IdentityStorage::IdentityStorageErrors, "IdentityStorage");
    decode_interface!(
        KnowledgeCollectionStorage::KnowledgeCollectionStorageErrors,
        "KnowledgeCollectionStorage"
    );
    decode_interface!(
        ParametersStorage::ParametersStorageErrors,
        "ParametersStorage"
    );
    decode_interface!(Paranet::ParanetErrors, "Paranet");
    decode_interface!(ParanetsRegistry::ParanetsRegistryErrors, "ParanetsRegistry");
    decode_interface!(Profile::ProfileErrors, "Profile");
    decode_interface!(RandomSampling::RandomSamplingErrors, "RandomSampling");
    decode_interface!(
        RandomSamplingStorage::RandomSamplingStorageErrors,
        "RandomSamplingStorage"
    );
    decode_interface!(ShardingTable::ShardingTableErrors, "ShardingTable");
    decode_interface!(
        ShardingTableStorage::ShardingTableStorageErrors,
        "ShardingTableStorage"
    );
    decode_interface!(Staking::StakingErrors, "Staking");
    decode_interface!(Token::TokenErrors, "Token");

    let revert_data = err.as_revert_data()?;

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
