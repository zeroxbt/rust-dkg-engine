#[allow(clippy::too_many_arguments)]
pub mod paranet_contract {
    use alloy::sol;

    sol!(
        #[derive(Debug)]
        #[sol(rpc)]
        Paranet,
        "abi/Paranet.json"
    );
}

#[allow(clippy::too_many_arguments)]
pub mod paranets_registry {
    use alloy::sol;

    sol!(
        #[derive(Debug)]
        #[sol(rpc)]
        ParanetsRegistry,
        "abi/ParanetsRegistry.json"
    );
}

pub use paranet_contract::Paranet;
pub use paranets_registry::{ParanetLib::Node as PermissionedNode, ParanetsRegistry};
