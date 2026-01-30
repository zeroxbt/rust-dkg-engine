#[allow(clippy::too_many_arguments)]
pub(crate) mod paranets_registry {
    use alloy::sol;

    sol!(
        #[sol(rpc)]
        ParanetsRegistry,
        "abi/ParanetsRegistry.json"
    );
}

pub(crate) use paranets_registry::{ParanetLib::Node as PermissionedNode, ParanetsRegistry};
