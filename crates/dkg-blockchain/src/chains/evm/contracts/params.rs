use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc)]
    ParametersStorage,
    "abi/ParametersStorage.json"
);
