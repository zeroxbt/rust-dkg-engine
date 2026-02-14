use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc)]
    DelegatorsInfo,
    "abi/DelegatorsInfo.json"
);
