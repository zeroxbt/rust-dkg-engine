use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc)]
    Chronos,
    "abi/Chronos.json"
);
