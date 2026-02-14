use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc)]
    Staking,
    "abi/Staking.json"
);
