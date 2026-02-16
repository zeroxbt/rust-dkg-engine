use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc)]
    RandomSampling,
    "abi/RandomSampling.json"
);
