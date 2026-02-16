use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc)]
    RandomSamplingStorage,
    "abi/RandomSamplingStorage.json"
);
