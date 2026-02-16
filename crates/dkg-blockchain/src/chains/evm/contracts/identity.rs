use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc)]
    Identity,
    "abi/Identity.json"
);
