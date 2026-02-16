use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc)]
    IdentityStorage,
    "abi/IdentityStorage.json"
);
