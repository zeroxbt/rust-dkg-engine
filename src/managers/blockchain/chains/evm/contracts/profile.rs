use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc)]
    Profile,
    "abi/Profile.json"
);
