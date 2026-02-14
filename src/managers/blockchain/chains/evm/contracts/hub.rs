use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc)]
    Hub,
    "abi/Hub.json"
);
