use alloy::sol;

sol!(
    #[derive(Debug)]
    #[sol(rpc)]
    Token,
    "abi/Token.json"
);
