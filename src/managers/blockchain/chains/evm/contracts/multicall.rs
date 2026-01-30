use alloy::sol;

sol! {
    /// Multicall3 contract for batching multiple calls into a single RPC request.
    /// See: https://www.multicall3.com/
    #[sol(rpc)]
    contract Multicall3 {
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }

        struct Result {
            bool success;
            bytes returnData;
        }

        function aggregate3(Call3[] calldata calls) public payable returns (Result[] memory returnData);
    }
}
