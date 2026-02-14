// Wrap each ABI in its own module to avoid duplicate ShardingTableLib definitions.
pub(crate) mod sharding_table {
    use alloy::sol;

    sol!(
        #[derive(Debug)]
        #[sol(rpc)]
        ShardingTable,
        "abi/ShardingTable.json"
    );
}

pub(crate) mod sharding_table_storage {
    use alloy::sol;

    sol!(
        #[derive(Debug)]
        #[sol(rpc)]
        ShardingTableStorage,
        "abi/ShardingTableStorage.json"
    );
}

pub(crate) use sharding_table::{ShardingTable, ShardingTableLib};
pub(crate) use sharding_table_storage::ShardingTableStorage;
