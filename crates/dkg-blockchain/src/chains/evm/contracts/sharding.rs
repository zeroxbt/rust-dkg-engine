// Wrap each ABI in its own module to avoid duplicate ShardingTableLib definitions.
pub mod sharding_table {
    use alloy::sol;

    sol!(
        #[derive(Debug)]
        #[sol(rpc)]
        ShardingTable,
        "abi/ShardingTable.json"
    );
}

pub mod sharding_table_storage {
    use alloy::sol;

    sol!(
        #[derive(Debug)]
        #[sol(rpc)]
        ShardingTableStorage,
        "abi/ShardingTableStorage.json"
    );
}

pub use sharding_table::{ShardingTable, ShardingTableLib};
pub use sharding_table_storage::ShardingTableStorage;
