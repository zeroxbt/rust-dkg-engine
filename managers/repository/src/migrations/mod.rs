use sea_orm_migration::async_trait::async_trait;
pub use sea_orm_migration::prelude::*;

mod m001_create_command;
mod m002_create_shard;
mod m003_create_blockchain;
mod m004_create_operation_ids;

pub struct Migrator;

#[async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m001_create_command::Migration),
            Box::new(m002_create_shard::Migration),
            Box::new(m003_create_blockchain::Migration),
            Box::new(m004_create_operation_ids::Migration),
        ]
    }
}
