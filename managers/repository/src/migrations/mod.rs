use sea_orm_migration::{MigrationTrait, MigratorTrait, async_trait::async_trait};

mod m001_create_command;
mod m002_create_shard;
mod m003_create_blockchain;
mod m004_create_operations;
mod m005_create_operations_responses;

pub struct Migrator;

#[async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m001_create_command::Migration),
            Box::new(m002_create_shard::Migration),
            Box::new(m003_create_blockchain::Migration),
            Box::new(m004_create_operations::Migration),
            Box::new(m005_create_operations_responses::Migration),
        ]
    }
}
