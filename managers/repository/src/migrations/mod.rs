use sea_orm_migration::{MigrationTrait, MigratorTrait, async_trait::async_trait};

mod m002_create_shard;
mod m003_create_blockchain;
mod m004_create_operations;
mod m006_create_signatures;
mod m007_create_finality_status;
mod m008_create_triples_insert_count;

pub struct Migrator;

#[async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m002_create_shard::Migration),
            Box::new(m003_create_blockchain::Migration),
            Box::new(m004_create_operations::Migration),
            Box::new(m006_create_signatures::Migration),
            Box::new(m007_create_finality_status::Migration),
            Box::new(m008_create_triples_insert_count::Migration),
        ]
    }
}
