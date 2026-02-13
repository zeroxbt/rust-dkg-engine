use sea_orm_migration::{MigrationTrait, MigratorTrait, async_trait::async_trait};

mod m002_create_shard;
mod m003_create_blockchain;
mod m004_create_operations;
mod m007_create_finality_status;
mod m008_create_triples_insert_count;
mod m009_create_kc_sync_progress;
mod m010_create_kc_sync_queue;
mod m011_create_proof_challenge;
mod m012_create_paranet_kc_sync;
mod m013_add_next_retry_at_to_kc_sync_queue;

pub(crate) struct Migrator;

#[async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m002_create_shard::Migration),
            Box::new(m003_create_blockchain::Migration),
            Box::new(m004_create_operations::Migration),
            Box::new(m007_create_finality_status::Migration),
            Box::new(m008_create_triples_insert_count::Migration),
            Box::new(m009_create_kc_sync_progress::Migration),
            Box::new(m010_create_kc_sync_queue::Migration),
            Box::new(m011_create_proof_challenge::Migration),
            Box::new(m012_create_paranet_kc_sync::Migration),
            Box::new(m013_add_next_retry_at_to_kc_sync_queue::Migration),
        ]
    }
}
