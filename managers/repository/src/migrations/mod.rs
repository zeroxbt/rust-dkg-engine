use sea_orm_migration::async_trait::async_trait;
pub use sea_orm_migration::prelude::*;

mod m20230922_180100_create_command;

pub struct Migrator;

#[async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(m20230922_180100_create_command::Migration)]
    }
}
