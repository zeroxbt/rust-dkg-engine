use std::sync::Arc;

use async_trait::async_trait;
use ethers::contract::abigen;

use crate::{
    blockchain_creator::{BlockchainProvider, Contracts},
    BlockchainConfig,
};

#[async_trait]
pub trait AbstractBlockchain {
    fn get_name(&self) -> String;
    fn get_config(&self) -> &BlockchainConfig;
    fn get_provider(&self) -> Arc<BlockchainProvider>;
    fn get_contracts(&self) -> &Contracts;
}
