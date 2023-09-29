mod blockchain_creator;
mod blockchains;

use blockchain_creator::BlockchainCreator;
use blockchains::abstract_blockchain::AbstractBlockchain;

use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct BlockchainConfig {
    evm_operational_wallet_private_key: String,
    evm_operational_wallet_public_key: String,
    evm_management_wallet_public_key: String,
    evm_management_wallet_private_key: Option<String>,
    hub_contract_address: String,
    rpc_endpoints: Vec<String>,
    shares_token_name: String,
    shares_token_symbol: String,
}

#[derive(Debug, Deserialize)]
pub enum Blockchain {
    Hardhat(BlockchainConfig),
    Otp(BlockchainConfig),
    Polygon(BlockchainConfig),
    Rinkeby(BlockchainConfig),
}

#[derive(Debug, Deserialize)]
pub struct BlockchainManagerConfig(Vec<Blockchain>);

pub struct BlockchainManager {
    blockchains: HashMap<String, Box<dyn AbstractBlockchain>>,
}

impl BlockchainManager {
    pub async fn new(config: BlockchainManagerConfig) -> Self {
        let mut blockchains = HashMap::new();
        for blockchain in config.0.iter() {
            match blockchain {
                Blockchain::Hardhat(c) => {
                    let blockchain = blockchains::hardhat::HardhatBlockchain::new(c.clone()).await;
                    let blockchain_name = blockchain.get_name();
                    let trait_object: Box<dyn AbstractBlockchain> = Box::new(blockchain);

                    blockchains.insert(blockchain_name, trait_object);
                }
                // Handle other blockchain types here...
                _ => {
                    panic!("no valid blockchain configuration found!");
                }
            }
        }

        Self { blockchains }
    }
}
