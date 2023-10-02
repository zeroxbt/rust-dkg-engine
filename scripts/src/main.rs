use clap::{Arg, Command};
use ethers::core::types::Address;
use ethers::middleware::MiddlewareBuilder;
use ethers::providers::{Http, Provider};
use ethers::signers::{LocalWallet, Signer};
use ethers::utils::parse_ether;
use std::sync::Arc;

ethers::contract::abigen!(Hub, "./../abi/Hub.json");
ethers::contract::abigen!(Profile, "./../abi/Profile.json");
ethers::contract::abigen!(Staking, "./../abi/Staking.json");
ethers::contract::abigen!(IdentityStorage, "./../abi/IdentityStorage.json");
ethers::contract::abigen!(Token, "./../abi/Token.json");

const HARDHAT_CHAIN_ID: u64 = 31337;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = Command::new("rust_ot_node")
        .subcommand(
            Command::new("set-stake")
                .arg(Arg::new("rpcEndpoint").long("rpcEndpoint").required(true))
                .arg(Arg::new("stake").long("stake").required(true))
                .arg(
                    Arg::new("operationalWalletPrivateKey")
                        .long("operationalWalletPrivateKey")
                        .required(true),
                )
                .arg(
                    Arg::new("managementWalletPrivateKey")
                        .long("managementWalletPrivateKey")
                        .required(true),
                )
                .arg(
                    Arg::new("hubContractAddress")
                        .long("hubContractAddress")
                        .required(true),
                ), // Add required arguments for set-stake here...
        )
        .subcommand(
            Command::new("set-ask")
                .arg(Arg::new("rpcEndpoint").long("rpcEndpoint").required(true))
                .arg(Arg::new("ask").long("ask").required(true))
                .arg(Arg::new("privateKey").long("privateKey").required(true))
                .arg(
                    Arg::new("hubContractAddress")
                        .long("hubContractAddress")
                        .required(true),
                ), // Add required arguments for set-ask here...
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("set-stake") {
        set_stake(matches).await?;
    } else if let Some(matches) = matches.subcommand_matches("set-ask") {
        set_ask(matches).await?;
    }

    Ok(())
}

async fn set_stake(matches: &clap::ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_endpoint: &String = matches.get_one("rpcEndpoint").unwrap();
    let stake = matches.get_one::<String>("stake").unwrap();
    let operational_wallet_pk = matches
        .get_one::<String>("operationalWalletPrivateKey")
        .unwrap();
    let management_wallet_pk = matches
        .get_one::<String>("managementWalletPrivateKey")
        .unwrap();
    let hub_contract_address: Address = matches
        .get_one::<String>("hubContractAddress")
        .unwrap()
        .parse()?;

    let provider = Provider::<Http>::try_from(rpc_endpoint)?;
    let operational_wallet = operational_wallet_pk
        .parse::<LocalWallet>()?
        .with_chain_id(HARDHAT_CHAIN_ID);
    let management_wallet = management_wallet_pk
        .parse::<LocalWallet>()?
        .with_chain_id(HARDHAT_CHAIN_ID);

    // Use ethers contract abigen to generate bindings for the ABIs
    let hub_contract = Hub::new(
        hub_contract_address,
        Arc::new(provider.clone().with_signer(operational_wallet.clone())),
    );

    let staking_contract_address: Address = hub_contract
        .get_contract_address("Staking".to_string())
        .call()
        .await?;
    let staking_contract = Staking::new(
        staking_contract_address,
        Arc::new(provider.clone().with_signer(management_wallet.clone())),
    );

    let identity_storage_address: Address = hub_contract
        .get_contract_address("IdentityStorage".to_string())
        .call()
        .await?;
    let identity_storage = IdentityStorage::new(
        identity_storage_address,
        Arc::new(provider.clone().with_signer(operational_wallet.clone())),
    );

    let identity_id = identity_storage
        .get_identity_id(operational_wallet.address())
        .call()
        .await?;

    let token_contract_address: Address = hub_contract
        .get_contract_address("Token".to_string())
        .call()
        .await?;
    let token_contract = Token::new(
        token_contract_address,
        Arc::new(provider.clone().with_signer(management_wallet)),
    );

    let stake_wei = parse_ether(stake)?;

    token_contract
        .increase_allowance(staking_contract_address, stake_wei)
        .send()
        .await?
        .await?;

    staking_contract
        .add_stake(identity_id, stake_wei.as_u128())
        .send()
        .await?
        .await?;

    println!("Set stake completed");
    Ok(())
}

async fn set_ask(matches: &clap::ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    let rpc_endpoint: &String = matches.get_one("rpcEndpoint").unwrap();
    let ask = matches.get_one::<String>("ask").unwrap();
    let wallet_pk = matches.get_one::<String>("privateKey").unwrap();
    let hub_contract_address: Address = matches
        .get_one::<String>("hubContractAddress")
        .unwrap()
        .parse()?;

    let provider = Provider::<Http>::try_from(rpc_endpoint)?;
    let wallet = wallet_pk
        .parse::<LocalWallet>()?
        .with_chain_id(HARDHAT_CHAIN_ID);

    let hub_contract = Hub::new(
        hub_contract_address,
        Arc::new(provider.clone().with_signer(wallet.clone())),
    );

    let profile_address: Address = hub_contract
        .get_contract_address("Profile".to_string())
        .call()
        .await?;
    let profile = Profile::new(
        profile_address,
        Arc::new(provider.clone().with_signer(wallet.clone())),
    );

    let identity_storage_address: Address = hub_contract
        .get_contract_address("IdentityStorage".to_string())
        .call()
        .await?;
    let identity_storage = IdentityStorage::new(
        identity_storage_address,
        Arc::new(provider.clone().with_signer(wallet.clone())),
    );

    let identity_id = identity_storage
        .get_identity_id(wallet.address())
        .call()
        .await?;

    let ask_wei = parse_ether(ask)?;

    profile
        .set_ask(identity_id, ask_wei.as_u128())
        .send()
        .await?
        .await?;

    println!("Set ask completed");
    Ok(())
}
