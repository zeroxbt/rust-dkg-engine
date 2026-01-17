use std::{fs::read_to_string, net::TcpStream, sync::Arc, time::Duration};

use alloy::{
    network::EthereumWallet,
    primitives::{Address, Bytes, U256, Uint},
    providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
    sol,
};

const HUB_CONTRACT_ADDRESS: &str = "0x5FbDB2315678afecb367f032d93F642f64180aa3";
const PRIVATE_KEYS_PATH: &str = "./tools/local_network/src/private_keys.json";
const LOCAL_BLOCKCHAIN_SCRIPT: &str = "tools/local-network-setup/run-local-blockchain.js";

sol!(
    #[sol(rpc)]
    Hub,
    "../../abi/Hub.json"
);

sol!(
    #[sol(rpc)]
    ParametersStorage,
    "../../abi/ParametersStorage.json"
);

pub struct TestParametersStorageParams {
    ask_lower_bound_factor: u128,
    ask_upper_bound_factor: u128,
    maximum_stake: u128,
    minimum_stake: u128,
    stake_withdrawal_delay: u128,
    node_ask_update_delay: u128,
    operator_fee_update_delay: u128,
    op_wallets_limit_on_profile_creation: u16,
    sharding_table_size_limit: u16,
    v81_release_epoch: u128,
}

pub struct LocalBlockchain;

impl LocalBlockchain {
    pub async fn run(port: u16, set_parameters: bool) -> Result<(), Box<dyn std::error::Error>> {
        if !Self::is_port_open(port) {
            Self::start_local_blockchain(port)?;
        }
        Self::wait_for_port(port).await?;

        if !set_parameters {
            return Ok(());
        }

        let private_keys: Vec<String> = serde_json::from_str(&read_to_string(PRIVATE_KEYS_PATH)?)?;

        let signer: PrivateKeySigner = private_keys
            .first()
            .ok_or("No private keys found")?
            .parse()?;
        let wallet = EthereumWallet::from(signer);

        let endpoint_url = format!("http://localhost:{port}").parse()?;
        let provider = Arc::new(
            ProviderBuilder::new()
                .wallet(wallet)
                .connect_http(endpoint_url),
        );

        let hub_contract = Hub::new(HUB_CONTRACT_ADDRESS.parse::<Address>()?, provider.clone());
        let parameters_storage_address: Address = hub_contract
            .getContractAddress("ParametersStorage".to_string())
            .call()
            .await?
            .0
            .into();

        Self::set_parameters_storage_params(
            &hub_contract,
            parameters_storage_address,
            TestParametersStorageParams {
                ask_lower_bound_factor: 533_000_000_000_000_000,
                ask_upper_bound_factor: 1_467_000_000_000_000_000,
                maximum_stake: 2_000_000_000_000_000_000_000_000,
                minimum_stake: 50_000_000_000_000_000_000_000,
                stake_withdrawal_delay: 60,
                node_ask_update_delay: 60,
                operator_fee_update_delay: 60,
                op_wallets_limit_on_profile_creation: 50,
                sharding_table_size_limit: 500,
                v81_release_epoch: 1,
            },
        )
        .await
    }

    async fn set_parameters_storage_params<P>(
        hub_contract: &Hub::HubInstance<P>,
        parameters_storage_address: Address,
        params: TestParametersStorageParams,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        P: alloy::providers::Provider + Clone,
    {
        let parameters_storage =
            ParametersStorage::new(parameters_storage_address, hub_contract.provider().clone());

        // Set each parameter by encoding the call and forwarding through hub
        let parameter_calls: Vec<Bytes> = vec![
            parameters_storage
                .setAskLowerBoundFactor(U256::from(params.ask_lower_bound_factor))
                .calldata()
                .clone(),
            parameters_storage
                .setAskUpperBoundFactor(U256::from(params.ask_upper_bound_factor))
                .calldata()
                .clone(),
            parameters_storage
                .setMaximumStake(Uint::<96, 2>::from(params.maximum_stake))
                .calldata()
                .clone(),
            parameters_storage
                .setMinimumStake(Uint::<96, 2>::from(params.minimum_stake))
                .calldata()
                .clone(),
            parameters_storage
                .setStakeWithdrawalDelay(U256::from(params.stake_withdrawal_delay))
                .calldata()
                .clone(),
            parameters_storage
                .setNodeAskUpdateDelay(U256::from(params.node_ask_update_delay))
                .calldata()
                .clone(),
            parameters_storage
                .setOperatorFeeUpdateDelay(U256::from(params.operator_fee_update_delay))
                .calldata()
                .clone(),
            parameters_storage
                .setOpWalletsLimitOnProfileCreation(params.op_wallets_limit_on_profile_creation)
                .calldata()
                .clone(),
            parameters_storage
                .setShardingTableSizeLimit(params.sharding_table_size_limit)
                .calldata()
                .clone(),
            parameters_storage
                .setV81ReleaseEpoch(U256::from(params.v81_release_epoch))
                .calldata()
                .clone(),
        ];

        for calldata in parameter_calls {
            hub_contract
                .forwardCall(parameters_storage_address, calldata)
                .send()
                .await?
                .get_receipt()
                .await?;
        }

        Ok(())
    }

    fn start_local_blockchain(port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let current_dir = std::env::current_dir()?;
        let dkg_engine_dir = current_dir.join("dkg-engine");
        let dkg_engine_dir_str = dkg_engine_dir
            .to_str()
            .ok_or("Failed to read dkg-engine path")?;
        let command = format!(
            "cd {} && node {} {}",
            dkg_engine_dir_str, LOCAL_BLOCKCHAIN_SCRIPT, port
        );
        std::process::Command::new("osascript")
            .args([
                "-e",
                &format!("tell app \"Terminal\" to do script \"{}\"", command),
            ])
            .output()?;

        Ok(())
    }

    fn is_port_open(port: u16) -> bool {
        let address = format!("127.0.0.1:{port}");
        let timeout = Duration::from_millis(200);
        TcpStream::connect_timeout(
            &address.parse().expect("Failed to parse RPC address"),
            timeout,
        )
        .is_ok()
    }

    async fn wait_for_port(port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let address = format!("127.0.0.1:{port}");
        let timeout = Duration::from_millis(500);
        let attempts = 120;

        for _ in 0..attempts {
            if TcpStream::connect_timeout(
                &address.parse().expect("Failed to parse RPC address"),
                timeout,
            )
            .is_ok()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Err("RPC endpoint did not become ready in time".into())
    }
}
