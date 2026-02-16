use std::{
    fs::read_to_string, net::TcpStream, path::Path, process::Command, sync::Arc, time::Duration,
};

use alloy::{
    network::EthereumWallet,
    primitives::{Address, Bytes, U256, Uint},
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol,
};

use crate::launch::open_terminal_with_command;

const HUB_CONTRACT_ADDRESS: &str = "0x5FbDB2315678afecb367f032d93F642f64180aa3";
const MULTICALL3_ADDRESS: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";
const PRIVATE_KEYS_PATH: &str = "./tools/local_network/src/private_keys.json";
// Multicall3 runtime bytecode (compiled with evmVersion=london for local hardhat_setCode).
const MULTICALL3_RUNTIME_BYTECODE: &str = "0x60806040526004361061001e5760003560e01c806382ad56cb14610023575b600080fd5b610036610031366004610256565b61004c565b60405161004391906102cd565b60405180910390f35b60608167ffffffffffffffff81111561006757610067610382565b6040519080825280602002602001820160405280156100ad57816020015b6040805180820190915260008152606060208201528152602001906001900390816100855790505b50905060005b8281101561024f576000808585848181106100d0576100d0610398565b90506020028101906100e291906103ae565b6100f09060208101906103ce565b6001600160a01b031686868581811061010b5761010b610398565b905060200281019061011d91906103ae565b61012b9060408101906103fe565b60405161013992919061044c565b6000604051808303816000865af19150503d8060008114610176576040519150601f19603f3d011682016040523d82523d6000602084013e61017b565b606091505b5091509150811580156101c0575085858481811061019b5761019b610398565b90506020028101906101ad91906103ae565b6101be90604081019060200161045c565b155b156102115760405162461bcd60e51b815260206004820152601760248201527f4d756c746963616c6c333a2063616c6c206661696c6564000000000000000000604482015260640160405180910390fd5b604051806040016040528083151581526020018281525084848151811061023a5761023a610398565b602090810291909101015250506001016100b3565b5092915050565b6000806020838503121561026957600080fd5b823567ffffffffffffffff81111561028057600080fd5b8301601f8101851361029157600080fd5b803567ffffffffffffffff8111156102a857600080fd5b8560208260051b84010111156102bd57600080fd5b6020919091019590945092505050565b6000602082016020835280845180835260408501915060408160051b86010192506020860160005b8281101561037657603f1987860301845281518051151586526020810151905060406020870152805180604088015260005b8181101561034457602081840181015160608a8401015201610327565b506000606082890101526060601f19601f830116880101965050506020820191506020840193506001810190506102f5565b50929695505050505050565b634e487b7160e01b600052604160045260246000fd5b634e487b7160e01b600052603260045260246000fd5b60008235605e198336030181126103c457600080fd5b9190910192915050565b6000602082840312156103e057600080fd5b81356001600160a01b03811681146103f757600080fd5b9392505050565b6000808335601e1984360301811261041557600080fd5b83018035915067ffffffffffffffff82111561043057600080fd5b60200191503681900382131561044557600080fd5b9250929050565b8183823760009101908152919050565b60006020828403121561046e57600080fd5b813580151581146103f757600080fdfea26469706673582212202861c764d6e871cf60841967336eb86709b7ae46bda5824079809f014415670164736f6c634300081a0033";

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

pub(crate) struct TestParametersStorageParams {
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

pub(crate) struct LocalBlockchain;

impl LocalBlockchain {
    pub(crate) async fn run(
        port: u16,
        set_parameters: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !Self::is_port_open(port) {
            Self::start_local_blockchain(port)?;
        }
        Self::wait_for_port(port).await?;

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

        Self::ensure_multicall_deployed(provider.as_ref()).await?;

        if !set_parameters {
            return Ok(());
        }

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

    async fn ensure_multicall_deployed<P>(provider: &P) -> Result<(), Box<dyn std::error::Error>>
    where
        P: Provider,
    {
        let multicall_address: Address = MULTICALL3_ADDRESS.parse()?;
        let code = provider.get_code_at(multicall_address).await?;
        if !code.is_empty() {
            return Ok(());
        }

        let _: bool = provider
            .client()
            .request(
                "hardhat_setCode",
                (multicall_address, MULTICALL3_RUNTIME_BYTECODE),
            )
            .await?;

        Ok(())
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
        Self::ensure_evm_tools_installed()?;

        let current_dir = std::env::current_dir()?;
        let current_dir_str = current_dir.to_str().ok_or("Failed to read current dir")?;

        // Contracts deploy for local dev is sourced from dkg-evm-module directly (via tools/evm),
        // rather than relying on the vendored JS node repo under ./dkg-engine.
        let command = format!(
            "cd {} && npm --prefix tools/evm run start:local_blockchain -- {}",
            current_dir_str, port
        );
        open_terminal_with_command(&command);

        Ok(())
    }

    fn ensure_evm_tools_installed() -> Result<(), Box<dyn std::error::Error>> {
        let module_package = Path::new("tools/evm/node_modules/dkg-evm-module/package.json");
        if module_package.exists() {
            return Ok(());
        }

        println!("Installing tools/evm dependencies (first run)...");
        let status = Command::new("npm")
            .args(["--prefix", "tools/evm", "install"])
            .status()?;

        if !status.success() {
            return Err(
                "Failed to install tools/evm dependencies. Try running `npm --prefix tools/evm install`."
                    .into(),
            );
        }

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
        let attempts = 300;

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
