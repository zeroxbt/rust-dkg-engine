use std::{convert::TryFrom, fs::read_to_string, net::TcpStream, sync::Arc, time::Duration};

use ethers::{
    abi::{Abi, Token},
    core::{k256::ecdsa::SigningKey, types::Address},
    middleware::{MiddlewareBuilder, SignerMiddleware},
    providers::{Http, Provider},
    signers::{LocalWallet, Signer, Wallet},
    types::{Bytes, U256},
};

const HARDHAT_CHAIN_ID: u64 = 31337;
const HUB_CONTRACT_ADDRESS: &str = "0x5FbDB2315678afecb367f032d93F642f64180aa3";
const PRIVATE_KEYS_PATH: &str = "./tools/local_network/src/private_keys.json";
const PARAMETERS_STORAGE_ABI_PATH: &str = "./abi/ParametersStorage.json";
const LOCAL_BLOCKCHAIN_SCRIPT: &str = "tools/local-network-setup/run-local-blockchain.js";

ethers::contract::abigen!(Hub, "../../abi/Hub.json");
ethers::contract::abigen!(ParametersStorage, "../../abi/ParametersStorage.json");

pub struct TestParametersStorageParams {
    epoch_length: u64,                 // 6 minutes
    commit_window_duration_perc: u64,  // 2 minutes
    min_proof_window_offset_perc: u64, // 4 minutes
    max_proof_window_offset_perc: u64, // 4 minutes
    proof_window_duration_perc: u64,   // 2 minutes
    finalization_commits_number: u64,
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

        let signer = private_keys
            .first()
            .ok_or("No private keys found")?
            .parse::<LocalWallet>()?
            .with_chain_id(HARDHAT_CHAIN_ID);
        let provider = Arc::new(
            Provider::<Http>::try_from(format!("http://localhost:{port}"))?.with_signer(signer),
        );

        let hub_contract = Hub::new(
            HUB_CONTRACT_ADDRESS.parse::<Address>()?,
            Arc::clone(&provider),
        );
        let parameters_storage_address: Address = hub_contract
            .get_contract_address("ParametersStorage".to_string())
            .call()
            .await?;
        let parameters_storage_interface: Abi =
            serde_json::from_str(&read_to_string(PARAMETERS_STORAGE_ABI_PATH)?)?;

        Self::set_parameters_storage_params(
            &hub_contract,
            &parameters_storage_interface,
            parameters_storage_address,
            TestParametersStorageParams {
                epoch_length: 6 * 60,
                commit_window_duration_perc: 33,
                min_proof_window_offset_perc: 66,
                max_proof_window_offset_perc: 66,
                proof_window_duration_perc: 33,
                finalization_commits_number: 3,
            },
        )
        .await
    }

    async fn set_parameters_storage_params(
        hub_contract: &Hub<SignerMiddleware<Provider<Http>, Wallet<SigningKey>>>,
        parameters_storage_interface: &Abi,
        parameters_storage_address: Address,
        params: TestParametersStorageParams,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let parameter_methods = [
            ("setEpochLength", params.epoch_length),
            (
                "setCommitWindowDurationPerc",
                params.commit_window_duration_perc,
            ),
            (
                "setMinProofWindowOffsetPerc",
                params.min_proof_window_offset_perc,
            ),
            (
                "setMaxProofWindowOffsetPerc",
                params.max_proof_window_offset_perc,
            ),
            (
                "setProofWindowDurationPerc",
                params.proof_window_duration_perc,
            ),
            (
                "setFinalizationCommitsNumber",
                params.finalization_commits_number,
            ),
        ];

        for (method, value) in &parameter_methods {
            let encoded_data = parameters_storage_interface
                .function(method)?
                .encode_input(&[Token::Uint(U256::from(*value))])?;
            hub_contract
                .forward_call(parameters_storage_address, Bytes::from(encoded_data))
                .send()
                .await?
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
