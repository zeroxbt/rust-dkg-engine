use ethers::abi::{Abi, Token};
use ethers::core::types::Address;
use ethers::prelude::*;
use ethers::providers::{Http, Provider};
use ethers::signers::LocalWallet;
use k256::ecdsa::SigningKey;
use std::convert::TryFrom;
use std::fs::read_to_string;
use std::sync::Arc;

const HUB_CONTRACT_ADDRESS: &str = "0x5FbDB2315678afecb367f032d93F642f64180aa3";
const PRIVATE_KEYS_PATH: &str = "./tools/local_network/src/private_keys.json";
const PARAMETERS_STORAGE_ABI_PATH: &str = "./abi/ParametersStorage.json";

ethers::contract::abigen!(Hub, "../../abi/Hub.json");
ethers::contract::abigen!(HubController, "../../abi/HubController.json");
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
    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let private_keys: Vec<String> = serde_json::from_str(&read_to_string(PRIVATE_KEYS_PATH)?)?;

        let signer = private_keys
            .get(0)
            .ok_or("No private keys found")?
            .parse::<LocalWallet>()?;
        let provider =
            Arc::new(Provider::<Http>::try_from("http://localhost:8545")?.with_signer(signer));

        let hub_contract = Hub::new(
            HUB_CONTRACT_ADDRESS.parse::<Address>()?,
            Arc::clone(&provider),
        );
        let hub_controller_address: Address = hub_contract.owner().call().await?;
        let hub_controller_contract =
            HubController::new(hub_controller_address, Arc::clone(&provider));

        let parameters_storage_address: Address = hub_contract
            .get_contract_address("ParametersStorage".to_string())
            .call()
            .await?;
        let parameters_storage_interface: Abi =
            serde_json::from_str(&read_to_string(PARAMETERS_STORAGE_ABI_PATH)?)?;

        Self::set_parameters_storage_params(
            &hub_controller_contract,
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
        hub_controller: &HubController<SignerMiddleware<Provider<Http>, Wallet<SigningKey>>>,
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
            hub_controller
                .forward_call(parameters_storage_address, Bytes::from(encoded_data))
                .call()
                .await?;
        }

        Ok(())
    }
}
