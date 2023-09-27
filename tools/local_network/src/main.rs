use clap::{value_parser, Arg, Command};
use serde_json::Value;
use std::fs;
fn open_terminal_with_command(command: &str) {
    std::process::Command::new("osascript")
        .args([
            "-e",
            &format!("tell app \"Terminal\" to do script \"{}\"", command),
        ])
        .output()
        .expect("Failed to start terminal with command");
}

fn main() {
    const NUM_CONFIGS: usize = 4;

    let matches = Command::new("Your Application Name")
        .arg(
            Arg::new("nodes")
                .short('n')
                .long("nodes")
                .value_parser(value_parser!(usize))
                .help("Number of nodes in the network"),
        )
        .get_matches();

    let nodes: usize = *matches.get_one("nodes").unwrap_or(&NUM_CONFIGS);

    let template_path = "tools/local_network/.node_config_template.json";
    let template_str = fs::read_to_string(template_path).expect("Failed to read the template file");
    let original_template_value: Value =
        serde_json::from_str(&template_str).expect("Failed to parse the template");

    for i in 0..=nodes - 1 {
        let mut template_value = original_template_value.clone();

        {
            let managers = template_value
                .get_mut("managers")
                .expect("Template format error");
            let network = managers.get_mut("network").expect("Template format error");

            let port = network["port"].as_u64().unwrap() + i as u64;
            let data_folder_path = format!("data{}", i);

            network["port"] = Value::from(port);
            network["data_folder_path"] = Value::from(data_folder_path);
        }

        {
            let managers = template_value
                .get_mut("managers")
                .expect("Template format error");
            let repository = managers
                .get_mut("repository")
                .expect("Template format error");

            let database = format!("operationaldb{}", i);

            repository["database"] = Value::from(database);
        }

        let http_port = template_value["http_api"]["port"].as_u64().unwrap() + i as u64;
        template_value["http_api"]["port"] = Value::from(http_port);

        // Serialize the modified configuration and write it to a new file
        let new_config_str =
            serde_json::to_string_pretty(&template_value).expect("Failed to serialize the config");
        let file_name = format!("tools/local_network/.node{}_config.json", i);
        fs::write(&file_name, new_config_str).expect("Failed to write the new config file");

        println!("Generated {}", file_name);
    }

    let current_dir = std::env::current_dir().expect("Failed to get current directory");
    let current_dir_str = current_dir.to_str().expect("Failed to convert Path to str");

    for i in 0..=nodes - 1 {
        let config_path = format!("tools/local_network/.node{}_config.json", i);
        open_terminal_with_command(&format!(
            "cd {} && cargo run -- --config {}",
            current_dir_str, config_path
        ));
    }
}
