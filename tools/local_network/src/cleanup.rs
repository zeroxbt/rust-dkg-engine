use std::{fs, path::Path, process::Command as StdCommand};

use crate::constants::{BLAZEGRAPH_URL, BOOTSTRAP_KEY_PATH};

pub(crate) fn drop_database(database_name: &str) {
    let drop_command = format!(
        "echo \"DROP DATABASE IF EXISTS {}\" | mysql -u root",
        database_name
    );
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(&drop_command)
        .output()
        .expect("Failed to execute drop database command");

    if !output.status.success() {
        eprintln!("Failed to drop database '{}'.", database_name);
    }
}

pub(crate) fn clear_rust_node_data(data_folder: &str, preserve_network_key: bool) {
    let data_path = Path::new(data_folder);

    if !data_path.exists() {
        return;
    }

    // If we need to preserve the network key, save it first
    let key_path = data_path.join(BOOTSTRAP_KEY_PATH);
    let saved_key = if preserve_network_key && key_path.exists() {
        fs::read(&key_path).ok()
    } else {
        None
    };

    // Remove the entire data folder
    match fs::remove_dir_all(data_path) {
        Ok(_) => println!("Cleared Rust node data folder: {}", data_path.display()),
        Err(e) => {
            eprintln!("Failed to clear data folder '{}': {}", data_path.display(), e);
            return;
        }
    }

    // Recreate the data folder
    fs::create_dir_all(data_path).expect("Failed to recreate data folder");

    // Restore the network key if we saved it
    if let Some(key_bytes) = saved_key {
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent).expect("Failed to recreate network key directory");
        }
        fs::write(&key_path, key_bytes).expect("Failed to restore network key");
        println!("Restored network key: {}", key_path.display());
    }
}

/// Delete Blazegraph namespaces for a node.
/// Each JS node uses three namespaces: dkg-{i}, private-current-{i}, public-current-{i}
pub(crate) fn delete_blazegraph_namespaces(node_index: usize) {
    let namespaces = [
        format!("dkg-{}", node_index),
        format!("private-current-{}", node_index),
        format!("public-current-{}", node_index),
    ];

    for namespace in &namespaces {
        let url = format!("{}/blazegraph/namespace/{}", BLAZEGRAPH_URL, namespace);

        // Use curl to send DELETE request to Blazegraph
        let output = StdCommand::new("curl")
            .args(["-s", "-X", "DELETE", &url])
            .output();

        match output {
            Ok(result) => {
                if result.status.success() {
                    println!("Deleted Blazegraph namespace: {}", namespace);
                }
                // Silently ignore errors (namespace might not exist)
            }
            Err(_) => {
                // curl not available or other error - silently continue
            }
        }
    }
}
