use std::{path::Path, process::Command as StdCommand};

use crate::constants::BINARY_NAME;

pub(crate) fn open_terminal_with_command(command: &str) {
    StdCommand::new("osascript")
        .args([
            "-e",
            &format!("tell app \"Terminal\" to do script \"{}\"", command),
        ])
        .output()
        .expect("Failed to start terminal with command");
}

pub(crate) fn get_binary_path(prefer_release: bool) -> String {
    let release_path = format!("./target/release/{}", BINARY_NAME);
    let debug_path = format!("./target/debug/{}", BINARY_NAME);

    if prefer_release {
        if Path::new(&release_path).exists() {
            return release_path;
        }
        if Path::new(&debug_path).exists() {
            return debug_path;
        }
    } else {
        if Path::new(&debug_path).exists() {
            return debug_path;
        }
        if Path::new(&release_path).exists() {
            return release_path;
        }
    }

    panic!(
        "Binary '{}' not found. Please run 'cargo build' or 'cargo build --release' first.",
        BINARY_NAME
    );
}
