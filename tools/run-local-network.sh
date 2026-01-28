#!/bin/bash
cargo build --workspace && cargo run --manifest-path tools/local_network/Cargo.toml -- "$@"
