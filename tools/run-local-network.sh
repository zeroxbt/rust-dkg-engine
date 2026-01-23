#!/bin/bash
cargo build --workspace --features dev-tools && cargo run --manifest-path tools/local_network/Cargo.toml -- "$@"
