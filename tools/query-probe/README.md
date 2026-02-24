# query-probe

Standalone query/memory probe for the Oxigraph-backed triple store.

It runs selected query phases and prints one JSON log line per phase with:
- elapsed time
- RSS before/after/delta
- FD count before/after/delta
- requested/existing KC counts
- payload/triple counts

## Build

```bash
cargo build --manifest-path tools/query-probe/Cargo.toml --release
```

This package contains two binaries:
- `query-probe`: runs probe phases and logs JSON lines
- `generate-workload`: derives workload entries from metadata graph

## Workload format

`--workload` accepts:
- JSON array, or
- JSONL (one object per line)

Schema:

```json
{
  "kc_ual": "did:dkg:otp:2043/0xabc...",
  "start_token_id": 1,
  "end_token_id": 100,
  "burned": [5, 17]
}
```

## Example

```bash
tools/query-probe/target/release/query-probe \
  --store-path /var/lib/rust-dkg-engine/triple-store \
  --workload /tmp/workload.json \
  --iterations 20 \
  --batch-size 50 \
  --fetch-data \
  --fetch-concurrency 4 \
  --metadata-mode batch
```

## Generate Workload From Store

```bash
cargo run --manifest-path tools/query-probe/Cargo.toml --bin generate-workload -- \
  --store-path /var/lib/rust-dkg-engine/triple-store \
  --output /tmp/workload.json
```

This scans `metadata:graph` for `hasNamedGraph` links ending with `/public`,
derives:
- `kc_ual`
- `start_token_id` (min token)
- `end_token_id` (max token)
- `burned` as `[]` (unknown from triple store alone)

## Generate Workload From KC Id Range (Fast Path)

If you already know a safe KC id upper bound from sync progress, generate random candidate KCs
without global metadata scans:

```bash
cargo run --manifest-path tools/query-probe/Cargo.toml --bin generate_workload -- \
  --store-path /var/lib/rust-dkg-engine/triple-store \
  --output /tmp/workload.json \
  --blockchain-id otp:2043 \
  --contract-address 0x8f678eb0e57ee8a109b295710e23076fa3a443fe \
  --max-kc-id 4333366 \
  --sample-size 20000 \
  --seed 42
```

The generator then resolves each sampled KC boundaries (`start/end`) by querying only that KC.

## Important

Do not run against the same Oxigraph store while the node is running.
Both processes try to open RocksDB, and one will fail.
