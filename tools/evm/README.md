# EVM Tools (Local Dev)

This folder is a minimal wrapper around `dkg-evm-module` for local contract deployment.

Goals:
- Local contract deploy does not depend on the vendored JS node under `./dkg-engine`.
- The `dkg-evm-module` ref is pinned (see `dkg-evm-module.lock` at repo root).

Usage (local):
```bash
cd tools/evm
npm install
```

Then, from repo root:
```bash
npm --prefix tools/evm run start:local_blockchain -- 8545
```

ABI sync/check infers the required ABI subset from code references in `src/` and
`tools/local_network/`.
