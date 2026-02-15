# rust-dkg-engine VPS Installer

This folder contains a single script that installs and configures `rust-dkg-engine` on a fresh systemd Linux VPS.

## What it does
- Installs base OS dependencies (best-effort) using your package manager.
- Installs and provisions MariaDB (default) if missing.
- Lets you choose triple store: Oxigraph (embedded, default) or Blazegraph (local service).
- Downloads `rust-dkg-engine` from GitHub Releases, verifies `SHA256SUMS`, installs under `/opt/rust-dkg-engine/releases/<version>/`, and sets `/opt/rust-dkg-engine/current`.
- Writes `/etc/rust-dkg-engine/config.toml` (mode `0640`, `root:rustdkg`) including secrets.
- Installs and starts a systemd service using `StateDirectory=rust-dkg-engine` (data under `/var/lib/rust-dkg-engine`).

## Usage
Copy the repo (or just this folder) to the VPS, then run:

```bash
sudo bash tools/installer/install.sh
```

Optional flags:
- `--version latest|<tag>`: release tag to install (default `latest`)
- `--overwrite-config`: overwrite `/etc/rust-dkg-engine/config.toml` if it already exists

## Files and locations
- Binary: `/opt/rust-dkg-engine/current/rust-dkg-engine`
- Releases: `/opt/rust-dkg-engine/releases/<tag>/`
- Config: `/etc/rust-dkg-engine/config.toml` (`root:rustdkg`, group-readable)
- Node state: `/var/lib/rust-dkg-engine/` (owned by `rustdkg`)
- Service: `rust-dkg-engine.service`

## Helpful commands
```bash
systemctl status rust-dkg-engine
systemctl start rust-dkg-engine
systemctl stop rust-dkg-engine
systemctl restart rust-dkg-engine
journalctl -u rust-dkg-engine -f
```

If you installed Blazegraph:
```bash
systemctl status blazegraph
journalctl -u blazegraph -f
```
