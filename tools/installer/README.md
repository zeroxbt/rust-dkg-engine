# rust-dkg-engine VPS Installer

This folder contains a single script that installs and configures `rust-dkg-engine` on a fresh systemd Linux VPS.

## What it does
- Installs base OS dependencies (best-effort) using your package manager.
- Installs and provisions MariaDB (default) if missing.
- Lets you choose triple store: Oxigraph (embedded, default) or Blazegraph (local service).
- Downloads `rust-dkg-engine` from GitHub Releases, verifies `SHA256SUMS`, installs under `/opt/rust-dkg-engine/releases/<version>/`, and sets `/opt/rust-dkg-engine/current`.
- Writes `/etc/rust-dkg-engine/config.toml` (mode `0640`, `root:rustdkg`) including secrets.
- Installs and starts a systemd service using `StateDirectory=rust-dkg-engine` (data under `/var/lib/rust-dkg-engine`).
- Optionally enables an auto-updater systemd timer (downloads latest release and restarts the service).
- Prompts for optional OpenTelemetry trace export configuration and writes it into `config.toml`.

## Usage
Recommended (no repo clone): run the bootstrap from the latest GitHub Release:

```bash
curl -fsSL https://github.com/zeroxbt/rust-dkg-engine/releases/latest/download/bootstrap.sh | sudo bash -s -- --version latest
```

Manual (repo checkout): run from the repo:

```bash
sudo bash tools/installer/install.sh
```

Optional flags:
- `--version latest|<tag>`: release tag to install (default `latest`)
- `--overwrite-config`: overwrite `/etc/rust-dkg-engine/config.toml` if it already exists

## Telemetry setup

During interactive install (when generating a new config), the installer asks whether to enable OpenTelemetry trace export:

- If enabled:
  - prompts for OTLP endpoint (for example `http://127.0.0.1:4317` or a remote collector URL)
  - prompts for telemetry service name
- If disabled:
  - writes `telemetry.enabled = false`

Note:
- This installer step configures node export only.
- It does **not** install local Grafana/Tempo/Prometheus services.

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

## File descriptor limit (Oxigraph)
If you see `Too many open files` errors, increase the systemd limit for the service.
The installer template sets `LimitNOFILE=1048576`. For an already-installed node you can apply it with:
```bash
sudo systemctl edit rust-dkg-engine
```
Then add:
```ini
[Service]
LimitNOFILE=1048576
```
And reload + restart:
```bash
sudo systemctl daemon-reload
sudo systemctl restart rust-dkg-engine
```

If you installed Blazegraph:
```bash
systemctl status blazegraph
journalctl -u blazegraph -f
```

## Auto-updater
If enabled, the installer sets up:
- `rust-dkg-engine-update.service` (oneshot)
- `rust-dkg-engine-update.timer` (hourly by default)

Helpful commands:
```bash
systemctl status rust-dkg-engine-update.timer
systemctl list-timers | rg rust-dkg-engine-update || true
systemctl start rust-dkg-engine-update.service
journalctl -u rust-dkg-engine-update.service -f
```
