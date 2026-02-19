# Observability Rollout Plan

## 1. Problem Statement

The node already has partial tracing support (OTLP export + targeted sync pipeline spans), but it is not yet a complete operator-facing observability system:

- Traces exist, but coverage is uneven across commands/tasks/lifecycle paths.
- Metrics are not first-class yet (limited dashboard-ready SLO visibility).
- Logs are rich but not uniformly structured for cross-component correlation.
- There is no packaged, reusable dashboard/provisioning bundle for operators.
- Installer flow (`tools/installer/install.sh`) does not guide telemetry setup.

Goal: make observability opt-in, production-safe, and easy for fresh VPS operators, while preserving advanced flexibility for power users.

## 2. Goals

- Provide a complete, configurable view of node behavior:
  - success/failure
  - latency
  - throughput
  - queue/backlog pressure
  - bottlenecks by stage
- Support both:
  - self-hosted "quick start" observability
  - remote/managed observability backends
- Ship reusable dashboards and alerting defaults.
- Keep default install simple and secure.

## 3. Non-Goals (for initial rollout)

- Full multi-tenant observability control plane.
- Automatic internet-exposed Grafana/Tempo without operator confirmation.
- One-step "magic" install that hides all infra/security choices.

## 4. Current Baseline

- `src/logger/mod.rs`:
  - supports `telemetry.enabled`, `telemetry.otlp_endpoint`, `telemetry.service_name`
  - exports traces via OTLP
  - gracefully falls back to local logging if exporter init fails
- `src/config/defaults.rs`:
  - telemetry defaults currently environment-based
- `tools/installer/install.sh`:
  - installs node + db + optional blazegraph + systemd units
  - does not configure observability stack/services/dashboards

## 5. Target Architecture

Recommended reference architecture:

1. Node emits:
   - traces via OTLP
   - metrics via OTLP or `/metrics` scrape endpoint
   - logs as JSON (optionally shipped externally)
2. OpenTelemetry Collector receives telemetry and routes to:
   - Tempo (traces)
   - Prometheus/Mimir (metrics)
   - Loki (logs, optional)
3. Grafana provides dashboards + alerting.

### Deployment modes

- `remote` mode:
  - Node sends telemetry to remote OTEL collector/backend URL.
  - Best for production operators.
- `local` mode:
  - A local observability stack runs on same VPS (for testing/small setups).
  - Useful for validation and dashboard development.

## 6. Decision: Installer Behavior on Fresh VPS

Do **not** auto-install and run observability services by default.

Reasoning:
- Adds significant resource overhead and operational complexity.
- Security hardening requirements differ from node runtime requirements.
- Some operators already have managed observability stacks.
- Default path should remain stable and minimal.

### Correct approach

Add explicit installer choices:

- `--observability none|remote|local` (default: `none`)
- `--enable-telemetry` as shorthand for `--observability remote` prompt flow

Behavior:

- `none`:
  - keep current behavior; telemetry disabled unless user configures manually
- `remote`:
  - prompt for OTLP endpoint + service labels
  - write telemetry config only
  - do not install Grafana/Tempo/Prometheus locally
- `local`:
  - install and run observability stack (collector + grafana + tempo + prometheus [+ loki optional])
  - configure node to export telemetry to local collector endpoint
  - explicitly prompt for confirmation and resource usage

Important: `local` should be opt-in and clearly labeled as higher-resource mode.

## 7. Configuration Design

Extend telemetry config from a single bool/endpoint to structured options.

Proposed TOML shape:

```toml
[telemetry]
enabled = true
service_name = "rust-dkg-engine"
service_namespace = "origintrail"
node_id = "node-01"
environment = "mainnet"

[telemetry.traces]
enabled = true
otlp_endpoint = "http://127.0.0.1:4317"
sampling_ratio = 0.2

[telemetry.metrics]
enabled = true
exporter = "prometheus" # or "otlp"
bind_address = "127.0.0.1:9464" # for prometheus exporter
# otlp_endpoint = "http://127.0.0.1:4317" # for otlp exporter

[telemetry.logs]
structured = true
include_trace_context = true
```

Backward compatibility:
- keep existing fields working
- map old config to new structure during load/resolve

## 8. Instrumentation Standards

Define one shared contract for spans/log fields:

- Required span fields where applicable:
  - `operation_id`
  - `task`
  - `command`
  - `blockchain`
  - `peer_id`
  - `ual`
  - `status`
- Span naming convention:
  - `runtime.*`
  - `command.*`
  - `operation.*`
  - `task.*`
  - `network.*`
  - `repository.*`
  - `triple_store.*`

Add span boundaries at:
- command ingestion -> execution -> completion/failure
- periodic task tick -> stage -> finish
- operation lifecycle (create -> in_progress -> completed/failed)
- external IO calls (network, chain, db, triple store)

## 9. Metrics Catalog (Initial)

Create metrics for dashboards and alerting:

- Command metrics:
  - `node_command_total{command,status}`
  - `node_command_duration_seconds{command,status}` (histogram)
- Operation metrics:
  - `node_operation_total{kind,status}`
  - `node_operation_duration_seconds{kind,status}` (histogram)
- Task metrics:
  - `node_task_runs_total{task,status}`
  - `node_task_duration_seconds{task,status}` (histogram)
  - `node_task_next_delay_seconds{task}` (gauge)
- Sync pipeline:
  - `node_sync_stage_duration_seconds{stage,status}` (histogram)
  - `node_sync_queue_depth{stage}` (gauge)
  - `node_sync_kc_processed_total{stage,result}`
- Network:
  - `node_network_requests_total{protocol,outcome}`
  - `node_network_request_duration_seconds{protocol,outcome}` (histogram)
- Storage:
  - `node_repository_query_duration_seconds{query,status}` (histogram)
  - `node_triple_store_query_duration_seconds{type,status}` (histogram)

## 10. Dashboard Pack

Ship versioned dashboard pack in repo:

- `observability/grafana/dashboards/node-overview.json`
- `observability/grafana/dashboards/operations.json`
- `observability/grafana/dashboards/sync-pipeline.json`
- `observability/grafana/dashboards/network-and-storage.json`

Provisioning files:

- `observability/grafana/provisioning/datasources/*.yaml`
- `observability/grafana/provisioning/dashboards/*.yaml`

Validation workflow:
- bring up local stack
- run synthetic workload
- verify dashboard panels + traces + logs correlation

## 11. Installer Integration Plan

### Phase A (safe, immediate)

- Extend installer prompts/options for telemetry:
  - enable/disable
  - mode (`none`, `remote`, `local`)
  - endpoint when remote
- Write telemetry config section into `/etc/rust-dkg-engine/config.toml`
- Keep all observability services external for now

### Phase B (optional local stack)

- Add separate script: `tools/installer/install-observability.sh`
  - installs Docker engine + compose plugin if needed
  - deploys local observability stack from repo templates
  - writes systemd unit for stack lifecycle (or compose profile wrapper)
- Main installer calls this script only when `--observability local`

### Phase C (dashboard sync)

- Add installer step that installs dashboard pack files
- Add helper command to update dashboards on release upgrade

## 12. Security and Operations Requirements

- Local Grafana must not be internet-exposed by default.
- Default binds:
  - Grafana: `127.0.0.1`
  - OTEL collector: `127.0.0.1`
  - Prometheus: `127.0.0.1`
- Require explicit operator action to expose ports.
- Optional auth setup:
  - Grafana admin password prompt
  - basic auth/reverse proxy guidance

## 13. Testing Strategy

- Unit:
  - config parsing/resolution for new telemetry fields
  - metrics registration behavior
- Integration:
  - node -> collector -> tempo/prometheus smoke test
  - installer `--observability remote` and `--observability local`
- Regression:
  - node starts normally with telemetry disabled
  - startup still succeeds if telemetry backend unavailable (with warnings)

## 14. Rollout Phases and Milestones

### Milestone 1: Config + trace consistency

- telemetry schema extension
- instrumentation standards documented and applied to critical paths
- no metrics yet

### Milestone 2: Metrics foundation

- add metrics crate wiring
- implement core counters/histograms/gauges
- expose metrics endpoint or OTLP metrics export

### Milestone 3: Dashboard pack + local stack

- commit dashboard JSON + provisioning
- add local observability compose bundle
- document local testing workflow

### Milestone 4: Installer support

- add installer flags/prompts
- support remote mode
- optional local mode script

### Milestone 5: Alerts + operator docs

- baseline alerts for failure/backlog/no-progress
- production hardening checklist

## 15. Concrete Next Steps (Execution Order)

1. Implement telemetry config schema extension with backward compatibility.
2. Add observability mode handling to installer config generation (`none`/`remote`).
3. Add shared instrumentation guidelines doc and enforce span field consistency in command/task boundaries.
4. Introduce metrics (start with command/task/operation duration + status).
5. Create local `observability/` stack and first 3 dashboards.
6. Add optional `install-observability.sh` and wire it behind installer opt-in.

---

This plan intentionally favors safe defaults and explicit operator choice, while still enabling a turnkey path for fresh VPS users who want local dashboards quickly.
