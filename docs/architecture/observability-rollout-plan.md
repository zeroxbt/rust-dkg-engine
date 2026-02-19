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

## 2.1 Delivery Strategy (Vertical Slice First)

Before broad observability coverage, deliver one complete thin slice:

1. Minimal telemetry plumbing for metrics (in addition to existing traces)
2. Minimal instrumentation set (command/task + sync heartbeat)
3. One dashboard (`Node Overview`)
4. End-to-end validation in Grafana

Why:
- validates data model and label conventions early
- catches exporter/query/dashboard issues before scaling
- gives immediate operator value with low implementation risk

## 3. Non-Goals (for initial rollout)

- Full multi-tenant observability control plane.
- Automatic internet-exposed Grafana/Tempo without operator confirmation.
- One-step "magic" install that hides all infra/security choices.

## 4. Current Baseline

- `src/logger/mod.rs`:
  - supports structured telemetry config:
    - `[telemetry.traces]` (OTLP traces)
    - `[telemetry.metrics]` (Prometheus `/metrics`)
  - traces and metrics can be enabled independently
  - gracefully falls back to local logging if exporter init fails
- `src/config/defaults.rs`:
  - telemetry defaults currently environment-based (`traces` enabled in development)
- `tools/installer/install.sh`:
  - installs node + db + optional blazegraph + systemd units
  - prompts for trace and metrics export config
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

Use structured telemetry config with independent signals.

Current TOML shape:

```toml
[telemetry]

[telemetry.traces]
enabled = true
otlp_endpoint = "http://127.0.0.1:4317"
service_name = "rust-dkg-engine"

[telemetry.metrics]
enabled = true
bind_address = "127.0.0.1:9464"
```

Breaking change:
- legacy flat keys under `[telemetry]` are no longer supported:
  - `enabled`
  - `otlp_endpoint`
  - `service_name`
- supported shape is nested only:
  - `[telemetry.traces]`
  - `[telemetry.metrics]`

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

## 9. Metrics Catalog

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

### 9.1 Pilot Metric Set (implement first)

Use this subset for the first dashboard:

- `node_command_total{command,status}` (counter)
- `node_command_duration_seconds{command,status}` (histogram)
- `node_task_runs_total{task,status}` (counter)
- `node_task_duration_seconds{task,status}` (histogram)
- `node_sync_last_success_unix` (gauge heartbeat)

Everything else in section 9 follows only after the pilot is validated.

## 10. Dashboard Pack

### 10.1 Pilot dashboard (ship first)

- `observability/grafana/dashboards/node-overview.json`

Panels (minimum):
- command throughput (`rate(node_command_total[5m])`)
- command error ratio
- command latency p95/p99 from histogram
- task success/failure rate
- sync heartbeat/no-progress panel from `node_sync_last_success_unix`

### 10.2 Full dashboard pack (after pilot)

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

### Milestone 1: Pilot telemetry slice

- telemetry metrics config extension (minimal)
- implement pilot metric set (section 9.1)
- ship `Node Overview` dashboard
- verify dashboard correctness in local Grafana

### Milestone 2: Config + trace consistency expansion

- telemetry schema hardening + backward compatibility cleanup
- instrumentation standards applied beyond pilot boundaries

### Milestone 3: Metrics foundation expansion

- extend from pilot metrics to full catalog in section 9
- add remaining counters/histograms/gauges incrementally

### Milestone 4: Dashboard pack + local stack

- commit dashboard JSON + provisioning
- add local observability compose bundle
- document local testing workflow

### Milestone 5: Installer support

- add installer flags/prompts
- support remote mode
- optional local mode script

### Milestone 6: Alerts + operator docs

- baseline alerts for failure/backlog/no-progress
- production hardening checklist

## 15. Concrete Next Steps (Execution Order)

1. Implement minimal metrics export plumbing and pilot metrics set (section 9.1).
2. Create and validate `Node Overview` dashboard in local Grafana.
3. Expand telemetry schema and span-field consistency after pilot validation.
4. Add observability mode handling to installer config generation (`none`/`remote`/`local`).
5. Create local `observability/` stack and additional dashboards.
6. Add optional `install-observability.sh` and wire it behind installer opt-in.

---

This plan intentionally favors safe defaults and explicit operator choice, while still enabling a turnkey path for fresh VPS users who want local dashboards quickly.
