# Observability Quick Start (Pilot)

This directory contains pilot dashboards for validating node telemetry end-to-end.

Telemetry signals used here:

- Traces: timeline of one request/operation across components (for drill-down)
- Metrics: aggregated numeric time series (for dashboards/alerts)

Quick intuition:

- A trace answers: "why was this specific publish/get slow or failing?"
- A metric answers: "how many publishes failed in the last 10 minutes?"
- You can enable one without the other.

## Prerequisites

1. Node config enables telemetry signals you need:

```toml
[telemetry.traces]
enabled = false
otlp_endpoint = "http://127.0.0.1:4317"
service_name = "rust-dkg-engine"

[telemetry.metrics]
enabled = true
bind_address = "127.0.0.1:9464"
```

2. Start/restart the node.
3. If metrics are enabled, Prometheus scrapes `http://<node-host>:9464/metrics`.
4. Grafana has a Prometheus datasource.

## Dashboard import

- Dashboard files:
  - `observability/grafana/dashboards/node-overview.json`
  - `observability/grafana/dashboards/operations.json`
  - `observability/grafana/dashboards/triple-store.json`
  - `observability/grafana/dashboards/network.json`
  - `observability/grafana/dashboards/sync.json`
- In Grafana:
  - Dashboards -> New -> Import
  - Upload the JSON file
  - Select your Prometheus datasource
  - If prompted for `DS_PROMETHEUS`, choose that same Prometheus datasource

## What this pilot dashboard validates

`node-overview.json`:
- Command throughput and rejection/error signals
- Command p95/p99 latency
- Task run rates by task/status
- Task p95/p99 latency
- Sync heartbeat freshness (`time() - node_sync_last_success_unix`)

`operations.json`:
- Completed throughput by command
- Rejected/expired pressure by command
- Reject/expire ratio by command
- Completed volume (1h) by command
- Command p95/p99 and average latency
- Throughput/latency by operation family (publish/get/finality/batch-get)

`triple-store.json`:
- Triple-store backend op rates, errors, avg/p95/p99 durations
- Concurrency pressure (permit wait + available/in-use permits)
- Knowledge collection insert rates/latency segmented by KC size and KA count buckets
- Query rates/errors/latency segmented by query type and visibility
- Query result footprint (average result bytes and triples)

`network.json`:
- Outbound request rates/errors/latency by protocol and outcome
- Inbound request decisions (scheduled vs rate-limited/controller-busy)
- Peer-event rates (identify, discovery, connection)
- 1h outbound request volume by protocol/outcome

`sync.json`:
- Fetch batch rates/latency by status (success/partial/failed)
- Peer request rates/latency/yield (valid KCs per request)
- Fetched vs failed KC rates and failure ratio
- Shard peer availability (shard members / identified / usable)

## Notes

- This is intentionally minimal and used to validate instrumentation and queries.
- After validation, expand with operation/network/storage dashboards.
