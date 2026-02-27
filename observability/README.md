# Observability Quick Start (Pilot)

This directory contains pilot dashboards for validating node telemetry end-to-end.

Telemetry signals used here:

- Metrics: aggregated numeric time series (for dashboards/alerts)

Quick intuition:

- A metric answers: "how many publishes failed in the last 10 minutes?"

## Prerequisites

1. Node config enables telemetry signals you need:

```toml
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
  - `observability/grafana/dashboards/sync-v2.json`
  - `observability/grafana/dashboards/memory.json`
  - `observability/grafana/dashboards/internals.json`
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
- Network action queue backpressure (enqueue wait, depth, enqueue/dequeue rate)
- Pending outbound request gauges and response-send success/failure rates

`sync.json`:
- Fetch batch rates/latency by status (success/partial/failed)
- Peer request rates/latency/yield (valid KCs per request)
- Fetched vs failed KC rates and failure ratio
- Shard peer availability (shard members / identified / usable)

`sync-v2.json`:
- KCs with synced metadata totals from SQL (`node_sync_metadata_kcs_total` and source-filtered `node_sync_metadata_backfill_kcs_total`)
- KCs estimated fully synced (`clamp_min(node_sync_metadata_kcs_total - node_sync_queue_total, 0)`)
- Sync queue snapshot: due (`node_sync_queue_due`), retrying (`node_sync_queue_retrying`), failed estimate (`clamp_min(node_sync_queue_total - node_sync_queue_due - node_sync_queue_retrying, 0)`)
- Fetch batch duration p50/p95/p99 for successful batches (`node_sync_fetch_batch_duration_seconds{status="success",quantile=...}`)
- KA/KC count per fetch batch averages (`sum(rate(node_sync_fetch_batch_assets_sum[5m])) / sum(rate(node_sync_fetch_batch_assets_count[5m]))` and `sum(rate(node_sync_fetch_batch_kcs_sum[5m])) / sum(rate(node_sync_fetch_batch_kcs_count[5m]))`)
- Insert batch duration p50/p95/p99 for successful batches (`node_sync_insert_batch_duration_seconds{status="success",quantile=...}`)
- KC/KA count per insert batch averages (`sum(rate(node_sync_insert_batch_kcs_sum[5m])) / sum(rate(node_sync_insert_batch_kcs_count[5m]))` and `sum(rate(node_sync_insert_batch_assets_sum[5m])) / sum(rate(node_sync_insert_batch_assets_count[5m]))`)

`memory.json`:
- Process-level memory (RSS/virtual) and file descriptor usage
- Process file-descriptor composition snapshots by type (socket/pipe/anon_inode/file/other)
- Container memory usage and usage percentage vs configured limits
- Host memory pressure (used %, available bytes, swap used)
- Internal backlog and pressure signals (sync queue, network queue depth, pending requests, peer registry size)

`internals.json`:
- Sync-cycle internals: cycle outcomes, per-cycle KC volume, RSS start/end/delta
- Sync pipeline pressure: filter->fetch and fetch->insert channel depths
- Fetch payload sizing: batch-level and per-KC payload bytes
- Network channel internals: action channel fill/depth, pending requests, response-channel waits/outcomes
- Peer registry internals: request outcomes/latency plus population/capability/backoff signals
- Process FD internals: total snapshots and type breakdown for FD leak detection

For `memory.json` to be fully populated, Prometheus must scrape additional exporters besides the node app endpoint:
- `node-exporter` for `node_memory_*` host metrics
- `cAdvisor` (or kubelet cAdvisor metrics) for `container_memory_*` metrics
- optional process exporter for process-level metrics when `process_*` is not emitted by the app

## Notes

- This is intentionally minimal and used to validate instrumentation and queries.
- After validation, expand with operation/network/storage dashboards.
