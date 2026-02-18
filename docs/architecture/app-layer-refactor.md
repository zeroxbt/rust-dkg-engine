# App-Layer Refactor Plan

## Goal
Replace the old mixed `services` layer with explicit architecture layers:
- `managers`: infrastructure adapters
- `node_state`: in-memory runtime coordination state
- `application`: use-cases/workflows
- adapters (`commands`, `periodic_tasks`, `controllers`) kept thin

Behavior policy: preserve behavior first, optimize later.

## Decisions Locked
- Rollout: incremental slices
- Naming: `application` + `node_state`
- Compatibility: preserve runtime behavior during migration
- Canonical plan path: `docs/architecture/app-layer-refactor.md`

## Status (Updated)

### Completed in this checkpoint
- Phase 1: module scaffolding created
  - Added `src/application/*`
  - Added `src/node_state/*`
- Phase 2: node state moved out of services
  - `PeerService` moved into `node_state::PeerRegistry` (wrapper removed)
  - `ResponseChannelsSet` moved into `node_state` and flattened into explicit per-protocol fields on `NodeState`
  - Legacy `src/state/*` internals (`PeerRegistry`, `ResponseChannels`) collapsed into `src/node_state/*`
  - Peer event loop ownership moved to `runtime`; `PeerRegistry` only applies events/state transitions
  - Runtime startup/deps rewired to `node_state`
- Phase 3: application use-case extraction
  - `GetFetchService` -> `GetAssertionUseCase`
  - `OperationStatusService` -> `OperationTracking`
  - `AssertionValidationService` -> `AssertionValidation`
  - `TripleStoreService` -> `TripleStoreAssertions`
  - Added explicit app wiring in `src/bootstrap/application.rs`
- Phase 4: shared GET/proving network-fetch logic
  - Added shared flow at `src/application/get_assertion/network_fetch.rs`
  - Proving now reuses shared network-fetch path
- Phase 5: token-range policy introduced
  - Added `TokenRangeResolutionPolicy::{Strict, CompatibleSingleTokenFallback}`
  - GET path uses compatibility fallback
  - Proving path uses strict mode
- Phase 7 (major part): legacy `services` module removed
  - Deleted `src/services/*`
  - Imports and wiring moved to `application` + `node_state`
- Compile validation
  - `cargo check --all-targets` passes
- Targeted unit tests added
  - `node_state::peer_registry` state + peer-event application tests
  - `node_state::initialize` response-channel initialization test
  - `application::get_assertion::network_fetch` response-validation tests
- Architecture guardrails deferred
  - We decided to remove the initial CI architecture guard for now.
  - Rule enforcement remains documentation/process-based until reintroduced.

### In progress / remaining
- Phase 6: adapter thinning
  - Naming and layering are improved, but some handlers/tasks can still be reduced further to pure mapping/orchestration.
- Phase 8: final naming cleanup
  - There are still some internal variable names/comments that can be normalized for clarity.
- Phase 9: test hardening
  - Remaining unit/regression tests:
    - strict vs compatibility token range behavior
    - proving/get shared fetch parity
    - additional `application/get_assertion` integration-path tests
- Phase 10: architecture guardrails
  - Optional: reintroduce CI guard checks if architectural drift becomes a recurring issue
  - Add contributor guidance/checklist entries

## Current Architecture Snapshot

### `src/application`
- `assertion_validation.rs`
- `triple_store_assertions.rs`
- `operation_tracking.rs`
- `get_assertion/`
  - `mod.rs`
  - `config.rs`
  - `network_fetch.rs`

### `src/node_state`
- `peer_registry.rs`
- `response_channels.rs`
- `mod.rs`

### Bootstrap wiring
- `src/bootstrap/application.rs` constructs application use-cases explicitly.
- `src/bootstrap/{commands,periodic,controllers,core}.rs` inject `application` + `node_state` directly.

## Next Implementation Slice (after this commit)
1. Add token-range policy tests (strict vs compatibility behavior).
2. Add proving/get parity tests for shared fetch validation flow.
3. Refine adapter thinness in heavy handlers/tasks (follow-up cleanup pass).
4. Optional: add/expand architecture guard rules when team/process needs it.
5. Update contributor docs/checklist.

## Risks and Mitigations
- Risk: subtle behavior drift while consolidating fetch flow.
  - Mitigation: add parity tests around GET and proving paths before further optimization.
- Risk: architecture drift over time.
  - Mitigation: CI guardrails + documentation + PR checklist.

## Checkpoint Verification
- Latest check before commit:
  - `cargo check --all-targets` ✅
