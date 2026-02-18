# App-Layer Refactor Plan

## Goal
Replace the old mixed `services` layer with explicit architecture layers:
- `managers`: infrastructure adapters
- `runtime_state`: in-memory runtime coordination state
- `application`: use-cases/workflows
- adapters (`commands`, `periodic_tasks`, `controllers`) kept thin

Behavior policy: preserve behavior first, optimize later.

## Decisions Locked
- Rollout: incremental slices
- Naming: `application` + `runtime_state`
- Compatibility: preserve runtime behavior during migration
- Canonical plan path: `docs/architecture/app-layer-refactor.md`

## Status (Updated)

### Completed in this checkpoint
- Phase 1: module scaffolding created
  - Added `src/application/*`
  - Added `src/runtime_state/*`
- Phase 2: runtime state moved out of services
  - `PeerService` moved/renamed to `PeerDirectory`
  - `ResponseChannelsSet` moved/renamed to `ProtocolResponseChannels`
  - Runtime startup/deps rewired to `runtime_state`
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
  - Imports and wiring moved to `application` + `runtime_state`
- Compile validation
  - `cargo check --all-targets` passes

### In progress / remaining
- Phase 6: adapter thinning
  - Naming and layering are improved, but some handlers/tasks can still be reduced further to pure mapping/orchestration.
- Phase 8: final naming cleanup
  - There are still some internal variable names/comments that can be normalized for clarity.
- Phase 9: test hardening
  - Need targeted unit/regression tests for:
    - `application/get_assertion`
    - `runtime_state/peer_directory`
    - `runtime_state/protocol_response_channels`
    - strict vs compatibility token range behavior
    - proving/get shared fetch parity
- Phase 10: architecture guardrails
  - Add CI checks preventing reintroduction of `services` and adapter->application rule violations
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

### `src/runtime_state`
- `peer_directory.rs`
- `protocol_response_channels.rs`
- `mod.rs`

### Bootstrap wiring
- `src/bootstrap/application.rs` constructs application use-cases explicitly.
- `src/bootstrap/{commands,periodic,controllers,core}.rs` inject `application` + `runtime_state` directly.

## Next Implementation Slice (after this commit)
1. Add targeted tests for `application/get_assertion` and token policy behavior.
2. Add targeted tests for `runtime_state` (`peer_directory`, `protocol_response_channels`).
3. Refine adapter thinness in heavy handlers/tasks (follow-up cleanup pass).
4. Add CI guard script + workflow checks for architectural boundaries.
5. Update contributor docs/checklist.

## Risks and Mitigations
- Risk: subtle behavior drift while consolidating fetch flow.
  - Mitigation: add parity tests around GET and proving paths before further optimization.
- Risk: architecture drift over time.
  - Mitigation: CI guardrails + documentation + PR checklist.

## Checkpoint Verification
- Latest check before commit:
  - `cargo check --all-targets` ✅
