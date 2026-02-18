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
- Targeted unit tests added
  - `runtime_state::peer_directory` event loop/state tests
  - `runtime_state::protocol_response_channels` initialization test
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

### `src/runtime_state`
- `peer_directory.rs`
- `protocol_response_channels.rs`
- `mod.rs`

### Bootstrap wiring
- `src/bootstrap/application.rs` constructs application use-cases explicitly.
- `src/bootstrap/{commands,periodic,controllers,core}.rs` inject `application` + `runtime_state` directly.

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
