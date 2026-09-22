# Wall Clock Retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make retention use an explicit wall-clock unit scale while preserving legacy datasets and preventing expired data from reappearing after clock rollback or restart.

**Architecture:** Add `timestamp_units_per_seconds: u64` to the immutable dataset configuration and persisted metadata. A value of `0` keeps the legacy latest-written-timestamp retention algorithm. A nonzero value derives the expiration threshold from current Unix seconds scaled into dataset timestamp units with checked `i128` arithmetic, then advances a persistent monotonic retention floor before any index or data segment is physically reclaimed. The core `write_now` and `append_now` methods use the same scale so their records and wall-clock retention share one timestamp domain.

**Tech Stack:** Rust 2021, mmap dataset state file, Cargo tests, C ABI crate, PyO3, Node-API, UniFFI with Java and .NET bindings.

**Spec:** `design.md`, `docs/design/data-model.md`, `docs/design/dataset-operations.md`, `docs/design/background-and-cache.md`, and `docs/design/store-and-ffi.md`

## Global Constraints

- Implement documentation updates before implementation changes. Update `design.md`, the relevant files in `docs/design/`, `plan.md`, and the related `docs/plan/` checklist before changing Rust or wrapper code.
- `timestamp_units_per_seconds` is an immutable `u64` dataset configuration value persisted in `DataSetMeta`.
- `timestamp_units_per_seconds == 0` means legacy retention. Its threshold remains based on `latest_written_timestamp - retention_window` and must not read wall-clock time.
- When `timestamp_units_per_seconds != 0`, obtain Unix seconds, calculate `now_units = unix_seconds * timestamp_units_per_seconds` through checked `i128` multiplication, convert only if the result fits `i64`, and return `TmslError::InvalidData` on overflow or a clock before the Unix epoch.
- For wall-clock retention, calculate `candidate_floor = now_units.saturating_sub(retention_window as i64)`. The effective floor is monotonic: `max(persisted_retention_floor, candidate_floor)`.
- Persist a newly advanced wall-clock retention floor and flush its mmap state before reclaiming any index or data segment. A later clock rollback, a process restart, or a failed reclaim must never lower the effective floor.
- Retention remains disabled when `retention_window == 0`. Do not reclaim or advance a floor in that case.
- `write_now` and `append_now` must use the same scaled wall-clock timestamp when `timestamp_units_per_seconds != 0`; they retain Unix seconds when it is `0`.
- Keep public explicit timestamp APIs unchanged. Explicit `write`, `append`, read, query, correction, delete, and queue timestamps remain caller supplied dataset timestamps.
- Propagate the new dataset creation option through C FFI, Python, Node.js, Java, and .NET APIs, generated or handwritten declarations, documentation, and tests.
- Do not alter existing callers' behavior when the option is omitted. Its default is `0`.
- Follow the repository policy: do not commit until review approval. Do not create a new worktree because the approved work is on the current branch.

---

## File Map

| File | Change |
| --- | --- |
| `design.md` | Add the approved wall-clock retention contract to the top-level design index. |
| `docs/design/data-model.md` | Define the timestamp-unit scale, legacy zero behavior, and metadata persistence. |
| `docs/design/dataset-operations.md` | Define threshold calculation, monotonic floor behavior, `write_now`, and `append_now` semantics. |
| `docs/design/background-and-cache.md` | Define floor durability ordering before physical reclaim and restart or rollback behavior. |
| `docs/design/store-and-ffi.md` | Document the new dataset-create option across public wrappers. |
| `plan.md`, `docs/plan/...` | Add and check off the scoped implementation checklist according to the repository's active plan layout. |
| `src/config.rs` | Add configuration storage, builder setter, default, validation, and store-derived configuration propagation. |
| `src/meta.rs` | Encode and decode the immutable `timestamp_units_per_seconds` metadata field with backward-compatible default `0`. |
| `src/dataset_state.rs` | Extend the state snapshot and mmap encoding with the persisted wall-clock retention floor, keeping old state-file compatibility intentional and tested. |
| `src/dataset.rs` | Centralize scaled wall-clock conversion, choose legacy or wall-clock thresholds, advance and persist the floor, reclaim only after durability, and scale now-based writes. |
| `src/store.rs`, `src/bg/...` | Update only if their create, inspect, or scheduled reclaim paths require the new field or test hooks. |
| `src/lib.rs` | Export any public configuration additions required by the existing API pattern. |
| `wrapper/cffi/...` | Add the create-dataset option to the C ABI types, conversion code, public header, tests, and C-facing docs. |
| `wrapper/python/...` | Expose the option in PyO3 create options and add Python tests. |
| `wrapper/nodejs/...` | Expose the option in Rust binding conversion, TypeScript declarations, JavaScript docs, and Node tests. |
| `wrapper/java/...` | Add the UniFFI config field and Java builder mapping, then test native and Java-facing propagation. |
| `wrapper/dotnet/...` | Add `ulong? TimestampUnitsPerSeconds`, map it in `ToNative`, and add .NET tests. |

### Task 1: Document the Approved Contract First

**Files:**
- Modify: `design.md`
- Modify: `docs/design/data-model.md`
- Modify: `docs/design/dataset-operations.md`
- Modify: `docs/design/background-and-cache.md`
- Modify: `docs/design/store-and-ffi.md`
- Modify: `plan.md`
- Modify: the active retention-related checklist under `docs/plan/`

**Interfaces:**
- Consumes: the approved constraints in this plan.
- Produces: an implementation contract that names `timestamp_units_per_seconds: u64`, its default, persistence model, threshold rules, floor ordering, now-method behavior, and wrapper option.

- [ ] **Step 1: Update the top-level and data-model documentation**

State that dataset timestamps remain `i64`, while `timestamp_units_per_seconds: u64` maps Unix seconds into a dataset timestamp domain. State exactly that zero preserves legacy retention and nonzero is immutable metadata with a default of zero for old metadata files.

- [ ] **Step 2: Update dataset-operation documentation**

Document these rules verbatim in behavior-oriented form:

```text
retention_window == 0: retention disabled.
timestamp_units_per_seconds == 0: threshold is latest_written_timestamp - retention_window.
timestamp_units_per_seconds != 0: threshold candidate is
  checked_i64(checked_i128(unix_seconds) * checked_i128(units_per_second))
  - retention_window.
effective floor is max(persisted_retention_floor, candidate_floor).
write_now and append_now write the scaled current timestamp for nonzero units.
```

Describe the overflow error path and that explicit timestamp methods are not rescaled.

- [ ] **Step 3: Update background and persistence documentation**

Specify the durable sequence: calculate candidate, advance the state-file floor if needed, synchronously flush the state mmap, then reclaim index segments and data segments. Specify that reclaim failures leave the advanced floor durable, so retries can only use that floor or a newer one.

- [ ] **Step 4: Update wrapper and delivery documentation**

List the option in C, Python, Node.js, Java, and .NET dataset-create APIs. Update `plan.md` and the active `docs/plan/` checklist before implementation starts, with explicit entries for metadata compatibility, state persistence, core behavior, and each wrapper.

- [ ] **Step 5: Review documentation consistency**

Run these read-only checks after the Task 1 documentation edits:

```bash
grep -nE 'latest[_ -]written[_ -]timestamp.*retention|retention.*latest[_ -]written[_ -]timestamp' \
  design.md \
  docs/design/data-model.md \
  docs/design/dataset-operations.md \
  docs/design/background-and-cache.md \
  docs/design/store-and-ffi.md \
  plan.md \
  docs/plan/*

grep -nE 'timestamp_units_per_seconds == 0|timestamp_units_per_seconds != 0|retention_window == 0|legacy retention|wall-clock retention' \
  design.md \
  docs/design/data-model.md \
  docs/design/dataset-operations.md \
  docs/design/background-and-cache.md \
  docs/design/store-and-ffi.md \
  plan.md \
  docs/plan/*
```

Expected results: the first command may report the legacy branch, but every match must explicitly limit latest-written-timestamp thresholding to `timestamp_units_per_seconds == 0`; it must report no unconditional latest-write-relative retention statement. The second command must report all three modes: `retention_window == 0` disables retention independently, `timestamp_units_per_seconds == 0` selects legacy latest-written-timestamp thresholding, and `timestamp_units_per_seconds != 0` selects wall-clock thresholding from scaled Unix seconds.

### Task 2: Add Configuration and Metadata Persistence

**Files:**
- Modify: `src/config.rs`
- Modify: `src/meta.rs`
- Modify: `src/lib.rs` if this is required for the established public export pattern
- Test: unit tests in `src/config.rs` and `src/meta.rs`

**Interfaces:**
- Consumes: documented field `DataSetConfig::timestamp_units_per_seconds() -> u64`.
- Produces: a persisted `DataSetMeta` value and a builder setter equivalent to `DataSetConfigBuilder::timestamp_units_per_seconds(u64)`.

- [ ] **Step 1: Write failing configuration tests**

Add tests proving the builder preserves a nonzero value and that omitted configuration returns `0`:

```rust
#[test]
fn dataset_config_defaults_timestamp_units_per_seconds_to_zero() {
    assert_eq!(DataSetConfig::builder().build().timestamp_units_per_seconds(), 0);
}

#[test]
fn dataset_config_preserves_timestamp_units_per_seconds() {
    let config = DataSetConfig::builder()
        .timestamp_units_per_seconds(1_000)
        .build();
    assert_eq!(config.timestamp_units_per_seconds(), 1_000);
}
```

- [ ] **Step 2: Run the focused configuration test module**

Run: `cargo test config::tests -- --test-threads=1`

Expected: the new tests fail because the builder and accessor do not exist.

- [ ] **Step 3: Implement configuration storage and validation**

Add the private field, getter, builder setter, default `0`, and propagation from any store-derived dataset configuration. Do not reject zero. Do not change `retention_window` units.

- [ ] **Step 4: Write failing metadata compatibility tests**

Add one round-trip test for a nonzero field and one fixture or manually encoded old metadata test that omits the field and opens as zero. Keep existing on-disk TLV validation rules for field lengths and integer endian order.

- [ ] **Step 5: Implement metadata encoding and backward-compatible decoding**

Assign a new metadata TLV tag without renumbering old tags. Encode as little-endian `u64`. Decode omission as `0`, and reject a malformed field length through the existing metadata error pattern.

- [ ] **Step 6: Run focused configuration and metadata tests**

Run: `cargo test config::tests meta::tests -- --test-threads=1`

Expected: PASS.

### Task 3: Persist a Monotonic Retention Floor in Dataset State

**Files:**
- Modify: `src/dataset_state.rs`
- Modify: `src/dataset.rs`
- Test: unit tests in `src/dataset_state.rs`; integration tests in the existing dataset test module or `tests/` file that owns retention tests

**Interfaces:**
- Consumes: wall-clock candidate floors as `i64` and `DatasetStateFile` mmap durability.
- Produces: `DatasetStateSnapshot.retention_floor: i64` or an explicitly documented optional representation, plus a method that only advances and flushes it.

- [ ] **Step 1: Write failing state-file tests**

Cover fresh state, state reopen, and monotonic updates:

```rust
#[test]
fn retention_floor_survives_state_reopen() { /* set 100, close, reopen, assert 100 */ }

#[test]
fn retention_floor_never_moves_backward() { /* set 100, request 99, assert 100 */ }
```

Also test the exact state-file version or reserved-byte migration policy selected by the implementation. If changing the file version, test old-version handling. If consuming reserved bytes without changing version, test that a legacy all-zero state maps to the documented no-floor sentinel.

- [ ] **Step 2: Run the state-file tests to prove failure**

Run: `cargo test dataset_state::tests -- --test-threads=1`

Expected: FAIL until the snapshot field and persistence method exist.

- [ ] **Step 3: Implement state floor encoding and synchronous persistence**

Use a signed little-endian `i64` field and a sentinel that cannot collide with a valid usable floor, preferably `TIMESTAMP_MIN_SENTINEL` if it already represents no lower bound. Add a method with behavior equivalent to:

```rust
fn advance_retention_floor(&mut self, candidate: i64) -> Result<i64> {
    let effective = self.snapshot.retention_floor.max(candidate);
    if effective != self.snapshot.retention_floor {
        self.snapshot.retention_floor = effective;
        self.flush_snapshot()?;
    }
    Ok(effective)
}
```

Keep read-only state non-mutating. The caller must receive an error before reclaim in read-only mode.

- [ ] **Step 4: Run state-file tests**

Run: `cargo test dataset_state::tests -- --test-threads=1`

Expected: PASS.

### Task 4: Implement Threshold Selection, Durable Reclaim Ordering, and Scaled Now Methods

**Files:**
- Modify: `src/dataset.rs`
- Modify: `src/store.rs` and `src/bg/...` only if their existing calls need signature or scheduling changes
- Test: existing retention tests in `src/dataset.rs` or their current integration-test file

**Interfaces:**
- Consumes: `DataSetConfig::timestamp_units_per_seconds()`, metadata value, and `DatasetStateFile::advance_retention_floor`.
- Produces: one internal current-time conversion helper, one retention-floor selection path, and unchanged public signatures for `DataSet::write_now` and `DataSet::append_now`.

- [ ] **Step 1: Write failing unit tests for scaled time conversion**

Inject or isolate a helper that accepts a supplied Unix second value so tests do not depend on wall-clock timing. Cover:

```rust
assert_eq!(scale_unix_seconds(123, 0)?, 123);
assert_eq!(scale_unix_seconds(123, 1_000)?, 123_000);
assert!(scale_unix_seconds(i64::MAX, u64::MAX).is_err());
```

Assert overflow produces `TmslError::InvalidData`, not wrapping or saturation. Retain the existing pre-epoch system-time error.

- [ ] **Step 2: Write failing retention behavior tests**

Add deterministic tests that prove all of the following:

1. A dataset with `timestamp_units_per_seconds == 0` retains legacy latest-written timestamp semantics even if the clock source would be far ahead.
2. A nonzero scale uses a wall-clock candidate floor, not the newest record timestamp.
3. `retention_window == 0` returns no threshold and leaves the persisted floor unchanged.
4. A candidate lower than the saved floor does not make an earlier record readable, writable through correction, deletable, or eligible for a lower reclaim threshold.
5. Reopen restores the saved floor and applies it even when the supplied current Unix second is earlier.
6. Reclaim writes and flushes the advanced floor before deleting segments. Use a controlled failure or spy seam for reclaim, then reopen and assert the floor persisted.
7. `write_now` and `append_now` produce timestamps in the configured scaled domain. For append, open or create the current timestamp record as required by the existing append contract.

- [ ] **Step 3: Run the new core tests to prove failure**

Run: `cargo test retention -- --test-threads=1`

Expected: FAIL because threshold selection uses only `latest_written_timestamp`, no floor persists, and now methods use raw Unix seconds.

- [ ] **Step 4: Implement one checked wall-clock conversion helper**

Keep `current_unix_seconds()` as the source of validated seconds. Add an internal helper that accepts `unix_seconds: i64` and `units_per_second: u64`, casts both operands to `i128`, uses `checked_mul`, verifies the product fits `i64`, and returns `InvalidData` on any failure. Route `write_now` and `append_now` through this helper using the opened dataset configuration.

- [ ] **Step 5: Split legacy and wall-clock threshold logic explicitly**

Preserve the existing legacy branch exactly for scale zero. In the nonzero branch, derive the candidate from the checked scaled current time and retention window, then call the dataset-state monotonic advance method before returning the effective floor. Do not write the floor from `read`, `query`, `delete`, correction, or append paths. Only the reclaim path advances it.

- [ ] **Step 6: Enforce persistence before physical reclaim**

In `DataSetInner::reclaim_expired_segments`, first calculate the effective threshold. For wall-clock retention, persist the advanced floor synchronously before `idle_close_segments`, index-segment reclaim, or data-segment reclaim. Then use that same returned floor for all reclaim calls. Preserve existing runtime locks and read-only rejection.

- [ ] **Step 7: Run focused core tests**

Run: `cargo test retention -- --test-threads=1`

Expected: PASS.

### Task 5: Propagate the Option Through Every Wrapper

**Files:**
- Modify: `wrapper/cffi/src/...`, `wrapper/cffi/include/...`, and existing C ABI tests
- Modify: `wrapper/python/src/...` and `wrapper/python/tests/...`
- Modify: `wrapper/nodejs/src/...`, `wrapper/nodejs/index.d.ts`, and existing Node tests
- Modify: `wrapper/java/native/src/bridge.rs`, Java configuration builder classes, generated-interface inputs, and Java tests
- Modify: `wrapper/dotnet/src/Timslite/DatasetConfig.cs`, the UniFFI interface inputs, and .NET tests

**Interfaces:**
- Consumes: core builder setter `timestamp_units_per_seconds(u64)`.
- Produces: optional wrapper create options mapping absent values to Rust default `0` and present values to the exact `u64` value.

- [ ] **Step 1: Write failing C ABI propagation tests**

Create a dataset through the C ABI with `timestamp_units_per_seconds = 1_000`, inspect or reopen it through the ABI, and assert the persisted option is visible through the appropriate info route. Add a default-value test for callers that leave the field unset according to the ABI's established optional-field convention.

- [ ] **Step 2: Implement C ABI structure, conversion, header, and test updates**

Add the field without reordering existing ABI fields. Update the public C header and any corresponding documentation. Preserve ABI versioning conventions already used by this crate.

- [ ] **Step 3: Write and implement Python and Node propagation tests**

In Python and Node tests, create a dataset with `timestamp_units_per_seconds: 1_000`, reopen or inspect it, and assert it retains the value. Assert omitted options produce `0`. Update PyO3 option parsing, Node Rust conversion, and `wrapper/nodejs/index.d.ts` with `timestampUnitsPerSeconds?: number | bigint` following existing naming conventions.

- [ ] **Step 4: Write and implement Java and .NET propagation tests**

Add the optional unsigned field to the UniFFI config record and bridge mapping. In Java, add the builder field and test its native configuration conversion. In .NET, add `public ulong? TimestampUnitsPerSeconds { get; init; }` and pass it as `TimestampUnitsPerSeconds:` in `DatasetConfig.ToNative()`. Add tests that create datasets and verify the value survives the binding boundary.

- [ ] **Step 5: Run wrapper-focused tests**

Run:

```bash
cargo check --manifest-path wrapper/cffi/Cargo.toml --all-targets
cargo test --manifest-path wrapper/cffi/Cargo.toml -- --test-threads=1
```

Then run each wrapper's existing check and language-side test command documented by that wrapper, subject to installed local toolchains. Record unavailable toolchains as environment limitations, not skipped semantic coverage.

### Task 6: Full Verification and Documentation Reconciliation

**Files:**
- Modify only as needed to correct implementation, tests, or documentation found inconsistent during this task.

**Interfaces:**
- Consumes: completed core and wrapper changes.
- Produces: validated repository state with no untracked scope expansion.

- [ ] **Step 1: Run formatting and static checks**

Run:

```bash
cargo fmt -- --check
cargo clippy -- -D warnings
```

Expected: PASS.

- [ ] **Step 2: Run the main test suite serially**

Run:

```bash
cargo test -- --test-threads=1
```

Expected: PASS. The serial flag is required because filesystem tests share temporary paths.

- [ ] **Step 3: Run language-server diagnostics on every changed Rust file**

Run diagnostics for `src/config.rs`, `src/meta.rs`, `src/dataset_state.rs`, `src/dataset.rs`, and each changed Rust wrapper source file. Resolve all errors and warnings caused by this work.

- [ ] **Step 4: Reconcile docs with implementation**

Confirm the documented formula and actual code agree on: `u64` scale, zero legacy behavior, checked `i128` multiplication, zero retention disablement, monotonic persisted floor, persistence before reclaim, restart and rollback behavior, and scaled now methods. Confirm all five wrapper surfaces expose the field.

- [ ] **Step 5: Prepare review, do not commit**

Inspect the final diff for accidental API changes, field reordering, non-little-endian disk encoding, or unrelated modifications. Request review according to repository process. Do not commit unless review approval is explicitly received.

## Acceptance Matrix

| Requirement | Test evidence |
| --- | --- |
| `timestamp_units_per_seconds: u64` defaults to zero | config builder and metadata omission tests |
| Zero scale preserves legacy retention | core legacy threshold test |
| Nonzero scale uses checked `i128` Unix-second scaling | helper normal, overflow, and pre-epoch tests |
| Floor is monotonic and restart safe | dataset-state reopen and clock-rollback tests |
| Floor is durable before reclaim | injected reclaim-failure ordering test |
| `write_now` and `append_now` use the scale | deterministic injected-clock tests |
| Wrapper propagation is complete | C, Python, Node, Java, and .NET create-option tests |
| Existing behavior remains compatible | full serial main test suite and wrapper checks |
