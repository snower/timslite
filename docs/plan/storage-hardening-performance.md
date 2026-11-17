# Storage Hardening and Performance Implementation Plan

**Goal:** Correct the reviewed concurrency, durability, capacity and corruption-handling defects, then remove evidenced read-path overhead without changing public APIs or disk formats.

**Architecture:** Preserve the dataset mutex as the consistency boundary. Queue claims use unlocked dataset reads followed by locked revalidation. Flush work is acknowledged only on successful synchronization. Decode validates boundaries and indexed timestamps. Internal cursors and borrowed raw payload slices remove unnecessary intermediate allocations.

**Constraints:** No commits before user review. No new dependencies, public API break, disk-format migration, or automatic truncation of inconsistent pending state. All filesystem tests execute single-threaded and separate test processes must not overlap.

## Approaches

Selected: incremental compatible changes to existing modules, with regression tests and narrow internal helpers. Rejected: wholesale queue/cache redesign or borrowed public result types, because they expand compatibility and lifetime risk without being necessary to remove the identified overhead.

## Review focus and evidence

- Concurrent same-group claims must return each fresh record to only one consumer; deterministic synchronization tests where practical.
- Queue poll/open with dataset flush must not acquire locks in reverse order; lock-path inspection plus bounded concurrency regression.
- Flush failure must retain failed/unattempted work for retry; test-only controllable failure where needed.
- Corrupt offsets/lengths/timestamps must return errors rather than panic or wrong data; synthetic corruption tests across full/length/hot-cache reads.
- Default geometry must accept a first 700 KiB incompressible record without caller retry; fresh and rollover regression tests.

## Task 1: Queue and Journal claims

Files: `src/queue/mod.rs`, `src/journal/queue.rs`; related tests within those files.
- [x] Add regressions for duplicate claims and lock-sensitive concurrency.
- [x] Remove QueueInner/state guards before dataset calls; revalidate claims before returning.
- [x] Replace fallback full-range materialization with bounded seek; re-resolve candidate timestamp visibility after unlocked seek.
- [x] Check focused queue/journal tests and report exact commands/status.

## Task 2: Segment capacity and safe decoding

Files: `src/segment/mod.rs`, `src/segment/data.rs`, `src/compress.rs`; related unit tests. Corrupt compressed blocks use bounded output decoding as well as exact length validation.
- [x] Add failing tests for first-write/rollover large records and corrupt block/record boundaries.
- [x] Loop expansion until sufficient or maximum reached; propagate non-capacity I/O errors.
- [x] Centralize checked read bounds and indexed timestamp validation; preserve compressed-cache semantics.
- [x] Parse raw payload directly while locked instead of copying whole blocks.
- [x] Bound decompression output and zstd history window before reading; verify corrupt/compatible frames.
- [x] Run focused segment/read tests and report commands/status.

## Task 3: Cache hot paths

Files: `src/cache.rs`; related unit tests.
- [x] Preserve invalidation, capacity accounting and immutable shared-block semantics in regression tests.
- [x] Use shared-lock hits with atomic access metadata; account capacity incrementally.
- [x] Replace full eviction sorting with a heapified candidate set; preserve tick/sequence order and verify against sorted oracle.
- [x] Run cache tests (28 passing); benchmark contention separately from correctness tests.

## Task 4: Dataset, flush and query integration

Files: `src/dataset.rs`, `src/bg/mod.rs`, `src/query/`, `src/index/` only if required.
- [x] Add retry regression for flush failures, retain failed/unattempted targets in a single shared recovery path.
- [x] Integrate queue seek helper and validate index timestamp, including hot-cache length shortcut.
- [x] Combine iterator index selection and data read under one dataset lock per result.
- [x] Remove full intermediate index materialization from query/query_length with internal cursor traversal under the existing dataset lock; preserve Vec outputs and per-record length semantics.
- [x] Add focused regression tests for filler/deleted/retention/negative/i64-boundary timestamps.

## Task 5: Documentation, benchmarks and gate

Files: `design.md`, `plan.md`, corresponding `docs/design/`, `benchmarks/`.
- [x] Document actual lock, claim, flush retry, decoding and cache invariants.
- [x] Add repeatable focused benchmarks for queue gaps/backlog, raw tail reads, cache hits and query iteration without hard timing assertions in unit tests.
- [x] Reconcile all writers and inspect changes; final single-thread suite: 969 passed, 0 failed, 2 ignored; fmt/clippy clean.
- [x] Run independent oracle gate and repair blockers; measured evidence and limitations: [review](../review/storage-hardening-performance.md).
- [x] Leave changes uncommitted for user review.
