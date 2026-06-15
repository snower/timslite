# Phase 34: Ordered Segment Registry

> Goal: merge data/index open and closed segment registries into BTreeMap-backed registries so hot read/update/delete paths can locate target segments by key instead of scanning every segment.
> Status: completed.

## 34.0 Design Documents

- [x] [Data segment](../design/data-segment.md)
- [x] [Time index](../design/time-index.md)

## 34.1 Scope

- [x] Merge separate open/closed data segment collections into one `BTreeMap<u64, DataSegmentEntry>`.
- [x] Route data reads and invalid-record updates by computed segment file offset.
- [x] Merge separate open/closed index segment collections into one `BTreeMap<i64, IndexSegmentEntry>`.
- [x] Route continuous index point lookup/update/delete by computed `segment_start`.
- [x] Route non-continuous index point lookup/update/delete through binary-selected candidate segments instead of opening every closed segment.
- [x] Limit range query closed-segment opens to ordered candidate ranges where practical.

## 34.2 Non-Goals

- [-] Do not merge open/closed state into a new persistent format.
- [-] Do not introduce a new on-disk segment directory layout.
- [-] Do not implement a full LRU open-segment manager in this phase.

## 34.3 Implementation Tasks

- [x] Add helper methods for BTreeMap-backed data segment lookup and state transitions.
- [x] Replace data read-path linear segment scans with exact segment offset lookup.
- [x] Add helper methods for BTreeMap-backed index segment lookup and state transitions.
- [x] Replace index closed-segment point scans with candidate lookup.
- [x] Preserve flush queue target semantics (`file_offset` / `start_timestamp`).
- [x] Keep idle-close and load-existing paths sorted after lifecycle transitions.

## 34.4 Tests

- [x] Data segment lazy-open keeps open/closed registries sorted after out-of-order historical reads.
- [x] Data segment `increment_invalid_record_count` locates closed segments by computed offset.
- [x] TimeIndex lazy-open/new/idle-close paths keep registries sorted.
- [x] Continuous index point lookup/update/delete opens only the computed segment.
- [x] Non-continuous index point lookup/update/delete works after idle-close without scanning unrelated closed segments.
- [x] Existing full suite remains green.

## 34.5 Verification

- [x] `cargo test segment::tests -- --test-threads=1`
- [x] `cargo test index::tests -- --test-threads=1`
- [x] `cargo test -- --test-threads=1`
- [x] `cargo fmt -- --check`
- [x] `cargo clippy --all-targets -- -D warnings`
