# Compact Index Implementation Plan

> **For agentic workers:** Use the user-selected Deepwork workflow; focused tests precede implementation and disjoint lanes converge before broad validation.

**Goal:** Ship the incompatible compact index format and immutable index_data_size creation configuration without changing record-operation APIs.
**Architecture:** Keep absolute in-memory entries and journal pointers. Encode relative u48 offsets only at index-segment boundaries, with runtime aligned stride propagated through all lifecycle operations.
**Tech Stack:** Rust, mmap, TLV metadata, C/PyO3/Node-API/UniFFI wrappers.
**Spec:** docs/superpowers/specs/2026-11-17-compact-index-design.md

## Global Constraints
- Core 16B; entry_stride=align_up(16+N,8); N:u32 default 0.
- No old-format compatibility, new record-operation APIs, or automatic commits.
- Immutable base from triggering real block start, including filler creation.
- Real delta <=0xFFFFFFFFFFFE; explicit errors; journal remains 18B absolute format.
- All filesystem tests run with --test-threads=1.

## Review Focus
1. First real entry and filler-only segment must never capture post-write cursor as base.
2. Long-lived historical segment update must reject out-of-range delta without altering old entry.
3. Core overwrite must preserve nonzero custom bytes; delete clears them; padding is checked.
4. Runtime stride must survive lazy-close/reopen and keep continuous grid stable.
5. Malformed/missing sizing metadata and zero-capacity configurations must be rejected.

## Task 1: Config, metadata and header
Files: src/config.rs, src/meta.rs, src/header.rs.
Interfaces: DataSetConfig::index_data_size()->u32; builder.index_data_size(u32); DataSetMeta.index_data_size:u32; IndexFileMetadata.base_block_offset:u64 and index_data_size:u32. Add checked sizing helper in config, reusable by index code. Index metadata creation accepts base and N; read requires fields and new index version.
Agreed exact interfaces: `pub(crate) fn index_entry_stride(index_data_size: u32) -> Result<usize>` in config; append `index_data_size:u32` to DataSetMeta::new; append `base_block_offset:u64,index_data_size:u32` to IndexFileMetadata::create_default. Dataset metadata uses exact bumped version; new TLVs reject duplicates/missing/wrong length.
- [x] Add tests for default/explicit N, required TLVs, 128B header fit, mismatched/corrupt formats and sizes unable to hold one stride.
- [x] Run focused config/meta/header tests and record expected failure.
- [x] Implement persistence, validation and sizing helper; update scoped constructor tests.
- [x] Run focused tests; report exact commands/status and interface signatures.

## Task 2: Index and dataset integration
Files: src/index/**, src/dataset.rs, src/store.rs, root tests/examples as needed.
Interfaces: consume Task 1 fields; preserve public record methods and memory IndexEntry absolute offsets. TimeIndex sizing receives N; segment creation receives triggering block base; segment codecs receive base and stride context.
- [x] Add byte-layout tests for delta zero/max/overflow/underflow, paired sentinels, custom region and padding.
- [x] Add tests for N=0/3/16 (stride 16/24/32), same-block rollover, sparse gap and backfill, OOO/correction, delete/reinsert and reopen.
- [x] Run focused tests to establish failure; implement codec, base propagation, runtime stride and lifecycle validation.
- [x] Make load count/header helpers return Result; reject discovered corrupt segments rather than defaulting to empty. Preflight encoding before filler mutation/archive-range publication. Explicit filler helper creation-base arguments propagate from the real entry.
- [x] Assert failed overwrite leaves original slot unchanged; exercise nonzero reserved bytes preservation without adding APIs.
- [x] Run focused index/dataset tests; report evidence and any stale checks.

## Task 3: Wrapper configuration
Files: wrapper/** excluding unrelated release/version files.
Interfaces: core builder.index_data_size(u32), getter u32; expose equivalent creation-only default-zero configuration and getters/metadata where existing patterns require them. Existing read/write methods unchanged.
- [x] Add creation/config roundtrip tests using N=16 and a nonmultiple-of-eight N.
- [x] Implement C struct/header parity, Python, Node.js, Java and .NET bridge/public configuration; regenerate bindings using project tools where available.
- [x] Run C cargo check/test serially; run wrapper cargo checks and supported language tests, recording unavailable tooling precisely.

## Task 4: Reconciliation and gate
Owner: orchestrator; no competing writers.
- [x] Inspect decisive source artifacts and all writer diffs; synchronize design.md, docs/design and phase checklist with actual behavior.
- [x] Run cargo test -- --test-threads=1, cargo fmt -- --check, cargo clippy -- -D warnings on reconciled state; reuse still-valid wrapper evidence.
- [x] Obtain oracle gate for data correctness and contract completeness; repair blockers in a bounded lane and revalidate affected checks.
- [x] Update checklist/progress with results and known limitations; leave changes uncommitted for user review.
