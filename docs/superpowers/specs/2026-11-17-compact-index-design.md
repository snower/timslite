# Compact Index Design

## Intent and scope
Reduce default index storage from 32B to 16B per slot and reserve immutable per-dataset custom index bytes without adding record read/write APIs. Existing dataset read/write/append/delete/query/queue/journal semantics remain unchanged. This is an incompatible storage change, not a migration.

## Approaches
Selected: 16B core containing i64 timestamp, u48 relative block offset and u16 in-block units, followed by N reserved bytes and zero padding to an 8B boundary. Absolute u48 would limit total dataset address space; retaining u64 with smaller padding would not achieve the selected 16B core. Arbitrary unaligned stride was rejected in favor of align_up(16+N,8).

## Format and invariants
- index_data_size is u32, default 0, set only at dataset creation. Persist as required dataset-meta and index-file immutable TLVs. Opening verifies equality. No record/index-data API additions.
- Index entry area remains at byte 128. Metadata/state must fit this reserved header.
- Memory IndexEntry remains absolute u64; only disk encoding is relative.
- Immutable base_block_offset is the triggering real entry's block start. Filler-only creation receives that same real block start, never a post-append cursor or a sentinel.
- Real delta is 0..=0xFFFFFFFFFFFE, encoded little-endian in 6 bytes. Negative delta, excessive delta and base-add overflow are errors.
- Disk filler/delete is delta=0xFFFFFFFFFFFF and units=0xFFFF; decode to existing memory sentinels before base arithmetic. Real entries cannot use either sentinel field.
- Entry stride is checked align_up(16+N,8). Initial and maximum index files must each fit header plus at least one entry. Continuous grids use maximum-file capacity.
- New entries/fillers initialize reserved custom bytes to zero. Location-only overwrite preserves custom bytes; delete clears them; reinsertion initializes them. Nonzero custom bytes are legal; alignment padding must be zero.
- Encode and validate before modifying mmap. Failed publication preserves old index and must not mark old data invalid, notify success or append a successful journal event. Orphan data payload is permitted by existing non-WAL semantics.
- Bump index format version and reject old index files. Require new dataset metadata. No compatibility or automatic upgrade paths.
- Dataset metadata also uses an exact new version. New base/sizing TLVs must occur exactly once with correct lengths. Discovered corrupt segments fail dataset open rather than being treated as empty; validate stride, file bounds, padding and dataset/header sizing equality during load.
- Header maximum file size must equal the stride-normalized dataset maximum used at creation. Continuous segments must lie on the maximum-capacity timestamp grid, and materialized slot i must contain timestamp segment_start+i; malformed grids/slots fail open.
- Preflight real-entry encoding before filler mutation, archived-range publication or index mmap writes. Filler helpers explicitly accept the triggering real block offset.
- JournalIndexInfo remains the independent 18B absolute-pointer encoding; no custom data replication contract is added.

## Boundaries
Core config/meta/header provides immutable sizing. TimeIndex and IndexSegment own base selection, relative codec and runtime stride. DataSet supplies existing absolute entries and keeps operation signatures unchanged. All language wrappers expose the creation configuration only. C header and implementation change together; ABI compatibility is not required.

## Verification
Byte-level codec tests establish layout/sentinel/bounds/padding; file tests establish stride/base persistence and corruption rejection. Dataset tests establish same-block rollover, sparse filler/backfill, historical overwrite, correction fallback, append/delete, lazy-open and reopen behavior. Root tests run serially. Final checks include root fmt/clippy, C check/test and available wrapper checks/language tests. No performance improvement claim without benchmarks.

## Non-goals and decisions
No new custom-data APIs, compaction/rebasing, overflow side tables, transactions, journal schema changes or legacy migration. No git commit before user review. Authoritative current contract: docs/design/time-index.md and docs/design/data-model.md.
