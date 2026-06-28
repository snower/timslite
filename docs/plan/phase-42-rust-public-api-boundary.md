# Phase 42: Rust Public API Boundary

Status: completed

## Goal

Make timslite safe to use as a normal Rust library, not only through the C FFI layer. Public Rust APIs must enter through Store-managed lifecycle and runtime context paths, while raw storage helpers remain crate-internal.

## Completed

- [x] Removed public direct dataset constructors and raw lifecycle helpers: `DataSet::create`, `DataSet::open`, and `DataSet::drop_dataset` are crate-internal.
- [x] Removed public raw dataset guard access: `DataSet::lock` and `DataSetGuard` are no longer exposed.
- [x] Kept `DataSet` as a Store-managed safe operation view returned directly by `Store::create_dataset*` and `Store::open_dataset*`.
- [x] Internalized physical index/query APIs: `query_index_entries`, `query_sources`, `read_entry_at_index`, `read_length_at_index`, `IndexEntry`, `ReadIndexEntry`, `QueryIterator`, `QuerySource`, and `SourceIndex`.
- [x] Internalized raw queue plumbing: `DatasetQueue::new`, `QueueInner`, and `ConsumerStateFile`; public ordinary queue lifecycle goes through Store-managed `DataSet::open_queue` / `DataSet::close_queue`.
- [x] Internalized `util` as an implementation module instead of a public Rust module.
- [x] Made config stored fields crate-internal and added read-only getters; external callers use builders/getters instead of struct literals.
- [x] Added Store-level journal source dereference: `Store::read_journal_source_record(dataset_identifier, index_info)`.
- [x] Updated integration tests and Python wrapper to use safe Store/DataSet public paths.
- [x] Updated design docs for Store/DataSet/query/queue/journal boundaries.

## Verification

- `cargo test --no-run`
- `cargo fmt -- --check`
- `cargo test -- --test-threads=1`
- `cargo clippy --all-targets -- -D warnings`
- `cargo check --manifest-path wrapper/python/Cargo.toml`
- `cargo test --manifest-path wrapper/python/Cargo.toml`
- `maturin develop --manifest-path wrapper/python/Cargo.toml`
- `python -m pytest wrapper/python/tests -q`
- `git diff --check`
