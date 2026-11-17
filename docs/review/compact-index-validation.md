# Compact Index Validation

Date: 2026-11-17. Changes remain uncommitted pending user review.

## Scope
16B index core with u48 segment-relative block offset; immutable index_data_size; runtime 8B-aligned stride; creation configuration in all wrappers. Dataset record read/write APIs and 18B journal pointers are unchanged.

## Root evidence
After strict-load gate repairs:
- `cargo test -- --test-threads=1`: exit 0, 925 passed; 2 doctests ignored.
- `cargo fmt -- --check`: exit 0.
- `cargo clippy -- -D warnings`: exit 0.
- `git diff --check`: exit 0.

New public-API regressions in `tests/index_open_validation_test.rs` establish rejection of normalized maximum mismatch, off-grid continuous slots/segments and unordered non-continuous entries, plus acceptance of N=3/max=4097 normalization. Index module tests cover delta boundaries, paired sentinel legality, custom bytes, padding, reopen and sparse filler base propagation.

## Review
Initial oracle gate identified missing dataset/header maximum comparison and continuous grid validation. Both repaired with regression tests (RED observed by neutralizing checks, then restored GREEN); scoped gate attempt 2 passed with no remaining blocker. Review confirmed N=3/max4097 normalizes to4088, i128 timestamp/grid validation, non-continuous ordering and strict lazy lifecycle.

## Final wrapper evidence
Native artifacts rebuilt after core repairs; all commands exited 0:
- C: `cargo test --manifest-path wrapper/cffi/Cargo.toml -- --test-threads=1`: 16 passed. Earlier `cargo check --manifest-path wrapper/cffi/Cargo.toml --all-targets` and C header syntax check also passed.
- Python: `PYTHONPATH=python /opt/homebrew/bin/python3.14 -m pytest tests -q`: 125 passed, using fresh extension built with pyo3/extension-module and dynamic_lookup link flags.
- Node.js: `npm run build` then `npm test`: 107 passed.
- Java: native cargo build then `mvn -q -Dexec.skip=true test`: surefire reports 108 tests, no failures/errors/skips. Skip applies only to binding generation, not tests.
- .NET: native cargo build/test (25 passed), then `dotnet test tests/Timslite.Tests/Timslite.Tests.csproj`: 106 passed. Fresh native library copied before testing.

Wrapper logs: `/tmp/timslite-wrapper-final/`; root test log: `/private/var/folders/hq/072wwkd16pjg0fcyr5rjc9qm0000gn/T/opencode/compact-index-root-tests.log`. Logs are local verification artifacts, not tracked deliverables.

## Limitations
No performance benchmark was run; improved density is established by layout, not a measured speed claim. Custom index data APIs and overflow rebasing remain out of scope. Original config/wrapper implementation lanes did not capture a pre-implementation RED run; subsequent boundary regressions did. Existing non-WAL orphan-payload semantics remain unchanged.
