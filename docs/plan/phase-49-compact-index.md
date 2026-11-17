# Phase 49: Compact Index

- [x] Persist immutable index_data_size and segment base_block_offset; reject incompatible metadata.
- [x] Implement 16B core, u48 codec, runtime aligned stride and filler encoding.
- [x] Preserve reserved data across core updates and clear on delete; keep existing record APIs.
- [x] Thread real block base through sparse filler creation and all segment lifecycle paths.
- [x] Expose creation configuration in C/Python/Node.js/Java/.NET wrappers.
- [x] Validate byte layout, range failures, corruption, continuous grids and reopen behavior.
- [x] Run serial root suite, fmt/clippy, C check/test and supported wrapper checks.

Spec: ../superpowers/specs/2026-11-17-compact-index-design.md
Implementation plan: ../superpowers/plans/2026-11-17-compact-index.md
