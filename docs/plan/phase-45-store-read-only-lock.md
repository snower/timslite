# Phase 45: Store Read-only Lock

## Goal

Prevent two writable Store instances from opening the same Store root while still allowing backup/migration readers to inspect data already persisted at open time.

## Contract

- Store uses `{data_dir}/.lock` only as the OS file-lock target.
- The existence of `.lock` is not a lock signal. A stale unlocked file must not force read-only mode.
- A writable Store keeps the acquired lock handle as `Option<File>` and releases it through normal Store drop/close.
- `StoreConfig.read_only` is tri-state:
  - `None`: auto mode, writable when the OS lock is acquired, otherwise read-only.
  - `Some(false)`: require writable mode and fail when the OS lock is held.
  - `Some(true)`: force read-only mode without checking or acquiring the OS lock.
- Read-only Store rejects all write paths, queue operations, journal queue open, and background task processing.
- Read-only journal supports latest/read/query only.
- Missing `.journal/logs` in read-only mode is treated as an empty journal and must not create directories or files.
- Read-only Store is an open-time persisted view, not a live reader of another writer's in-memory state.

## Implementation

- [x] Rust `StoreConfig` exposes `read_only: Option<bool>` with builder/getter support.
- [x] Store open acquires the OS file lock in writable modes and auto-falls back to read-only when configured.
- [x] Store keeps the lock file handle for writable mode.
- [x] Store and Store-managed datasets reject mutations in read-only mode.
- [x] Background tasks are disabled for read-only Store.
- [x] Dataset state and journal segments can open read-only without creating missing files.
- [x] Journal read-only manager supports latest/read/query and treats missing journal storage as empty.
- [x] FFI config version includes `read_only_mode`.
- [x] Python `StoreConfig` exposes `read_only`.

## Verification

- [x] Rust integration tests cover auto read-only fallback, forced writable failure, forced read-only open, stale `.lock`, write rejection, and read-only journal behavior.
- [x] FFI config conversion test covers AUTO/FALSE/TRUE modes.
- [x] Python config tests cover the tri-state wrapper surface.
