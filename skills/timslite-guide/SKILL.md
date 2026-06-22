---
name: timslite-guide
description: Use when integrating or using the timslite time-series storage library (Rust/Python/C FFI), when writing/reading time-series data via Store/DataSet/Queue/Journal APIs, when debugging timslite errors, when configuring StoreConfig/DataSetConfig, or when understanding mmap-backed block storage and retention/queue/journal semantics
---

# timslite Developer Guide

## Overview

timslite is a high-performance **mmap-backed time-series data storage library** written in Rust. It can be used as a Rust crate, a C ABI dynamic library (`cdylib`), or through Python bindings (PyO3).

**Core principle**: Multiple records are aggregated into Blocks (max 64KB), blocks are lazily compressed on seal, and a time index points to `(block_offset, in_block_offset)`. Each `(dataset_name, dataset_type)` pair has independent meta, data segments, index segments, and optional queue state.

## When to Use

**Use timslite when you need:**
- Time-series data storage with timestamp-indexed reads and range queries
- High-throughput writes with mmap-backed persistence
- Block-level compression (zstd/deflate) to save disk space
- Multi-consumer-group queues on top of time-series data
- A change log (journal) for hot migration, sync, or audit
- C ABI or Python bindings for cross-language integration

**Do NOT use timslite for:**
- Relational data with complex queries (use a SQL database)
- Full-text search (use a search engine)
- Strong transactional consistency (journal is an auxiliary log, not a WAL)

## Architecture

```
Store (facade, data_dir level)
├── DataSet (per (name, type) pair)
│   ├── DataSegmentSet → DataSegment (mmap data files, multiple Blocks)
│   └── TimeIndex → IndexSegment (mmap index files, binary search)
├── JournalManager (.journal/logs append log)
└── BlockCache (global immutable compressed-block cache)
```

**Storage hierarchy**: Record → Block (≤64KB payload) → DataSegment (file) → DataSet → Store.

**Key concepts**:
- `block_offset`: logical global offset from data area start, points to BlockHeader
- Physical offset = `segment.header_len + (block_offset - segment.file_offset)`
- Data segment filename = logical data area base offset (20-digit zero-padded)
- Index segment filename = segment base timestamp (20-digit zero-padded)

**Directory layout**:
```
{data_dir}/
├── {dataset_name}/{dataset_type}/
│   ├── identifier    # numeric dataset ID assigned by Store
│   ├── meta          # dataset metadata (TLV format)
│   ├── state         # inspect statistics cache
│   ├── data/         # data segment files
│   └── index/        # index segment files
└── .journal/logs/    # reserved journal path
    ├── data/
    └── queue/{group_name}
```

## Quick Start

### Rust

```rust
use timslite::{Store, StoreConfig};

// 1. Open a store
let config = StoreConfig::default();
let mut store = Store::open("/data/timslite", config)?;

// 2. Create a dataset (explicit parameters)
store.create_dataset(
    "sensor_001", "events",
    64 * 1024 * 1024,  // data_segment_size = 64MB
    4 * 1024 * 1024,   // index_segment_size = 4MB
    6,                 // compress_level (0-9)
    0,                 // index_continuous (0=sparse, 1=continuous)
    0,                 // retention_window (0 = no limit, in timestamp units)
)?;

// 3. Open the dataset (parameters read from meta)
let handle = store.open_dataset("sensor_001", "events")?;

// 4. Write records (timestamp must be >= latest_written_timestamp)
let ds = store.get_dataset(&handle)?;
ds.write(1, b"event_0")?;
ds.write(2, b"event_1")?;

// 5. Read single record
let record = ds.read(1)?;  // Option<(i64, Vec<u8>)>

// 6. Range query
let entries = ds.query(1, 100)?;  // Vec<(i64, Vec<u8>)>

// 7. Close
store.close()?;
```

### Python

```python
import timslite

with timslite.Store.open("/data/timslite") as store:
    store.create_dataset("sensor", "waveform")
    ds = store.open_dataset("sensor", "waveform")
    ds.write(1, b"reading_1")
    ds.write(2, b"reading_2")

    # Read single record
    record = ds.read(1)  # -> (1, b"reading_1") or None

    # Read latest
    latest = ds.read_latest()  # -> (2, b"reading_2") or None

    # Range query (lazy iterator)
    for ts, data in ds.query(1, 100):
        print(f"ts={ts}, data={data}")

    # Delete a record
    ds.delete(1)
```

### C FFI

```c
#include "timslite.h"

char err[256];
TmslStoreConfigFFI config;
tmsl_store_config_default(&config, err, sizeof(err));

void* store = tmsl_store_open_with_config("/data/timslite", &config, err, sizeof(err));
if (!store) { /* handle error */ }

// Create dataset, write, query via FFI functions...
tmsl_store_close(store, err, sizeof(err));
```

## API Quick Reference

### Store (lifecycle + dataset management)

| Method | Purpose |
|--------|---------|
| `Store::open(dir, config)` | Open or create a store |
| `store.close()` | Flush and close everything |
| `store.create_dataset(name, type, ...)` | Create dataset with explicit params |
| `store.create_dataset_with_config(name, type, builder)` | Create with DataSetConfigBuilder |
| `store.open_dataset(name, type)` | Open existing dataset → DataSetHandle |
| `store.open_dataset_by_identifier(id)` | Open by numeric identifier |
| `store.drop_dataset(name, type)` | Delete dataset and all files |
| `store.get_dataset(&handle)` | Get `Arc<DataSet>` for read/write |
| `store.list_datasets()` | List all dataset (name, type) pairs |
| `store.inspect_dataset(name, type)` | Get DataSetInfo + DataSetState |
| `store.is_read_only()` | Check if store is read-only |

### DataSet (read/write operations)

| Method | Returns | Purpose |
|--------|---------|---------|
| `ds.write(ts, data)` | `Result<()>` | Write a record (ts >= latest) |
| `ds.append(ts, data)` | `Result<()>` | Forward append or in-place tail append |
| `ds.delete(ts)` | `Result<()>` | Delete a record by timestamp |
| `ds.read(ts)` | `Option<(i64, Vec<u8>)>` | Read single record |
| `ds.read_latest()` | `Option<(i64, Vec<u8>)>` | Read latest written record |
| `ds.read_exist(ts)` | `bool` | Fast existence check (index only) |
| `ds.read_length(ts)` | `Option<u32>` | Read record data length (header only) |
| `ds.query(start, end)` | `Vec<(i64, Vec<u8>)>` | Range query (eager) |
| `ds.query_iter(start, end)` | `QueryIterator` | Range query (lazy) |
| `ds.query_exist(start, end)` | `Vec<u8>` (bitmap) | Fast range existence bitmap |
| `ds.query_length(start, end)` | `Vec<(i64, u32)>` | Range lengths (header only) |
| `ds.query_length_iter(start, end)` | `QueryLengthIterator` | Lazy range lengths |
| `ds.flush()` | `Result<()>` | Flush dirty segments to disk |
| `ds.inspect()` | `DataSetInspectResult` | Get info + state |
| `ds.latest_timestamp()` | `Option<i64>` | Get latest written timestamp |

### Queue (consumer group semantics)

| Method | Purpose |
|--------|---------|
| `store.open_queue(handle)` | Open DatasetQueue for a dataset |
| `store.queue_push(&q, data)` | Push data, auto-assigns next timestamp |
| `store.queue_poll(&c, timeout)` | Poll next unacked record |
| `store.queue_ack(&c, ts)` | Ack a polled record |
| `store.open_consumer(&q, group)` | Open consumer group |
| `store.open_consumer_with_config(&q, group, cfg)` | Open with retry config |
| `store.drop_consumer(&q, group)` | Drop a consumer group |
| `store.close_queue(q)` | Close queue |

### Journal (change log)

| Method | Purpose |
|--------|---------|
| `store.journal_latest_sequence()` | Get latest journal sequence |
| `store.journal_read(seq)` | Read a journal record by sequence |
| `store.journal_query(start, end)` | Range query journal records |
| `store.open_journal_queue()` | Open JournalQueue for consumption |
| `store.read_journal_source_record(id, index_info)` | Dereference journal to source data |

### Background Tasks

| Method | Purpose |
|--------|---------|
| `store.tick_background_tasks()` | Manual tick (returns TickResult) |
| `store.next_background_delay()` | Query delay until next task due |

## Configuration Defaults

### StoreConfig

| Field | Default | Purpose |
|-------|---------|---------|
| `flush_interval` | 15s | Background flush interval |
| `idle_timeout` | 1800s (30min) | Segment idle-close timeout |
| `data_segment_size` | 64 MiB | Default for new datasets |
| `index_segment_size` | 4 MiB | Default for new datasets |
| `initial_data_segment_size` | 256 KiB | Lazy allocation initial size |
| `initial_index_segment_size` | 4 KiB | Lazy allocation initial size |
| `compress_level` | 6 | Default compression level (0-9) |
| `compress_type` | 0 (zstd) | Compression algorithm |
| `cache_max_memory` | 256 MiB | Block cache memory limit (0=disabled) |
| `cache_idle_timeout` | 1800s | Cache entry eviction timeout |
| `retention_check_hour` | 0 | UTC hour for daily retention (0-23) |
| `enable_background_thread` | true | Auto-spawn background thread |
| `enable_journal` | true | Enable change log |
| `read_only` | None (auto) | None=auto, Some(true)=force RO |

### DataSetConfig (create-time only, immutable after)

| Field | Default | Purpose |
|-------|---------|---------|
| `data_segment_size` | from StoreConfig | Data segment max file size |
| `index_segment_size` | from StoreConfig | Index segment max file size |
| `initial_data_segment_size` | from StoreConfig | Lazy allocation initial size |
| `initial_index_segment_size` | from StoreConfig | Lazy allocation initial size |
| `compress_level` | from StoreConfig | Compression level (0-9) |
| `compress_type` | from StoreConfig | 0=zstd, 1=deflate |
| `index_continuous` | 0 (sparse) | 0=sparse, 1=continuous |
| `retention_window` | 0 (no limit) | Retention in timestamp units |
| `enable_journal` | true | Whether to journal this dataset |

## Detailed References

For in-depth information, load these supporting files:

- **[api-reference.md](api-reference.md)** — Complete API signatures, parameters, return types, and semantics for Store, DataSet, Queue, Journal, Config, and FFI
- **[scenarios.md](scenarios.md)** — Feature scenario analysis with copy-paste examples: write patterns, query optimization, queue consumption, journal recovery, retention, read-only mode
- **[troubleshooting.md](troubleshooting.md)** — Common errors, root causes, and solutions

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Calling `create_dataset` on existing name | Check with `list_datasets` first or handle `AlreadyExists` |
| Writing with `timestamp < latest_written_timestamp` | Use `append` for same-timestamp or accept out-of-order rejection |
| Record data > 4 MiB | Split data or use external blob storage |
| Not calling `flush()` before process exit in manual bg mode | Call `ds.flush()` or `store.close()` which flushes |
| Polling queue without opening consumer first | Call `open_consumer` before `queue_poll` |
| Expecting journal to be a WAL | Journal is auxiliary; no transaction guarantees |
| Forgetting `enable_background_thread=false` needs manual tick | Call `tick_background_tasks()` periodically |
| Assuming `read(-1)` reads latest | Use `read_latest()`; `read(-1)` reads timestamp `-1` |

## Naming Rules

Dataset name, dataset type, and queue consumer group name must match `^[0-9A-Za-z_-]+$`, max 255 bytes. `.journal` is reserved.
