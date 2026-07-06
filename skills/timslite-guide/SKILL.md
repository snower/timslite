---
name: timslite-guide
description: Use when integrating or using the timslite time-series storage library (Rust/Python/Node.js/Java), when writing/reading time-series data via Store/DataSet/Queue/Journal APIs, when debugging timslite errors, when configuring StoreConfig/DataSetConfig, or when understanding mmap-backed block storage and retention/queue/journal semantics
---

# timslite Developer Guide

## Overview

timslite is a high-performance **mmap-backed time-series data storage library** written in Rust. It can be used as a Rust crate, a C ABI dynamic library (`cdylib`), or through language bindings for Python, Node.js, and Java.

**Core principle**: Multiple records are aggregated into Blocks (max 64KB), blocks are lazily compressed on seal, and a time index points to `(block_offset, in_block_offset)`. Each `(dataset_name, dataset_type)` pair has independent meta, data segments, index segments, and optional queue state.

## When to Use

**Use timslite when you need:**
- Time-series data storage with timestamp-indexed reads and range queries
- High-throughput writes with mmap-backed persistence
- Block-level compression (zstd/deflate) to save disk space
- Multi-consumer-group queues on top of time-series data
- A change log (journal) for hot migration, sync, or audit
- Cross-language bindings (Rust, Python, Node.js, Java, C/C++)

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
└── .journal/logs/    # reserved journal
```

## Language Guides

Choose your language for detailed documentation, quick start guides, API references, and examples.

### Rust

- **Installation**: Add `timslite = "0.1"` to your `Cargo.toml`
- **[Quick Start](rust/quick-start.md)** — Getting started with Rust
- **[API Reference](rust/api-reference.md)** — Complete Rust API signatures
- **[Examples](rust/examples.md)** — Feature scenarios with Rust examples

### Python

- **Installation**: `pip install timslite`
- **[Quick Start](python/quick-start.md)** — Getting started with Python
- **[API Reference](python/api-reference.md)** — Complete Python API signatures
- **[Examples](python/examples.md)** — Feature scenarios with Python examples

### Node.js

- **Installation**: `npm install timslite`
- **[Quick Start](nodejs/quick-start.md)** — Getting started with Node.js
- **[API Reference](nodejs/api-reference.md)** — Complete Node.js API signatures
- **[Examples](nodejs/examples.md)** — Feature scenarios with Node.js examples

### Java

- **Installation**: Maven dependency `io.github.snower:timslite:0.1.3`
- **[Quick Start](java/quick-start.md)** — Getting started with Java
- **[API Reference](java/api-reference.md)** — Complete Java API signatures
- **[Examples](java/examples.md)** — Feature scenarios with Java examples

### C / C++

Download prebuilt `libtimslite` from [GitHub Releases](https://github.com/user/timslite/releases), or build from source: `cargo build --release`. Link against `libtimslite.so/.dylib/.dll` and include `include/timslite.h`.

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

## Troubleshooting

See **[troubleshooting.md](troubleshooting.md)** for common errors, root causes, and solutions.

## Detailed References

For in-depth information, see the language-specific guides above or the universal troubleshooting guide.