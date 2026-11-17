//! Hot-path benchmarks: sparse queue backlog, raw pending reads, concurrent
//! compressed-block cache hits, and full-scan query comparison.
//!
//! Run with `cargo bench --bench bench_hotpaths`.
//! Scale down with `TIMSLITE_BENCH_SCALE=0.1 cargo bench --bench bench_hotpaths`.
//!
//! This benchmark is standalone (no external data files) and uses assertions to
//! validate that each hot path is actually exercised. It deliberately avoids
//! timing assertions.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;
use timslite::{
    DataSet, DataSetConfig, Store, StoreConfig, BLOCK_MAX_SIZE, QUEUE_MAX_PENDING_ENTRIES,
};

const BLOCK_HEADER_OVERHEAD: usize = 12;
const RECORD_HEADER_SIZE: usize = 12;
const DATASET_TYPE: &str = "hotpaths";

fn bench_scale() -> f64 {
    match std::env::var("TIMSLITE_BENCH_SCALE") {
        Ok(raw) => {
            let scale: f64 = raw
                .parse()
                .unwrap_or_else(|_| panic!("TIMSLITE_BENCH_SCALE must be a number, got {raw:?}"));
            assert!(
                scale.is_finite() && scale > 0.0,
                "TIMSLITE_BENCH_SCALE must be finite and > 0, got {scale}"
            );
            scale
        }
        Err(_) => 1.0,
    }
}

fn scaled(base: usize, scale: f64, min: usize) -> usize {
    (((base as f64) * scale) as usize).max(min)
}

fn report(case: &str, params: &str, ops: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let ns_per_op = if ops == 0 {
        0.0
    } else {
        elapsed.as_nanos() as f64 / ops as f64
    };
    println!(
        "[bench] case={case} params={params} ops={ops} elapsed={secs:.6}s ns/op={ns_per_op:.1}"
    );
}

fn new_temp_dir() -> TempDir {
    TempDir::new().expect("create temp dir")
}

fn open_store(dir: &Path) -> Store {
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build();
    Store::open(dir, config).expect("open store")
}

fn create_dataset(
    store: &mut Store,
    name: &str,
    index_continuous: u8,
    initial_data_segment_size: u64,
    initial_index_segment_size: u64,
) -> DataSet {
    let builder = DataSetConfig::builder()
        .data_segment_size(64 * 1024 * 1024)
        .index_segment_size(16 * 1024 * 1024)
        .initial_data_segment_size(initial_data_segment_size)
        .initial_index_segment_size(initial_index_segment_size)
        .compress_level(6)
        .compress_type(0)
        .index_continuous(index_continuous)
        .retention_window(0)
        .enable_journal(false);
    store
        .create_dataset_with_config(name, DATASET_TYPE, Some(builder))
        .expect("create dataset")
}

/// Case 1: sparse queue backlog.
///
/// A consumer group is opened *before* any data exists, then records are
/// written with large timestamp gaps. Every poll advances over sparse index
/// gaps and is immediately acked, so the 4 KiB state file never accumulates
/// more than one pending entry (`QUEUE_MAX_PENDING_ENTRIES = 226`).
fn bench_sparse_queue_backlog(scale: f64) {
    let temp_dir = new_temp_dir();
    let mut store = open_store(&temp_dir.path().join("sparse_queue"));
    let dataset = create_dataset(&mut store, "sparse_q", 0, 1024 * 1024, 64 * 1024);

    let queue = dataset.open_queue().expect("open queue");
    // Opened against an empty dataset: initial processed_ts = i64::MIN.
    let consumer = queue
        .open_consumer("bench_sparse_group")
        .expect("open consumer");

    let records = scaled(3_000, scale, 64);
    let base_ts: i64 = 1_000_000_000;
    let step: i64 = 10_000_000;
    let payload: &[u8] = b"sparse-queue-record";

    let mut timestamps = Vec::with_capacity(records);
    for i in 0..records {
        let ts = base_ts + (i as i64) * step;
        dataset.write(ts, payload).expect("queue backlog write");
        timestamps.push(ts);
    }
    assert_eq!(
        dataset.latest_written_timestamp(),
        Some(*timestamps.last().expect("non-empty backlog"))
    );

    let start = Instant::now();
    let mut max_pending = 0usize;
    let mut polled = 0u64;
    for (i, &expected_ts) in timestamps.iter().enumerate() {
        let (ts, data) = consumer
            .poll(Duration::from_millis(0))
            .expect("poll backlog")
            .unwrap_or_else(|| panic!("missing backlog record at index {i}"));
        assert_eq!(ts, expected_ts, "poll order mismatch at index {i}");
        assert_eq!(data.as_slice(), payload);

        // Sample occupancy without adding an inspect() to every measured op.
        if i % 512 == 0 || i + 1 == records {
            let state = consumer.inspect().expect("inspect consumer");
            max_pending = max_pending.max(state.state.pending_entries.len());
        }

        consumer.ack(ts).expect("ack backlog record");
        polled += 1;
    }
    let elapsed = start.elapsed();

    assert_eq!(polled, records as u64);
    assert!(max_pending <= 1, "pending entries grew to {max_pending}");
    assert!(
        max_pending < QUEUE_MAX_PENDING_ENTRIES,
        "pending entries must stay below the state-file capacity"
    );
    assert!(consumer
        .poll(Duration::from_millis(0))
        .expect("poll after drain")
        .is_none());
    let state = consumer.inspect().expect("inspect after drain");
    assert!(state.state.pending_entries.is_empty());
    assert_eq!(state.state.processed_ts, *timestamps.last().unwrap());

    report(
        "sparse_queue_poll_ack",
        &format!(
            "records={records} ts_step={step} acked_each=true queue_max_pending={QUEUE_MAX_PENDING_ENTRIES} observed_max_pending={max_pending}"
        ),
        polled,
        elapsed,
    );
}

/// Case 2: reads against a near-full raw pending block.
///
/// Small records are packed until ~92% of `BLOCK_MAX_SIZE`, staying below the
/// overflow threshold so the pending block never seals/compresses. Reading the
/// tail through `read_latest` / `read_length` therefore exercises raw pending
/// reads, and the global block cache must stay untouched.
fn bench_raw_pending_reads(scale: f64) {
    let temp_dir = new_temp_dir();
    let mut store = open_store(&temp_dir.path().join("raw_pending"));
    let dataset = create_dataset(&mut store, "raw_pending", 0, 1024 * 1024, 256 * 1024);

    let payload_len = 32usize;
    let payload = vec![0x5Au8; payload_len];
    let record_span = RECORD_HEADER_SIZE + payload_len; // already 4-byte aligned
    let max_records = (BLOCK_MAX_SIZE as usize - BLOCK_HEADER_OVERHEAD) / record_span;
    let records = max_records * 92 / 100;
    let pending_bytes = records * record_span;
    assert!(pending_bytes <= BLOCK_MAX_SIZE as usize);
    assert!(pending_bytes * 100 >= BLOCK_MAX_SIZE as usize * 90);

    for i in 1..=records {
        dataset
            .write(i as i64, &payload)
            .expect("raw pending write");
    }
    let last_ts = records as i64;
    assert_eq!(dataset.latest_written_timestamp(), Some(last_ts));

    // Raw pending blocks are not eligible for the global cache.
    let stats = store.block_cache().stats();
    assert_eq!(stats.hit_count, 0, "near-full pending must not hit cache");
    assert_eq!(stats.miss_count, 0, "near-full pending must not miss cache");

    let mut rng = StdRng::seed_from_u64(0x2EAD_1A7E);
    let read_ops = scaled(50_000, scale, 100);

    let start = Instant::now();
    for _ in 0..read_ops {
        let (ts, data) = dataset
            .read_latest()
            .expect("read_latest")
            .expect("pending tail exists");
        assert_eq!(ts, last_ts);
        assert_eq!(data.len(), payload_len);
    }
    let latest_elapsed = start.elapsed();
    report(
        "raw_pending_read_latest",
        &format!(
            "records={records} payload={payload_len}B pending_bytes={pending_bytes}/{} ({:.1}%) sealed=false",
            BLOCK_MAX_SIZE,
            pending_bytes as f64 * 100.0 / BLOCK_MAX_SIZE as f64
        ),
        read_ops as u64,
        latest_elapsed,
    );

    let sample_ts: Vec<i64> = (0..read_ops).map(|_| rng.gen_range(1..=last_ts)).collect();

    let start = Instant::now();
    for &ts in &sample_ts {
        let len = dataset
            .read_length(ts)
            .expect("read_length")
            .expect("pending record exists");
        assert_eq!(len as usize, payload_len);
    }
    let length_elapsed = start.elapsed();
    report(
        "raw_pending_read_length",
        &format!(
            "records={records} payload={payload_len}B pending_bytes={pending_bytes}/{} sealed=false",
            BLOCK_MAX_SIZE
        ),
        read_ops as u64,
        length_elapsed,
    );

    let stats = store.block_cache().stats();
    assert_eq!(stats.hit_count, 0, "raw pending reads must not hit cache");
    assert_eq!(stats.miss_count, 0, "raw pending reads must not miss cache");
}

fn read_workload(
    dataset_indices: &[usize],
    ops: usize,
    read_limit: usize,
    rng: &mut StdRng,
) -> Vec<(usize, i64)> {
    assert!(!dataset_indices.is_empty());
    let mut workload = Vec::with_capacity(ops);
    for i in 0..ops {
        let dataset_idx = dataset_indices[i % dataset_indices.len()];
        let ts = rng.gen_range(1..=read_limit as i64);
        workload.push((dataset_idx, ts));
    }
    workload
}

fn run_read_group(workload: &[(usize, i64)], datasets: &Arc<Vec<DataSet>>, payload_len: usize) {
    for &(dataset_idx, ts) in workload {
        let (read_ts, data) = datasets[dataset_idx]
            .read(ts)
            .expect("compressed block read")
            .expect("compressed block record exists");
        assert_eq!(read_ts, ts);
        assert_eq!(data.len(), payload_len);
    }
}

/// Case 3: concurrent hits on hot *compressed* blocks.
///
/// The `cache` module is not public, so `BlockCache::get` cannot be called
/// directly. Instead each dataset owns a sealed+compressed block whose reads
/// go through the store's global block cache. Threads are partitioned onto
/// disjoint datasets so the same dataset mutex never dominates, and cache stats
/// prove every measured read is a hit.
fn bench_cached_compressed_block_reads(scale: f64) {
    let temp_dir = new_temp_dir();
    let mut store = open_store(&temp_dir.path().join("compressed_cache"));
    let dataset_count = 8usize;
    let payload_len = 64usize;
    let payload: Vec<u8> = (0..payload_len).map(|i| b'a' + (i % 26) as u8).collect();
    let record_span = RECORD_HEADER_SIZE + payload_len;
    let block_records = (BLOCK_MAX_SIZE as usize - BLOCK_HEADER_OVERHEAD) / record_span;
    let read_limit = block_records - 64;
    let write_records = block_records + 64;
    assert!(read_limit > 0 && write_records > block_records);

    let mut datasets = Vec::with_capacity(dataset_count);
    for i in 0..dataset_count {
        let name = format!("cache_ds_{i}");
        let dataset = create_dataset(&mut store, &name, 0, 1024 * 1024, 256 * 1024);
        for ts in 1..=write_records {
            dataset
                .write(ts as i64, &payload)
                .expect("cache dataset write");
        }
        datasets.push(dataset);
    }
    let datasets = Arc::new(datasets);

    // Warm every sealed block into the global cache.
    let mut warmup_misses = 0u64;
    for dataset in datasets.iter() {
        for ts in 1..=read_limit {
            let (read_ts, data) = dataset
                .read(ts as i64)
                .expect("warmup read")
                .expect("warmup record exists");
            assert_eq!(read_ts, ts as i64);
            assert_eq!(data.len(), payload_len);
        }
    }
    let warm_stats = store.block_cache().stats();
    assert_eq!(
        warm_stats.miss_count, dataset_count as u64,
        "one miss per sealed block expected during warmup"
    );
    assert!(warm_stats.hit_count > 0);
    warmup_misses += warm_stats.miss_count;

    // Same total operation count for the 1-thread and 4-thread runs.
    let ops_per_thread = scaled(50_000, scale, 250);
    let total_ops = ops_per_thread * 4;
    let all_indices: Vec<usize> = (0..dataset_count).collect();
    let mut rng = StdRng::seed_from_u64(0xC0FF_EE42);
    let single_workload = read_workload(&all_indices, total_ops, read_limit, &mut rng);

    let before = store.block_cache().stats();
    let start = Instant::now();
    run_read_group(&single_workload, &datasets, payload_len);
    let single_elapsed = start.elapsed();
    let after_single = store.block_cache().stats();
    assert_eq!(
        after_single.hit_count - before.hit_count,
        total_ops as u64,
        "every 1-thread read must be a cache hit"
    );
    assert_eq!(after_single.miss_count, before.miss_count);
    assert_eq!(warmup_misses, dataset_count as u64);
    report(
        "compressed_cache_read_1t",
        &format!(
            "datasets={dataset_count} threads=1 records_per_dataset={write_records} sealed_block={block_records} ops_per_thread={total_ops} cache_hits={}",
            after_single.hit_count - before.hit_count
        ),
        total_ops as u64,
        single_elapsed,
    );

    let groups: Vec<Vec<(usize, i64)>> = (0..4)
        .map(|t| {
            let indices = vec![t * 2, t * 2 + 1];
            read_workload(&indices, ops_per_thread, read_limit, &mut rng)
        })
        .collect();

    let before = store.block_cache().stats();
    let start = Instant::now();
    std::thread::scope(|scope| {
        for group in &groups {
            let datasets = Arc::clone(&datasets);
            scope.spawn(move || run_read_group(group, &datasets, payload_len));
        }
    });
    let four_elapsed = start.elapsed();
    let after_four = store.block_cache().stats();
    assert_eq!(
        after_four.hit_count - before.hit_count,
        total_ops as u64,
        "every 4-thread read must be a cache hit"
    );
    assert_eq!(after_four.miss_count, before.miss_count);
    report(
        "compressed_cache_read_4t",
        &format!(
            "datasets={dataset_count} threads=4 datasets_per_thread=2 records_per_dataset={write_records} sealed_block={block_records} ops_per_thread={ops_per_thread} cache_hits={}",
            after_four.hit_count - before.hit_count
        ),
        total_ops as u64,
        four_elapsed,
    );
}

/// Case 4: full-scan `query` (Vec) versus `query_iter`.
fn bench_query_full_scan(scale: f64) {
    let temp_dir = new_temp_dir();
    let mut store = open_store(&temp_dir.path().join("query_scan"));
    let dataset = create_dataset(
        &mut store,
        "query_scan",
        1,
        16 * 1024 * 1024,
        4 * 1024 * 1024,
    );

    let records = scaled(100_000, scale, 1_000);
    let payload_len = 64usize;
    let payload: Vec<u8> = (0..payload_len).map(|i| b'A' + (i % 26) as u8).collect();
    for ts in 1..=records {
        dataset
            .write(ts as i64, &payload)
            .expect("query scan write");
    }
    let start_ts = 1i64;
    let end_ts = records as i64;

    // Warm the block cache before timing.
    let warm = dataset.query(start_ts, end_ts).expect("warm query");
    assert_eq!(warm.len(), records);
    assert_eq!(warm[0].0, start_ts);
    assert_eq!(warm[records - 1].0, end_ts);
    assert!(warm.iter().all(|(_, data)| data.len() == payload_len));
    drop(warm);

    let reps = scaled(3, scale, 1);
    let mut query_bytes = 0u64;
    let start = Instant::now();
    for _ in 0..reps {
        let rows = dataset.query(start_ts, end_ts).expect("full query");
        assert_eq!(rows.len(), records);
        query_bytes = rows.iter().map(|(_, data)| data.len() as u64).sum();
    }
    let query_elapsed = start.elapsed();
    assert_eq!(query_bytes, (records * payload_len) as u64);
    report(
        "query_vec_full_scan",
        &format!(
            "records={records} payload={payload_len}B range={start_ts}..{end_ts} scans={reps} records_per_scan={records}"
        ),
        reps as u64,
        query_elapsed,
    );

    let warm_iter = dataset
        .query_iter(start_ts, end_ts)
        .expect("warm query_iter")
        .collect_all()
        .expect("warm query_iter collect");
    assert_eq!(warm_iter.len(), records);
    drop(warm_iter);

    let mut iter_bytes = 0u64;
    let start = Instant::now();
    for _ in 0..reps {
        let rows = dataset
            .query_iter(start_ts, end_ts)
            .expect("query_iter")
            .collect_all()
            .expect("query_iter collect");
        assert_eq!(rows.len(), records);
        iter_bytes = rows.iter().map(|(_, data)| data.len() as u64).sum();
        assert_eq!(rows[0].0, start_ts);
        assert_eq!(rows[records - 1].0, end_ts);
    }
    let iter_elapsed = start.elapsed();
    assert_eq!(iter_bytes, query_bytes);
    report(
        "query_iter_full_scan",
        &format!(
            "records={records} payload={payload_len}B range={start_ts}..{end_ts} scans={reps} records_per_scan={records}"
        ),
        reps as u64,
        iter_elapsed,
    );
}

fn main() {
    let scale = bench_scale();
    println!("[bench] suite=bench_hotpaths scale={scale}");

    bench_sparse_queue_backlog(scale);
    bench_raw_pending_reads(scale);
    bench_cached_compressed_block_reads(scale);
    bench_query_full_scan(scale);

    println!("[bench] suite=bench_hotpaths completed");
}
