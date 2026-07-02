mod common;

use common::{create_temp_dir, BenchmarkMetrics, LogData};
use rand::Rng;
use std::time::Instant;
use timslite::{Store, StoreConfig};

const WRITE_COUNT: u64 = 400_000;
const READ_SEQUENTIAL_COUNT: u64 = 300_000;
const READ_RANDOM_COUNT: u64 = 200_000;
const QUERY_TOTAL_RECORDS: u64 = 1_000_000;

fn main() {
    let log_data = LogData::load();
    let temp_dir = create_temp_dir();
    let data_dir = temp_dir.path().join("timslite_data");

    let config = StoreConfig::default();
    let mut store = Store::open(&data_dir, config).unwrap();

    let dataset = store
        .create_dataset(
            "bench_data",
            "logs",
            64 * 1024 * 1024,
            16 * 1024 * 1024,
            6,
            0,
            0,
        )
        .unwrap();

    let mut metrics = BenchmarkMetrics::new();

    let start = Instant::now();
    for i in 1..=WRITE_COUNT {
        let line = log_data.random_raw_line();
        dataset.write(i as i64, line.as_bytes()).unwrap();
        metrics.write_bytes += line.len() as u64;
    }
    metrics.write_duration = start.elapsed();
    metrics.write_ops = WRITE_COUNT;

    let start = Instant::now();
    for i in 1..=READ_SEQUENTIAL_COUNT {
        if let Some((_, data)) = dataset.read(i as i64).unwrap() {
            metrics.read_sequential_bytes += data.len() as u64;
        }
    }
    metrics.read_sequential_duration = start.elapsed();
    metrics.read_sequential_ops = READ_SEQUENTIAL_COUNT;

    let mut rng = rand::thread_rng();
    let start = Instant::now();
    for _ in 0..READ_RANDOM_COUNT {
        let ts = rng.gen_range(1..=WRITE_COUNT as i64);
        if let Some((_, data)) = dataset.read(ts).unwrap() {
            metrics.read_random_bytes += data.len() as u64;
        }
    }
    metrics.read_random_duration = start.elapsed();
    metrics.read_random_ops = READ_RANDOM_COUNT;

    // query benchmark
    let mut rng = rand::thread_rng();
    let start = Instant::now();
    let mut total_query_records = 0u64;
    let mut query_ops = 0u64;
    while total_query_records < QUERY_TOTAL_RECORDS {
        let batch_size = rng.gen_range(5..=2000) as i64;
        let max_start = WRITE_COUNT as i64 - batch_size;
        let start_ts = if max_start <= 1 {
            1
        } else {
            rng.gen_range(1..=max_start)
        };
        let end_ts = start_ts + batch_size - 1;
        let results = dataset.query(start_ts, end_ts).unwrap();
        let count = results.len() as u64;
        let bytes: u64 = results.iter().map(|(_, d)| d.len() as u64).sum();
        total_query_records += count;
        metrics.query_bytes += bytes;
        query_ops += 1;
    }
    metrics.query_duration = start.elapsed();
    metrics.query_records = total_query_records;
    metrics.query_ops = query_ops;

    // query_iter benchmark (30% reverse)
    let mut rng = rand::thread_rng();
    let start = Instant::now();
    let mut total_iter_records = 0u64;
    let mut iter_ops = 0u64;
    while total_iter_records < QUERY_TOTAL_RECORDS {
        let batch_size = rng.gen_range(5..=2000) as i64;
        let max_start = WRITE_COUNT as i64 - batch_size;
        let start_ts = if max_start <= 1 {
            1
        } else {
            rng.gen_range(1..=max_start)
        };
        let end_ts = start_ts + batch_size - 1;
        let iter = dataset.query_iter(start_ts, end_ts).unwrap();
        let iter = if rng.gen_bool(0.3) { iter.reverse() } else { iter };
        let results = iter.collect_all().unwrap();
        let count = results.len() as u64;
        let bytes: u64 = results.iter().map(|(_, d)| d.len() as u64).sum();
        total_iter_records += count;
        metrics.query_iter_bytes += bytes;
        iter_ops += 1;
    }
    metrics.query_iter_duration = start.elapsed();
    metrics.query_iter_records = total_iter_records;
    metrics.query_iter_ops = iter_ops;

    let inspect_result = store.inspect_dataset("bench_data", "logs").unwrap();
    metrics.total_data_size = inspect_result.state.total_data_size;
    metrics.total_uncompressed_size = inspect_result.state.total_uncompressed_size;

    drop(dataset);
    drop(store);
    metrics.print_results("Single Thread Benchmark");
}
