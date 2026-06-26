mod common;

use common::{BenchmarkMetrics, LogData, TimestampGenerator, create_temp_dir};
use rand::Rng;
use std::time::Instant;
use timslite::{Store, StoreConfig};

const WRITE_COUNT: u64 = 400_000;
const READ_SEQUENTIAL_COUNT: u64 = 300_000;
const READ_RANDOM_COUNT: u64 = 200_000;

fn main() {
    let log_data = LogData::load();
    let temp_dir = create_temp_dir();
    let data_dir = temp_dir.path().join("timslite_data");

    let config = StoreConfig::default();
    let mut store = Store::open(&data_dir, config).unwrap();

    let handle = store
        .create_dataset("bench_data", "logs", 64 * 1024 * 1024, 16 * 1024 * 1024, 6, 0, 0)
        .unwrap();

    let mut metrics = BenchmarkMetrics::new();
    let mut ts_gen = TimestampGenerator::new();

    let start = Instant::now();
    for _ in 0..WRITE_COUNT {
        let line = log_data.random_raw_line();
        let ts = ts_gen.next();
        store.write_dataset(handle, ts, line.as_bytes()).unwrap();
        metrics.write_bytes += line.len() as u64;
    }
    metrics.write_duration = start.elapsed();
    metrics.write_ops = WRITE_COUNT;

    let start = Instant::now();
    for i in 1..=READ_SEQUENTIAL_COUNT {
        if let Some((_, data)) = store.read_dataset(handle, i as i64).unwrap() {
            metrics.read_sequential_bytes += data.len() as u64;
        }
    }
    metrics.read_sequential_duration = start.elapsed();
    metrics.read_sequential_ops = READ_SEQUENTIAL_COUNT;

    let mut rng = rand::thread_rng();
    let start = Instant::now();
    for _ in 0..READ_RANDOM_COUNT {
        let ts = rng.gen_range(1..=WRITE_COUNT as i64);
        if let Some((_, data)) = store.read_dataset(handle, ts).unwrap() {
            metrics.read_random_bytes += data.len() as u64;
        }
    }
    metrics.read_random_duration = start.elapsed();
    metrics.read_random_ops = READ_RANDOM_COUNT;

    let inspect_result = store.inspect_dataset("bench_data", "logs").unwrap();
    metrics.total_data_size = inspect_result.state.total_data_size;
    metrics.total_uncompressed_size = inspect_result.state.total_uncompressed_size;

    drop(store);
    metrics.print_results("Single Thread Benchmark");
}