mod common;

use common::{create_temp_dir, BenchmarkMetrics, LogData};
use rand::Rng;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use timslite::{Store, StoreConfig};

const WRITE_COUNT: u64 = 400_000;
const READ_SEQUENTIAL_COUNT: u64 = 300_000;
const READ_RANDOM_COUNT: u64 = 200_000;
const WRITE_THREADS: usize = 4;
const READ_THREADS: usize = 4;

fn main() {
    let log_data = Arc::new(LogData::load());
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
            1,
            0,
        )
        .unwrap();

    let mut metrics = BenchmarkMetrics::new();
    let ts_counter = Arc::new(AtomicI64::new(1));

    let start = Instant::now();
    let mut join_handles = vec![];

    for _ in 0..WRITE_THREADS {
        let dataset = dataset.clone();
        let log_data_clone = Arc::clone(&log_data);
        let ts_counter_clone = Arc::clone(&ts_counter);
        let writes_per_thread = WRITE_COUNT / WRITE_THREADS as u64;

        let jh = thread::spawn(move || {
            let mut bytes_written = 0u64;
            for _ in 0..writes_per_thread {
                let line = log_data_clone.random_raw_line();
                let ts = ts_counter_clone.fetch_add(1, Ordering::SeqCst);
                match dataset.write(ts, line.as_bytes()) {
                    Ok(_) => bytes_written += line.len() as u64,
                    Err(e) => eprintln!("Write error at ts {}: {:?}", ts, e),
                }
            }
            bytes_written
        });

        join_handles.push(jh);
    }

    for jh in join_handles {
        metrics.write_bytes += jh.join().unwrap();
    }

    metrics.write_duration = start.elapsed();
    metrics.write_ops = WRITE_COUNT;

    let total_written = metrics.write_ops as i64;

    let start = Instant::now();
    let mut join_handles = vec![];

    for i in 0..READ_THREADS {
        let dataset = dataset.clone();
        let reads_per_thread = READ_SEQUENTIAL_COUNT / READ_THREADS as u64;
        let start_ts = (i as i64 * reads_per_thread as i64) + 1;

        let jh = thread::spawn(move || {
            let mut bytes_read = 0u64;
            for ts in start_ts..start_ts + reads_per_thread as i64 {
                if let Some((_, data)) = dataset.read(ts).unwrap() {
                    bytes_read += data.len() as u64;
                }
            }
            bytes_read
        });

        join_handles.push(jh);
    }

    for jh in join_handles {
        metrics.read_sequential_bytes += jh.join().unwrap();
    }
    metrics.read_sequential_duration = start.elapsed();
    metrics.read_sequential_ops = READ_SEQUENTIAL_COUNT;

    let start = Instant::now();
    let mut join_handles = vec![];

    for _ in 0..READ_THREADS {
        let dataset = dataset.clone();
        let reads_per_thread = READ_RANDOM_COUNT / READ_THREADS as u64;

        let jh = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut bytes_read = 0u64;
            for _ in 0..reads_per_thread {
                let ts = rng.gen_range(1..=total_written);
                if let Some((_, data)) = dataset.read(ts).unwrap() {
                    bytes_read += data.len() as u64;
                }
            }
            bytes_read
        });

        join_handles.push(jh);
    }

    for jh in join_handles {
        metrics.read_random_bytes += jh.join().unwrap();
    }
    metrics.read_random_duration = start.elapsed();
    metrics.read_random_ops = READ_RANDOM_COUNT;

    let inspect_result = store.inspect_dataset("bench_data", "logs").unwrap();
    metrics.total_data_size = inspect_result.state.total_data_size;
    metrics.total_uncompressed_size = inspect_result.state.total_uncompressed_size;

    drop(dataset);
    drop(store);
    metrics.print_results(&format!(
        "Multi Thread Benchmark ({}W/{}R)",
        WRITE_THREADS, READ_THREADS
    ));
}
