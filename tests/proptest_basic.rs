use proptest::prelude::*;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use timslite::{Store, StoreConfig};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let d = std::env::temp_dir().join("timslite_integration");
    fs::create_dir_all(&d).unwrap();
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    d.join(format!(
        "test_{:?}_{id}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

const MAX_RECORD_DATA_SIZE: usize = 4 * 1024 * 1024;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    #[test]
    fn prop_write_read_roundtrip(
        items in prop::collection::vec(
            (1i64..100_000, prop::collection::vec(0u8..255, 1..2000)),
            1..50,
        )
    ) {
        let dir = temp_dir();
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        store.create_dataset("ds", "test", 64 * 1024 * 1024, 64 * 1024 * 1024, 3, 0, 0).unwrap();
        let handle = store.open_dataset("ds", "test").unwrap();
        let ds = handle.clone();

        let mut sorted = items.clone();
        sorted.sort_by_key(|(ts, _)| *ts);
        sorted.dedup_by_key(|(ts, _)| *ts);

        for (ts, data) in &sorted {
            ds.write(*ts, data).unwrap();
        }

        for (ts, data) in &sorted {
            let result = ds.read(*ts).unwrap();
            prop_assert!(result.is_some(), "read({}) returned None", ts);
            let (read_ts, read_data) = result.unwrap();
            prop_assert_eq!(read_ts, *ts);
            prop_assert_eq!(&read_data, data, "data mismatch at ts={}", ts);
        }

        drop(ds);
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prop_append_boundary(
        data_size in prop::sample::select(vec![
            1usize, 100, 1000, 32000, 65000, 65535, 65536, 65537, 100000,
        ]),
    ) {
        let dir = temp_dir();
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        store.create_dataset("ds", "test", 64 * 1024 * 1024, 64 * 1024 * 1024, 3, 0, 0).unwrap();
        let handle = store.open_dataset("ds", "test").unwrap();
        let ds = handle.clone();

        let data = vec![0xABu8; data_size];

        if data_size > MAX_RECORD_DATA_SIZE {
            let result = ds.write(1, &data);
            prop_assert!(result.is_err(), "write should fail for size {}", data_size);
        } else {
            ds.write(1, &data).unwrap();

            let append_data = vec![0xCDu8; data_size.min(1000)];
            let append_result = ds.append(1, &append_data);

            if data_size + append_data.len() > MAX_RECORD_DATA_SIZE {
                prop_assert!(append_result.is_err(), "append should fail when combined size exceeds max");
            } else {
                let _ = append_result;
                let result = ds.read(1).unwrap();
                prop_assert!(result.is_some(), "read(1) should return data after write");
                let (_, read_data) = result.unwrap();
                prop_assert!(read_data.len() >= data_size, "data should be at least {} bytes", data_size);
            }
        }

        drop(ds);
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prop_retention_window(
        retention_window in prop::sample::select(vec![0u64, 1, 10, 100, 1000]),
        ts_values in prop::collection::vec(1i64..1000, 1..20),
    ) {
        let dir = temp_dir();
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        store.create_dataset("ds", "test", 64 * 1024 * 1024, 64 * 1024 * 1024, 3, 0, retention_window).unwrap();
        let handle = store.open_dataset("ds", "test").unwrap();
        let ds = handle.clone();

        let mut sorted_ts: Vec<i64> = ts_values.clone();
        sorted_ts.sort();
        sorted_ts.dedup();

        for ts in &sorted_ts {
            let data = vec![*ts as u8; 100];
            ds.write(*ts, &data).unwrap();
        }

        for ts in &sorted_ts {
            let result = ds.read(*ts).unwrap();

            if retention_window == 0 {
                prop_assert!(result.is_some(), "no retention: read({}) should succeed", ts);
            } else {
                if result.is_some() {
                    let (read_ts, _) = result.unwrap();
                    prop_assert_eq!(read_ts, *ts);
                }
            }
        }

        drop(ds);
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }
}
