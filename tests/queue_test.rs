//! Queue integration tests: push/poll/ack, consumer groups, persistence, threading.
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

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

use std::time::Duration;

#[test]
fn t27_1_1_basic_push_poll_ack() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();

    let ts = store.queue_push(&q, b"hello").unwrap();
    assert!(ts > 0);

    let (rts, data) = store
        .queue_poll(&c, Duration::from_millis(100))
        .unwrap()
        .unwrap();
    assert_eq!(rts, ts);
    assert_eq!(data, b"hello");

    store.queue_ack(&c, rts).unwrap();
    assert!(store
        .queue_poll(&c, Duration::from_millis(50))
        .unwrap()
        .is_none());
    store.close().unwrap();
}

#[test]
fn t27_1_2_multiple_pushes_sequential_poll() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();

    for i in 0..10i64 {
        let ts = store
            .queue_push(&q, &format!("msg_{}", i).into_bytes())
            .unwrap();
        assert_eq!(ts, i + 1);
    }

    for i in 0..10i64 {
        let (ts, data) = store
            .queue_poll(&c, Duration::from_millis(50))
            .unwrap()
            .unwrap();
        assert_eq!(ts, i + 1);
        assert_eq!(data, format!("msg_{}", i).as_bytes());
        store.queue_ack(&c, ts).unwrap();
    }

    assert!(store
        .queue_poll(&c, Duration::from_millis(50))
        .unwrap()
        .is_none());
    store.close().unwrap();
}

#[test]
fn t27_1_3_poll_timeout_empty_queue() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();

    let result = store.queue_poll(&c, Duration::from_millis(50)).unwrap();
    assert!(result.is_none());
    store.close().unwrap();
}

#[test]
fn t27_1_4_poll_skips_continuous_filler_gap() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    let h = store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 1, 0)
        .unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();

    // Write 3 records
    store.write_dataset(h, 10, b"first").unwrap();
    store.write_dataset(h, 20, b"second").unwrap();
    store.write_dataset(h, 30, b"third").unwrap();

    // Delete the middle record (creates a filler/gap)
    store.delete_dataset_record(h, 20).unwrap();

    // Poll should skip deleted ts=20 and return ts=10 first
    let (ts, data) = store
        .queue_poll(&c, Duration::from_millis(100))
        .unwrap()
        .unwrap();
    assert_eq!(ts, 10);
    assert_eq!(data, b"first");
    store.queue_ack(&c, ts).unwrap();

    // Poll should skip ts=20 (deleted/filler) and return ts=30
    let (ts, data) = store
        .queue_poll(&c, Duration::from_millis(100))
        .unwrap()
        .unwrap();
    assert_eq!(ts, 30);
    assert_eq!(data, b"third");
    store.queue_ack(&c, ts).unwrap();

    assert!(store
        .queue_poll(&c, Duration::from_millis(50))
        .unwrap()
        .is_none());
    store.close().unwrap();
}

#[test]
fn t27_1_5_sparse_gap_acked_progress_persists_after_reopen() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    {
        let mut store = Store::open(
            &dir,
            StoreConfig::builder()
                .enable_background_thread(false)
                .build(),
        )
        .unwrap();
        let h = store
            .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 1, 0)
            .unwrap();
        let q = store.open_queue(h).unwrap();
        let c = store.open_consumer(&q, "g1").unwrap();

        store.write_dataset(h, 10, b"first").unwrap();
        store.write_dataset(h, 20, b"second").unwrap();
        store.write_dataset(h, 30, b"third").unwrap();
        store.delete_dataset_record(h, 20).unwrap();

        let (ts, data) = store
            .queue_poll(&c, Duration::from_millis(100))
            .unwrap()
            .unwrap();
        assert_eq!(ts, 10);
        assert_eq!(data, b"first");
        store.queue_ack(&c, ts).unwrap();

        let (ts, data) = store
            .queue_poll(&c, Duration::from_millis(100))
            .unwrap()
            .unwrap();
        assert_eq!(ts, 30);
        assert_eq!(data, b"third");
        store.queue_ack(&c, ts).unwrap();

        store.close().unwrap();
    }
    {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        let h = store.open_dataset("t27q", "events").unwrap();
        let q = store.open_queue(h).unwrap();
        let c = store.open_consumer(&q, "g1").unwrap();

        assert!(store
            .queue_poll(&c, Duration::from_millis(50))
            .unwrap()
            .is_none());
        store.close().unwrap();
    }
}

#[test]
fn t27_2_1_multi_consumer_groups() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let ca = store.open_consumer(&q, "ga").unwrap();
    let cb = store.open_consumer(&q, "gb").unwrap();

    store.queue_push(&q, b"item1").unwrap();
    store.queue_push(&q, b"item2").unwrap();

    let (ts_a, data_a) = store
        .queue_poll(&ca, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(data_a, b"item1");
    store.queue_ack(&ca, ts_a).unwrap();

    let (ts_b, data_b) = store
        .queue_poll(&cb, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(data_b, b"item1");
    store.queue_ack(&cb, ts_b).unwrap();

    let (_, data_a2) = store
        .queue_poll(&ca, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(data_a2, b"item2");

    let (_, data_b2) = store
        .queue_poll(&cb, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(data_b2, b"item2");

    store.close().unwrap();
}

#[test]
fn t27_2_2_two_consumers_same_group() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c1 = store.open_consumer(&q, "shared").unwrap();
    let c2 = store.open_consumer(&q, "shared").unwrap();

    store.queue_push(&q, b"shared_item").unwrap();

    let (ts, _) = store
        .queue_poll(&c1, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(ts, 1);
    assert!(store
        .queue_poll(&c2, Duration::from_millis(10))
        .unwrap()
        .is_none());

    store.close().unwrap();
}

#[test]
fn t41_1_same_group_unexpired_pending_does_not_block_next_record() {
    use timslite::{QueueConsumerConfig, Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t41q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t41q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let config = QueueConsumerConfig::builder()
        .running_expired_seconds(60)
        .max_retry_count(3)
        .build()
        .unwrap();
    let c1 = q.open_consumer_with_config("shared", config).unwrap();
    let c2 = q.open_consumer_with_config("shared", config).unwrap();

    store.queue_push(&q, b"first").unwrap();
    store.queue_push(&q, b"second").unwrap();

    let (ts1, data1) = store
        .queue_poll(&c1, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(ts1, 1);
    assert_eq!(data1, b"first");

    let (ts2, data2) = store
        .queue_poll(&c2, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(ts2, 2);
    assert_eq!(data2, b"second");

    store.queue_ack(&c1, ts1).unwrap();
    store.queue_ack(&c2, ts2).unwrap();
    store.close().unwrap();
}

#[test]
fn t41_2_expired_pending_retries_once_then_drops_and_advances() {
    use timslite::{QueueConsumerConfig, Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t41q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t41q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let config = QueueConsumerConfig::builder()
        .running_expired_seconds(1)
        .max_retry_count(1)
        .build()
        .unwrap();
    let c = q.open_consumer_with_config("retry", config).unwrap();

    store.queue_push(&q, b"first").unwrap();
    store.queue_push(&q, b"second").unwrap();

    let (ts1, data1) = store
        .queue_poll(&c, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(ts1, 1);
    assert_eq!(data1, b"first");

    std::thread::sleep(Duration::from_millis(1100));
    let (retry_ts, retry_data) = store
        .queue_poll(&c, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(retry_ts, 1);
    assert_eq!(retry_data, b"first");

    std::thread::sleep(Duration::from_millis(1100));
    let (next_ts, next_data) = store
        .queue_poll(&c, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(next_ts, 2);
    assert_eq!(next_data, b"second");

    store.queue_ack(&c, next_ts).unwrap();
    store.close().unwrap();
}

#[test]
fn t41_3_same_group_rejects_config_mismatch() {
    use timslite::{QueueConsumerConfig, Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t41q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t41q", "events").unwrap();
    let q = store.open_queue(h).unwrap();

    q.open_consumer("shared").unwrap();
    let mismatched = QueueConsumerConfig::builder()
        .running_expired_seconds(30)
        .max_retry_count(3)
        .build()
        .unwrap();
    assert!(q.open_consumer_with_config("shared", mismatched).is_err());

    store.close().unwrap();
}

#[test]
fn t27_2_3_open_consumer_creates_group() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "new_group").unwrap();
    store.queue_push(&q, b"test").unwrap();
    let result = store.queue_poll(&c, Duration::from_millis(100)).unwrap();
    assert!(result.is_some());
    store.close().unwrap();
}

#[test]
fn t27_2_4_consumer_group_name_must_be_path_safe() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();

    for group_name in [
        "",
        ".",
        "..",
        "../bad",
        "bad/name",
        "bad\\name",
        "bad name",
        "中文",
    ] {
        assert!(
            store.open_consumer(&q, group_name).is_err(),
            "group name {group_name:?} must be rejected"
        );
    }
    let too_long_group = "a".repeat(256);
    assert!(
        store.open_consumer(&q, &too_long_group).is_err(),
        "group name longer than 255 bytes must be rejected"
    );

    store.open_consumer(&q, "A-z_09").unwrap();
    store.close().unwrap();
}

#[test]
fn t27_3_1_open_queue_twice_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    store.open_queue(h).unwrap();
    assert!(store.open_queue(h).is_err());
    store.close().unwrap();
}

#[test]
fn t27_3_2_push_to_closed_queue_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    store.close_queue(h).unwrap();
    assert!(store.queue_push(&q, b"test").is_err());
    store.close().unwrap();
}

#[test]
fn t27_3_3_poll_after_close_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();
    store.close_queue(h).unwrap();
    assert!(store.queue_poll(&c, Duration::from_millis(50)).is_err());
    store.close().unwrap();
}

#[test]
fn t27_3_4_ack_nonexistent_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();
    assert!(store.queue_ack(&c, 99999).is_err());
    store.close().unwrap();
}

#[test]
fn t27_3_5_drop_nonexistent_consumer_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    assert!(store.drop_consumer(&q, "no_such").is_err());
    store.close().unwrap();
}

#[test]
fn t27_3_5_open_consumer_on_closed_queue_errors() {
    use timslite::{Store, StoreConfig, TmslError};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    store.close_queue(h).unwrap();
    let err = store.open_consumer(&q, "g1").unwrap_err();
    assert!(matches!(err, TmslError::QueueClosed(_)));
    store.close().unwrap();
}

#[test]
fn t27_3_6_drop_nonexistent_consumer_group_errors() {
    use timslite::{Store, StoreConfig, TmslError};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let err = store.drop_consumer(&q, "no_such_group").unwrap_err();
    assert!(matches!(err, TmslError::ConsumerGroupNotFound(_)));
    store.close().unwrap();
}

#[test]
fn t27_4_1_pending_survives_reopen() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        store
            .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
            .unwrap();
        let h = store.open_dataset("t27q", "events").unwrap();
        let q = store.open_queue(h).unwrap();
        let c = store.open_consumer(&q, "g1").unwrap();

        store.queue_push(&q, b"a").unwrap();
        store.queue_push(&q, b"b").unwrap();
        store.queue_push(&q, b"c").unwrap();

        let (ts1, _) = store
            .queue_poll(&c, Duration::from_millis(50))
            .unwrap()
            .unwrap();
        store.queue_ack(&c, ts1).unwrap();

        let (ts2, _) = store
            .queue_poll(&c, Duration::from_millis(50))
            .unwrap()
            .unwrap();
        assert_eq!(ts2, 2);
        store.close().unwrap();
    }
    {
        let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
        let h = store.open_dataset("t27q", "events").unwrap();
        let q = store.open_queue(h).unwrap();
        let c = store.open_consumer(&q, "g1").unwrap();

        let (ts, data) = store
            .queue_poll(&c, Duration::from_millis(50))
            .unwrap()
            .unwrap();
        assert_eq!(ts, 2);
        assert_eq!(data, b"b");
        store.queue_ack(&c, ts).unwrap();

        let (ts3, data3) = store
            .queue_poll(&c, Duration::from_millis(50))
            .unwrap()
            .unwrap();
        assert_eq!(ts3, 3);
        assert_eq!(data3, b"c");

        store.close().unwrap();
    }
}

#[test]
fn t27_4_2_drop_and_recreate_consumer() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    store.open_consumer(&q, "temp").unwrap();
    store.drop_consumer(&q, "temp").unwrap();
    store.open_consumer(&q, "temp").unwrap();
    store.close().unwrap();
}

#[test]
fn t27_4_3_acked_progress_persists() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();
    for i in 0..3 {
        let ts = store
            .queue_push(&q, format!("item{}", i).as_bytes())
            .unwrap();
        store
            .queue_poll(&c, Duration::from_millis(100))
            .unwrap()
            .unwrap();
        store.queue_ack(&c, ts).unwrap();
    }
    store.close().unwrap();
    let mut store2 = Store::open(&dir, StoreConfig::default()).unwrap();
    let h2 = store2.open_dataset("t27q", "events").unwrap();
    let q2 = store2.open_queue(h2).unwrap();
    let c2 = store2.open_consumer(&q2, "g1").unwrap();
    assert!(store2
        .queue_poll(&c2, Duration::from_millis(50))
        .unwrap()
        .is_none());
    store2.close().unwrap();
}

#[test]
fn t27_5_1_producer_consumer_threads() {
    use std::sync::{Arc, Barrier};
    use std::thread;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = Arc::new(store.open_queue(h).unwrap());
    let q_prod = q.clone();
    let q_cons = q.clone();
    let barrier = Arc::new(Barrier::new(2));
    let b_prod = barrier.clone();
    let b_cons = barrier.clone();
    let dir2 = dir.clone();

    let producer = thread::spawn(move || {
        b_prod.wait();
        for i in 0..10i64 {
            q_prod.push(format!("p_{}", i).as_bytes()).unwrap();
            thread::sleep(Duration::from_millis(1));
        }
    });

    let consumer = thread::spawn(move || {
        let mut store2 = Store::open(&dir2, StoreConfig::default()).unwrap();
        let _h2 = store2.open_dataset("t27q", "events").unwrap();
        let c = store2.open_consumer(&q_cons, "workers").unwrap();
        b_cons.wait();

        let mut count = 0;
        for _ in 0..10 {
            if let Some((ts, _)) = store2.queue_poll(&c, Duration::from_secs(5)).unwrap() {
                store2.queue_ack(&c, ts).unwrap();
                count += 1;
            }
        }
        assert_eq!(count, 10);
        store2.close().unwrap();
    });

    producer.join().unwrap();
    consumer.join().unwrap();
    store.close().unwrap();
}

#[test]
fn t27_5_2_multiple_producers() {
    use std::sync::{Arc, Barrier};
    use std::thread;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = Arc::new(store.open_queue(h).unwrap());
    let qc = store.open_consumer(&q, "g1").unwrap();
    let q1 = q.clone();
    let q2 = q.clone();
    let q3 = q.clone();
    let cc = qc.clone();
    let barrier = Arc::new(Barrier::new(4));
    let mut prod_handles = Vec::new();
    for (i, q_ref) in [q1, q2, q3].into_iter().enumerate() {
        let b = Arc::clone(&barrier);
        prod_handles.push(thread::spawn(move || {
            b.wait();
            for j in 0..7 {
                q_ref.push(format!("p{}_{}", i, j).as_bytes()).unwrap();
            }
        }));
    }
    let consumer = thread::spawn(move || {
        barrier.wait();
        thread::sleep(Duration::from_millis(20));
        let mut count = 0;
        for _ in 0..21 {
            match cc.poll(Duration::from_secs(10)) {
                Ok(Some((ts, _data))) => {
                    cc.ack(ts).unwrap();
                    count += 1;
                }
                Ok(None) => panic!("timeout"),
                Err(e) => panic!("err: {:?}", e),
            }
        }
        assert_eq!(count, 21);
    });
    for ph in prod_handles {
        ph.join().unwrap();
    }
    consumer.join().unwrap();
    store.close().unwrap();
}

#[test]
fn t27_5_3_multiple_consumers_different_groups() {
    use std::sync::{Arc, Barrier};
    use std::thread;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let ca = store.open_consumer(&q, "group_a").unwrap();
    let cb = store.open_consumer(&q, "group_b").unwrap();
    for i in 0..5 {
        store
            .queue_push(&q, format!("msg_{}", i).as_bytes())
            .unwrap();
    }
    let ca_clone = ca.clone();
    let cb_clone = cb.clone();
    let barrier = Arc::new(Barrier::new(2));
    let b1 = Arc::clone(&barrier);
    let b2 = Arc::clone(&barrier);
    let t1 = thread::spawn(move || {
        b1.wait();
        let mut count = 0;
        for _ in 0..5 {
            let (ts, _) = ca_clone.poll(Duration::from_secs(5)).unwrap().unwrap();
            ca_clone.ack(ts).unwrap();
            count += 1;
        }
        count
    });
    let t2 = thread::spawn(move || {
        b2.wait();
        let mut count = 0;
        for _ in 0..5 {
            let (ts, _) = cb_clone.poll(Duration::from_secs(5)).unwrap().unwrap();
            cb_clone.ack(ts).unwrap();
            count += 1;
        }
        count
    });
    assert_eq!(t1.join().unwrap(), 5);
    assert_eq!(t2.join().unwrap(), 5);
    store.close().unwrap();
}

#[test]
fn t27_6_1_store_invalid_handle_errors() {
    use timslite::{DataSetHandle, Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    assert!(store.open_queue(DataSetHandle(99999)).is_err());
    store.close().unwrap();
}

#[test]
fn t27_6_2_store_close_queue() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    store.queue_push(&q, b"x").unwrap();
    store.close_queue(h).unwrap();
    assert!(store.queue_push(&q, b"y").is_err());
    store.close().unwrap();
}

#[test]
fn t27_6_1_store_open_queue_valid() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();
    store.queue_push(&q, b"data").unwrap();
    assert!(store
        .queue_poll(&c, Duration::from_millis(100))
        .unwrap()
        .is_some());
    store.close().unwrap();
}

#[test]
fn t27_6_4_store_open_consumer_and_drop_consumer() {
    use timslite::{Store, StoreConfig, TmslError};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();
    store.queue_push(&q, b"test").unwrap();
    assert!(store
        .queue_poll(&c, Duration::from_millis(100))
        .unwrap()
        .is_some());
    store.drop_consumer(&q, "g1").unwrap();
    let err = store.drop_consumer(&q, "g1").unwrap_err();
    assert!(matches!(err, TmslError::ConsumerGroupNotFound(_)));
    store.close().unwrap();
}

#[test]
fn t27_7_push_notification_wakes_blocking_consumer_poll() {
    // End-to-end: consumer is blocked in poll() when push() fires.
    // The Condvar notification in notify_queue() should wake the consumer
    // well before the poll timeout expires.
    use std::thread;
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let config = StoreConfig::builder()
        .enable_background_thread(false)
        .build();
    let mut store = Store::open(&dir, config).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();

    // Push from a background thread after a short delay
    let q_clone = q.clone();
    let producer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        q_clone.push(b"wake_up_data").unwrap();
    });

    // Poll with a 5-second timeout; should return quickly via Condvar wake
    let start = std::time::Instant::now();
    let result = c.poll(Duration::from_secs(5)).unwrap();
    let elapsed = start.elapsed();

    assert!(
        result.is_some(),
        "poll should return data after push notification"
    );
    let (ts, data) = result.unwrap();
    assert_eq!(data, b"wake_up_data");
    assert!(ts > 0);
    // Condvar wake should be well under the 5s timeout
    assert!(
        elapsed < Duration::from_secs(3),
        "poll took {:?}, expected < 3s (Condvar wake-up, not timeout)",
        elapsed
    );

    producer.join().unwrap();
    store.close().unwrap();
}

// ─── Queue boundary tests (P0-Q-1~7) ────────────────────────────────────────

#[test]
fn t27_7_1_push_to_closed_queue_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();

    // Push works before close
    store.queue_push(&q, b"before_close").unwrap();

    // Close the queue
    store.close_queue(h).unwrap();

    // Push after close should fail
    let result = store.queue_push(&q, b"after_close");
    assert!(result.is_err(), "push to closed queue should return error");

    store.close().unwrap();
}

#[test]
fn t27_7_2_poll_closed_consumer_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();

    // Push some data first
    store.queue_push(&q, b"data").unwrap();

    // Close the queue
    store.close_queue(h).unwrap();

    // Poll after close should fail
    let result = store.queue_poll(&c, Duration::from_millis(50));
    assert!(result.is_err(), "poll on closed queue should return error");

    store.close().unwrap();
}

#[test]
fn t27_7_3_ack_closed_consumer_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();

    // Push and poll to get a timestamp
    let ts = store.queue_push(&q, b"data").unwrap();
    let polled = store
        .queue_poll(&c, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(polled.0, ts);

    // Close the queue
    store.close_queue(h).unwrap();

    // Ack after close should fail
    let result = store.queue_ack(&c, ts);
    assert!(result.is_err(), "ack on closed queue should return error");

    store.close().unwrap();
}

#[test]
fn t27_7_4_drop_consumer_twice_errors() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();

    // Open a consumer group
    store.open_consumer(&q, "g1").unwrap();

    // First drop should succeed
    store.drop_consumer(&q, "g1").unwrap();

    // Second drop should fail
    let result = store.drop_consumer(&q, "g1");
    assert!(result.is_err(), "drop consumer twice should return error");

    store.close().unwrap();
}

#[test]
fn t27_7_5_poll_timeout_precision() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();

    // Poll with 200ms timeout on empty queue
    let start = std::time::Instant::now();
    let result = store.queue_poll(&c, Duration::from_millis(200)).unwrap();
    let elapsed = start.elapsed();

    assert!(result.is_none(), "empty queue should return None");

    // Timeout should be within reasonable range (200ms ± 100ms)
    assert!(
        elapsed >= Duration::from_millis(150) && elapsed < Duration::from_millis(500),
        "timeout precision: expected ~200ms, got {:?}",
        elapsed
    );

    store.close().unwrap();
}

#[test]
fn t27_7_6_push_empty_data() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();
    let c = store.open_consumer(&q, "g1").unwrap();

    // Push empty data - should succeed (empty message is valid)
    let ts = store.queue_push(&q, b"").unwrap();
    assert!(ts > 0);

    // Poll should return the empty data
    let (rts, data) = store
        .queue_poll(&c, Duration::from_millis(50))
        .unwrap()
        .unwrap();
    assert_eq!(rts, ts);
    assert!(
        data.is_empty(),
        "empty push should result in empty poll data"
    );

    store.close().unwrap();
}

#[test]
fn t27_7_7_consumer_group_name_boundary() {
    use timslite::{Store, StoreConfig};

    let dir = temp_dir();
    let mut store = Store::open(&dir, StoreConfig::default()).unwrap();
    store
        .create_dataset("t27q", "events", 64 * 1024 * 1024, 4 * 1024 * 1024, 6, 0, 0)
        .unwrap();
    let h = store.open_dataset("t27q", "events").unwrap();
    let q = store.open_queue(h).unwrap();

    // Valid: long name (within PATH_COMPONENT_MAX_LEN = 255)
    let long_name = "a".repeat(200);
    store.open_consumer(&q, &long_name).unwrap();

    // Valid: name with hyphens and underscores
    store.open_consumer(&q, "my-group_1").unwrap();

    // Valid: numeric name
    store.open_consumer(&q, "12345").unwrap();

    // Invalid: empty name
    let result = store.open_consumer(&q, "");
    assert!(result.is_err(), "empty consumer group name should fail");

    // Invalid: name with spaces
    let result = store.open_consumer(&q, "has space");
    assert!(
        result.is_err(),
        "consumer group name with space should fail"
    );

    // Invalid: name with special characters
    let result = store.open_consumer(&q, "group/name");
    assert!(
        result.is_err(),
        "consumer group name with slash should fail"
    );

    store.close().unwrap();
}
