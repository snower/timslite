use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::{Result, TmslError};
use crate::journal::log::JournalLog;
use crate::queue::{
    now_unix_seconds, ConsumerStateFile, PendingEntry, QueueConsumerConfig, QueueNotifier,
    QueuePollCallback, QueuePollCallbackSlot, PENDING_STATUS_UNACKED,
};
use crate::util::{is_path_safe_component, PATH_COMPONENT_MAX_LEN};

fn validate_consumer_group_name(group_name: &str) -> Result<()> {
    if is_path_safe_component(group_name) {
        Ok(())
    } else {
        Err(TmslError::InvalidData(format!(
            "journal queue consumer group_name must match ^[0-9A-Za-z_-]+$ and be at most {PATH_COMPONENT_MAX_LEN} bytes"
        )))
    }
}

struct JournalQueueInner {
    consumers: HashMap<String, Arc<Mutex<ConsumerStateFile>>>,
    consumer_configs: HashMap<String, QueueConsumerConfig>,
    closed: Arc<AtomicBool>,
}

impl JournalQueueInner {
    fn new() -> Self {
        Self {
            consumers: HashMap::new(),
            consumer_configs: HashMap::new(),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone)]
pub struct JournalQueue {
    log: Arc<Mutex<JournalLog>>,
    queue_dir: PathBuf,
    inner: Arc<Mutex<JournalQueueInner>>,
    notify: Arc<QueueNotifier>,
}

impl JournalQueue {
    pub(crate) fn new(log: Arc<Mutex<JournalLog>>, queue_dir: PathBuf) -> Self {
        Self {
            log,
            queue_dir,
            inner: Arc::new(Mutex::new(JournalQueueInner::new())),
            notify: Arc::new(QueueNotifier::new()),
        }
    }

    pub fn open_consumer(&self, group_name: &str) -> Result<JournalQueueConsumer> {
        self.open_consumer_with_config(group_name, QueueConsumerConfig::default())
    }

    pub fn open_consumer_with_config(
        &self,
        group_name: &str,
        config: QueueConsumerConfig,
    ) -> Result<JournalQueueConsumer> {
        validate_consumer_group_name(group_name)?;
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("journal queue mutex poisoned".into()))?;
        if inner.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("journal queue is closed".into()));
        }
        if let Some(existing) = inner.consumer_configs.get(group_name) {
            if *existing != config {
                return Err(TmslError::InvalidData(format!(
                    "journal consumer group {group_name} is already open with a different config"
                )));
            }
        }
        let state_file = if let Some(existing) = inner.consumers.get(group_name) {
            Arc::clone(existing)
        } else {
            let initial_sequence = self
                .log
                .lock()
                .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
                .latest_sequence()
                .unwrap_or(0);
            let state_path = self.queue_dir.join(group_name);
            let state = Arc::new(Mutex::new(ConsumerStateFile::open_or_create(
                state_path,
                initial_sequence,
            )?));
            inner
                .consumers
                .insert(group_name.to_string(), Arc::clone(&state));
            inner
                .consumer_configs
                .insert(group_name.to_string(), config);
            state
        };
        let callback_slot = Arc::new(Mutex::new(None));
        self.notify.register_callback_slot(&callback_slot)?;

        Ok(JournalQueueConsumer {
            state_file,
            config,
            log: Arc::clone(&self.log),
            notify: Arc::clone(&self.notify),
            closed: Arc::clone(&inner.closed),
            poll_callback: callback_slot,
        })
    }

    pub fn close(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("journal queue mutex poisoned".into()))?;
        if inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        for state in inner.consumers.values() {
            if let Ok(mut guard) = state.lock() {
                let _ = guard.sync();
                let _ = guard.flush();
            }
        }
        inner.consumers.clear();
        inner.consumer_configs.clear();
        drop(inner);
        self.notify_waiters();
        Ok(())
    }

    pub(crate) fn notify_record_appended(&self) {
        let _ = self.notify.notify_data_available();
    }

    fn notify_waiters(&self) {
        let _ = self.notify.notify_waiters();
    }

    pub(crate) fn flush_state_files(&self) -> Result<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("journal queue mutex poisoned".into()))?;
        for state in inner.consumers.values() {
            let mut guard = state
                .lock()
                .map_err(|_| TmslError::InvalidData("journal state file mutex poisoned".into()))?;
            guard.sync()?;
            guard.flush()?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct JournalQueueConsumer {
    state_file: Arc<Mutex<ConsumerStateFile>>,
    config: QueueConsumerConfig,
    log: Arc<Mutex<JournalLog>>,
    notify: Arc<QueueNotifier>,
    closed: Arc<AtomicBool>,
    poll_callback: QueuePollCallbackSlot,
}

impl JournalQueueConsumer {
    pub fn poll(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("journal queue is closed".into()));
        }
        if let Some(row) = self.try_poll_available()? {
            return Ok(Some(row));
        }
        if timeout.is_zero() {
            return Ok(None);
        }
        let deadline = Instant::now() + timeout;
        let (lock, cvar) = self.notify.wait_parts();
        let mut notified = lock
            .lock()
            .map_err(|_| TmslError::InvalidData("journal notify mutex poisoned".into()))?;
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return Err(TmslError::QueueClosed("journal queue is closed".into()));
            }
            if *notified {
                *notified = false;
                drop(notified);
                if let Some(row) = self.try_poll_available()? {
                    return Ok(Some(row));
                }
                notified = lock
                    .lock()
                    .map_err(|_| TmslError::InvalidData("journal notify mutex poisoned".into()))?;
                continue;
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                drop(notified);
                return self.try_poll_available();
            };
            let (guard, wait_result) = cvar.wait_timeout(notified, remaining).unwrap();
            notified = guard;
            if wait_result.timed_out() {
                drop(notified);
                return self.try_poll_available();
            }
        }
    }

    pub fn ack(&self, sequence: i64) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("journal queue is closed".into()));
        }
        let mut state = self
            .state_file
            .lock()
            .map_err(|_| TmslError::InvalidData("journal state file mutex poisoned".into()))?;
        state.ack_pending(sequence)?;
        state.cleanup_acked();
        state.sync()?;
        Ok(())
    }

    pub fn poll_callback(&self, callback: Option<QueuePollCallback>) -> Result<()> {
        let mut slot = self.poll_callback.lock().map_err(|_| {
            TmslError::InvalidData("journal queue callback slot mutex poisoned".into())
        })?;
        if callback.is_some() && slot.is_some() {
            return Err(TmslError::InvalidData(
                "journal queue poll callback is already registered".into(),
            ));
        }
        *slot = callback;
        Ok(())
    }

    fn try_poll_available(&self) -> Result<Option<(i64, Vec<u8>)>> {
        let retry_scan = {
            let mut state = self
                .state_file
                .lock()
                .map_err(|_| TmslError::InvalidData("journal state file mutex poisoned".into()))?;
            let scan = state.take_retryable_pending(self.config, now_unix_seconds());
            if scan.changed {
                state.sync()?;
            }
            scan
        };
        if let Some(sequence) = retry_scan.timestamp {
            return self.read_sequence(sequence);
        }

        let next_sequence = self
            .log
            .lock()
            .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
            .next_sequence();

        let next = {
            let state = self
                .state_file
                .lock()
                .map_err(|_| TmslError::InvalidData("journal state file mutex poisoned".into()))?;
            let mut candidate = state.next_poll_ts();
            while candidate < next_sequence && state.is_in_pending(candidate) {
                candidate = candidate.saturating_add(1);
            }
            candidate
        };
        if next >= next_sequence {
            return Ok(None);
        }

        let row = self.read_sequence(next)?;
        if row.is_some() {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let mut state = self
                .state_file
                .lock()
                .map_err(|_| TmslError::InvalidData("journal state file mutex poisoned".into()))?;
            if !state.is_in_pending(next) {
                state.add_pending(PendingEntry {
                    timestamp: next,
                    start_time: now,
                    status: PENDING_STATUS_UNACKED,
                    retry_count: 0,
                })?;
                state.sync()?;
            }
        }
        Ok(row)
    }

    fn read_sequence(&self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>> {
        self.log
            .lock()
            .map_err(|_| TmslError::InvalidData("journal log mutex poisoned".into()))?
            .read(sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StoreConfig;
    use crate::journal::log::JournalLog;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "timslite_journal_queue_{name}_{:?}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_log(name: &str) -> (Arc<Mutex<JournalLog>>, std::path::PathBuf) {
        let dir = temp_dir(name);
        let config = StoreConfig {
            data_segment_size: 4096,
            initial_data_segment_size: 4096,
            ..StoreConfig::default()
        };
        let log = JournalLog::open_or_create(dir.clone(), &config).unwrap();
        (Arc::new(Mutex::new(log)), dir)
    }

    #[test]
    fn journal_queue_new_consumer_starts_after_current_latest() {
        let (log, dir) = make_log("starts_after_latest");
        log.lock().unwrap().append(b"existing").unwrap();
        let queue = JournalQueue::new(log.clone(), dir.join("queue"));
        let consumer = queue.open_consumer("group1").unwrap();

        assert!(consumer.poll(Duration::from_millis(1)).unwrap().is_none());

        let seq = log.lock().unwrap().append(b"next").unwrap();
        queue.notify_record_appended();
        let row = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
        assert_eq!(row.0, seq);
        assert_eq!(row.1, b"next");
    }

    #[test]
    fn journal_queue_polls_realtime_sequence_one_based() {
        let (log, dir) = make_log("one_based");
        let queue = JournalQueue::new(log.clone(), dir.join("queue"));
        let consumer = queue.open_consumer("group1").unwrap();

        assert_eq!(log.lock().unwrap().append(b"first").unwrap(), 1);
        queue.notify_record_appended();

        let row = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
        assert_eq!(row.0, 1);
        assert_eq!(row.1, b"first");
    }

    #[test]
    fn journal_queue_ack_persists_processed_sequence() {
        let (log, dir) = make_log("ack_persists");
        let queue_dir = dir.join("queue");
        let queue = JournalQueue::new(log.clone(), queue_dir.clone());
        let consumer = queue.open_consumer("group1").unwrap();
        log.lock().unwrap().append(b"first").unwrap();
        queue.notify_record_appended();
        let row = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
        consumer.ack(row.0).unwrap();
        queue.flush_state_files().unwrap();
        drop(consumer);
        drop(queue);

        let queue = JournalQueue::new(log, queue_dir);
        let consumer = queue.open_consumer("group1").unwrap();

        assert!(consumer.poll(Duration::from_millis(1)).unwrap().is_none());
    }
}
