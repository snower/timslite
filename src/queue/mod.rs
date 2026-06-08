//! Queue module: Dataset queue semantics with consumer groups.
//!
//! Provides push/poll/ack queue semantics on top of Dataset, with:
//! - Multi-consumer-group support (independent progress)
//! - Multi-consumer-instance per group (shared progress, exclusive poll)
//! - Persistent 4KB mmap state files (QSTF magic)
//! - Condvar-based wait/notify for poll
//!
//! Directory layout:
//! ```text
//! {data_dir}/{name}/{type}/queue/{group_name}
//! ```

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use memmap2::MmapMut;

use crate::dataset::DataSet;
use crate::error::{Result, TmslError};
use crate::util::{
    is_path_safe_component, read_i64_from_mmap, read_u16_from_mmap, read_u32_from_mmap,
    PATH_COMPONENT_MAX_LEN,
};

/// Queue state file magic bytes.
pub const QUEUE_STATE_MAGIC: &[u8; 4] = b"QSTF";

/// Queue state file version.
pub const QUEUE_STATE_VERSION: u32 = 1;

/// Fixed state file size (4KB).
pub const STATE_FILE_SIZE: usize = 4096;

/// Size of a single pending entry in the state file.
pub const PENDING_ENTRY_SIZE: usize = 17;

/// Maximum number of pending entries that fit in 4KB.
pub const MAX_PENDING_ENTRIES: usize = 239;

/// Status: pending (not yet acked).
pub const PENDING_STATUS_UNACKED: u8 = 0;

/// Status: acked.
pub const PENDING_STATUS_ACKED: u8 = 1;

/// Header size: magic(4) + version(4) + state_length(2) + processed_ts(8) + pending_length(2) + pending_value_size(1) = 21
pub const STATE_HEADER_SIZE: usize = 21;

/// Default pending timeout in seconds (5 minutes).
pub const DEFAULT_PENDING_TIMEOUT_SECS: u64 = 300;

fn validate_consumer_group_name(group_name: &str) -> Result<()> {
    if is_path_safe_component(group_name) {
        Ok(())
    } else {
        Err(TmslError::InvalidData(format!(
            "queue consumer group_name must match ^[0-9A-Za-z_-]+$ and be at most {PATH_COMPONENT_MAX_LEN} bytes"
        )))
    }
}

/// A single pending entry tracked in the consumer state file.
#[derive(Clone, Debug)]
pub struct PendingEntry {
    pub timestamp: i64,
    pub start_time: i64,
    pub status: u8,
}

impl PendingEntry {
    fn serialize_to(&self, buf: &mut [u8; PENDING_ENTRY_SIZE]) {
        buf[0..8].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[8..16].copy_from_slice(&self.start_time.to_le_bytes());
        buf[16] = self.status;
    }

    fn deserialize_from(buf: &[u8; PENDING_ENTRY_SIZE]) -> Self {
        PendingEntry {
            timestamp: i64::from_le_bytes(buf[0..8].try_into().unwrap()),
            start_time: i64::from_le_bytes(buf[8..16].try_into().unwrap()),
            status: buf[16],
        }
    }
}

/// 4KB mmap-backed state file for a single consumer group.
///
/// Layout:
/// - magic (4B) + version (4B) + state_length (2B) + processed_ts (8B)
/// - pending_length (2B) + pending_value_size (1B)
/// - pending_entries (pending_length * 17 B)
pub struct ConsumerStateFile {
    path: PathBuf,
    mmap: MmapMut,
    processed_ts: i64,
    pending_entries: Vec<PendingEntry>,
}

impl ConsumerStateFile {
    /// Open existing state file or create a new one.
    pub fn open_or_create(path: PathBuf, initial_processed_ts: i64) -> Result<Self> {
        if path.exists() {
            Self::open_existing(path)
        } else {
            Self::create_new(path, initial_processed_ts)
        }
    }

    fn create_new(path: PathBuf, initial_processed_ts: i64) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(STATE_FILE_SIZE as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        // Write header
        mmap[0..4].copy_from_slice(QUEUE_STATE_MAGIC);
        mmap[4..8].copy_from_slice(&QUEUE_STATE_VERSION.to_le_bytes());
        // state_length = 8 (processed_ts is 8 bytes)
        mmap[8..10].copy_from_slice(&8u16.to_le_bytes());
        write_i64_at(&mut mmap, 10, initial_processed_ts);
        // pending_length = 0
        mmap[18..20].copy_from_slice(&0u16.to_le_bytes());
        // pending_value_size = 17
        mmap[20] = PENDING_ENTRY_SIZE as u8;

        mmap.flush()?;

        Ok(ConsumerStateFile {
            path,
            mmap,
            processed_ts: initial_processed_ts,
            pending_entries: Vec::new(),
        })
    }

    fn open_existing(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        if &mmap[0..4] != QUEUE_STATE_MAGIC {
            return Err(TmslError::InvalidMagic);
        }
        let version = read_u32_from_mmap(&mmap, 4);
        if version != QUEUE_STATE_VERSION {
            return Err(TmslError::InvalidVersion(version as u16));
        }

        let processed_ts = read_i64_from_mmap(&mmap, 10);
        let pending_length = read_u16_from_mmap(&mmap, 18) as usize;

        let mut pending_entries = Vec::with_capacity(pending_length);
        for i in 0..pending_length {
            let offset = STATE_HEADER_SIZE + i * PENDING_ENTRY_SIZE;
            if offset + PENDING_ENTRY_SIZE > STATE_FILE_SIZE {
                return Err(TmslError::InvalidData(format!(
                    "pending entry {i} out of bounds (offset={offset})"
                )));
            }
            let mut buf = [0u8; PENDING_ENTRY_SIZE];
            buf.copy_from_slice(&mmap[offset..offset + PENDING_ENTRY_SIZE]);
            pending_entries.push(PendingEntry::deserialize_from(&buf));
        }

        Ok(ConsumerStateFile {
            path,
            mmap,
            processed_ts,
            pending_entries,
        })
    }

    /// Write in-memory state back to mmap (no fsync 鈥?unified with bg flush).
    pub fn sync_to_mmap(&mut self) -> Result<()> {
        // Update header processed_ts
        write_i64_at(&mut self.mmap, 10, self.processed_ts);
        // Update pending_length
        let len = self.pending_entries.len() as u16;
        self.mmap[18..20].copy_from_slice(&len.to_le_bytes());
        // Write pending entries
        for (i, entry) in self.pending_entries.iter().enumerate() {
            let offset = STATE_HEADER_SIZE + i * PENDING_ENTRY_SIZE;
            if offset + PENDING_ENTRY_SIZE > STATE_FILE_SIZE {
                return Err(TmslError::PendingFull(format!("{}", self.path.display())));
            }
            let mut buf = [0u8; PENDING_ENTRY_SIZE];
            entry.serialize_to(&mut buf);
            self.mmap[offset..offset + PENDING_ENTRY_SIZE].copy_from_slice(&buf);
        }
        // Zero out unused pending area
        let used_end = STATE_HEADER_SIZE + self.pending_entries.len() * PENDING_ENTRY_SIZE;
        let remaining = STATE_FILE_SIZE - used_end;
        if remaining > 0 {
            let zeros = vec![0u8; remaining];
            self.mmap[used_end..STATE_FILE_SIZE].copy_from_slice(&zeros);
        }
        Ok(())
    }

    /// Flush mmap to disk (MS_SYNC) 鈥?called by background flush task.
    pub fn flush(&self) -> Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Add a pending entry. Returns error if capacity reached.
    pub fn add_pending(&mut self, entry: PendingEntry) -> Result<()> {
        if self.pending_entries.len() >= MAX_PENDING_ENTRIES {
            return Err(TmslError::PendingFull(format!(
                "{} ({} entries)",
                self.path.display(),
                self.pending_entries.len()
            )));
        }
        self.pending_entries.push(entry);
        Ok(())
    }

    /// Find pending entry by timestamp.
    pub fn find_pending(&self, timestamp: i64) -> Option<&PendingEntry> {
        self.pending_entries
            .iter()
            .find(|e| e.timestamp == timestamp)
    }

    /// Check if timestamp is in pending (any status).
    pub fn is_in_pending(&self, timestamp: i64) -> bool {
        self.pending_entries
            .iter()
            .any(|e| e.timestamp == timestamp)
    }

    /// Find the first unacked pending entry (for poll allocation).
    pub fn find_first_unacked(&self) -> Option<&PendingEntry> {
        self.pending_entries
            .iter()
            .find(|e| e.status == PENDING_STATUS_UNACKED)
    }

    /// Mark a pending entry as acked by timestamp.
    pub fn ack_pending(&mut self, timestamp: i64) -> Result<()> {
        let entry = self
            .pending_entries
            .iter_mut()
            .find(|e| e.timestamp == timestamp)
            .ok_or_else(|| {
                TmslError::NotFound(format!("pending entry for timestamp {}", timestamp))
            })?;
        entry.status = PENDING_STATUS_ACKED;
        Ok(())
    }

    /// Scan consecutive acked entries from the beginning and update processed_ts.
    /// Returns the number of entries cleaned up.
    pub fn cleanup_acked(&mut self) -> usize {
        let mut count = 0;
        for entry in &self.pending_entries {
            if entry.status == PENDING_STATUS_ACKED {
                count += 1;
            } else {
                break;
            }
        }
        if count > 0 {
            // Update processed_ts to the max consecutive acked timestamp
            if let Some(entry) = self.pending_entries.get(count - 1) {
                if entry.timestamp > self.processed_ts {
                    self.processed_ts = entry.timestamp;
                }
            }
            self.pending_entries.drain(..count);
        }
        count
    }

    /// Remove pending entries that have been waiting longer than timeout_secs.
    pub fn cleanup_timeout(&mut self, timeout_secs: u64) -> usize {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let before = self.pending_entries.len();
        self.pending_entries
            .retain(|e| e.start_time == 0 || (now - e.start_time) < timeout_secs as i64);
        before - self.pending_entries.len()
    }

    /// Get the next timestamp to poll (processed_ts + 1, or initial if no records).
    pub fn next_poll_ts(&self) -> i64 {
        if self.processed_ts > 0 {
            self.processed_ts + 1
        } else {
            1
        }
    }

    pub fn processed_ts(&self) -> i64 {
        self.processed_ts
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn pending_count(&self) -> usize {
        self.pending_entries.len()
    }

    pub fn pending_entries(&self) -> &[PendingEntry] {
        &self.pending_entries
    }

    /// Write in-memory state back to mmap (alias for sync_to_mmap).
    pub fn sync(&mut self) -> Result<()> {
        self.sync_to_mmap()
    }
}

fn read_i64_at(mmap: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(mmap[offset..offset + 8].try_into().unwrap())
}

fn write_i64_at(mmap: &mut MmapMut, offset: usize, val: i64) {
    mmap[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
}

/// Shared internal state for a dataset queue.
pub struct QueueInner {
    consumers: HashMap<String, Arc<Mutex<ConsumerStateFile>>>,
    closed: Arc<AtomicBool>,
}

impl QueueInner {
    pub(crate) fn new() -> Self {
        QueueInner {
            consumers: HashMap::new(),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    pub(crate) fn consumers(&self) -> &HashMap<String, Arc<Mutex<ConsumerStateFile>>> {
        &self.consumers
    }

    pub(crate) fn consumers_mut(&mut self) -> &mut HashMap<String, Arc<Mutex<ConsumerStateFile>>> {
        &mut self.consumers
    }
}

/// Queue handle for a dataset (Clone-safe, singleton per dataset).
pub struct DatasetQueue {
    pub(crate) dataset: Arc<Mutex<DataSet>>,
    pub(crate) inner: Arc<Mutex<QueueInner>>,
    pub(crate) notify: Arc<(Mutex<bool>, Condvar)>,
    pub(crate) allow_push: bool,
}

impl Clone for DatasetQueue {
    fn clone(&self) -> Self {
        DatasetQueue {
            dataset: Arc::clone(&self.dataset),
            inner: Arc::clone(&self.inner),
            notify: Arc::clone(&self.notify),
            allow_push: self.allow_push,
        }
    }
}

impl fmt::Debug for DatasetQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatasetQueue").finish_non_exhaustive()
    }
}

impl DatasetQueue {
    /// Construct a new DatasetQueue from raw components.
    ///
    /// Used by FFI and Python wrappers that manage dataset lifecycle
    /// separately from Store's internal handle registry.
    pub fn new(
        dataset: Arc<Mutex<DataSet>>,
        inner: Arc<Mutex<QueueInner>>,
        notify: Arc<(Mutex<bool>, Condvar)>,
    ) -> Self {
        DatasetQueue {
            dataset,
            inner,
            notify,
            allow_push: true,
        }
    }

    pub(crate) fn new_readonly_producer(
        dataset: Arc<Mutex<DataSet>>,
        inner: Arc<Mutex<QueueInner>>,
        notify: Arc<(Mutex<bool>, Condvar)>,
    ) -> Self {
        DatasetQueue {
            dataset,
            inner,
            notify,
            allow_push: false,
        }
    }

    /// Open or create a consumer group and return a consumer handle.
    pub fn open_consumer(&self, group_name: &str) -> Result<DatasetQueueConsumer> {
        validate_consumer_group_name(group_name)?;

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;

        if inner.is_closed() {
            return Err(TmslError::QueueClosed("queue is closed".into()));
        }

        let queue_dir = {
            let ds = self
                .dataset
                .lock()
                .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
            ds.queue_dir()
        };

        let state_file = if inner.consumers.contains_key(group_name) {
            Arc::clone(&inner.consumers[group_name])
        } else {
            let state_path = queue_dir.join(group_name);
            // Determine initial processed_ts from the dataset
            let initial_ts = {
                let ds = self
                    .dataset
                    .lock()
                    .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
                ds.latest_written_timestamp()
            };
            let sf = Arc::new(Mutex::new(ConsumerStateFile::open_or_create(
                state_path, initial_ts,
            )?));
            inner
                .consumers
                .insert(group_name.to_string(), Arc::clone(&sf));
            sf
        };

        Ok(DatasetQueueConsumer {
            group_name: group_name.to_string(),
            state_file,
            notify: Arc::clone(&self.notify),
            dataset: Arc::clone(&self.dataset),
            closed: Arc::clone(&inner.closed),
        })
    }

    /// Drop (close and remove) a consumer group.
    pub fn drop_consumer(&self, group_name: &str) -> Result<()> {
        validate_consumer_group_name(group_name)?;

        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;

        // Sync state file before dropping
        if let Some(sf) = inner.consumers.get(group_name) {
            let mut guard = sf
                .lock()
                .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
            guard.sync()?;
        }

        let removed = inner.consumers.remove(group_name);
        if removed.is_none() {
            return Err(TmslError::ConsumerGroupNotFound(group_name.to_string()));
        }

        // Try to delete the state file
        let queue_dir = {
            let ds = self
                .dataset
                .lock()
                .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
            ds.queue_dir()
        };
        let state_path = queue_dir.join(group_name);
        let _ = fs::remove_file(&state_path);

        Ok(())
    }

    /// Push data into the dataset queue.
    ///
    /// Assigns an auto-increment timestamp (latest_written_timestamp + 1),
    /// writes to the dataset, and notifies waiting consumers.
    pub fn push(&self, data: &[u8]) -> Result<i64> {
        if !self.allow_push {
            return Err(TmslError::InvalidData(
                "queue producer is read-only for this dataset".into(),
            ));
        }
        if self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?
            .is_closed()
        {
            return Err(TmslError::QueueClosed("queue is closed".into()));
        }

        let mut ds = self
            .dataset
            .lock()
            .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;

        // Auto-increment timestamp
        let timestamp = ds.latest_written_timestamp() + 1;
        ds.write(timestamp, data)?;

        // Notify waiting consumers (only on normal write, ts > old_latest)
        let (lock, cvar) = (&self.notify.0, &self.notify.1);
        let mut notified = lock
            .lock()
            .map_err(|_| TmslError::InvalidData("notify mutex poisoned".into()))?;
        *notified = true;
        cvar.notify_all();

        Ok(timestamp)
    }

    /// Close the queue (marks as closed, drops all consumers).
    pub fn close(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;

        if inner.is_closed() {
            return Ok(());
        }

        // Sync all consumer state files
        for sf in inner.consumers.values() {
            if let Ok(mut guard) = sf.lock() {
                let _ = guard.sync();
            }
        }

        inner.close();
        inner.consumers.clear();

        // Notify any waiting polls
        let (lock, cvar) = (&self.notify.0, &self.notify.1);
        let mut notified = lock
            .lock()
            .map_err(|_| TmslError::InvalidData("notify mutex poisoned".into()))?;
        *notified = true;
        cvar.notify_all();

        Ok(())
    }
}

/// Consumer handle for a specific consumer group.
///
/// Multiple consumers can exist for the same group, sharing progress
/// via the shared state file. poll() is exclusive via state file mutex.
pub struct DatasetQueueConsumer {
    group_name: String,
    state_file: Arc<Mutex<ConsumerStateFile>>,
    notify: Arc<(Mutex<bool>, Condvar)>,
    dataset: Arc<Mutex<DataSet>>,
    closed: Arc<AtomicBool>,
}

impl Clone for DatasetQueueConsumer {
    fn clone(&self) -> Self {
        DatasetQueueConsumer {
            group_name: self.group_name.clone(),
            state_file: Arc::clone(&self.state_file),
            notify: Arc::clone(&self.notify),
            dataset: Arc::clone(&self.dataset),
            closed: Arc::clone(&self.closed),
        }
    }
}

impl fmt::Debug for DatasetQueueConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatasetQueueConsumer")
            .field("group_name", &self.group_name)
            .finish_non_exhaustive()
    }
}

impl DatasetQueueConsumer {
    /// Poll for the next unacked record.
    ///
    /// - If there's already an unacked pending entry, return it immediately.
    /// - Otherwise, read from the dataset starting at processed_ts + 1.
    /// - If no data is available, wait up to `timeout` for a push notification.
    ///
    /// Returns `Ok(Some((timestamp, data)))` if a record is found,
    /// `Ok(None)` if timeout expires, or `Err(QueueClosed)` if the queue was closed.
    pub fn poll(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("queue is closed".into()));
        }

        // Check for existing unacked pending entry
        {
            let sf = self
                .state_file
                .lock()
                .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
            if let Some(entry) = sf.find_first_unacked() {
                let ts = entry.timestamp;
                drop(sf);
                return self.read_record_at(ts);
            }
        }

        if let Some(row) = self.try_poll_available()? {
            Ok(Some(row))
        } else {
            self.wait_for_data(timeout)
        }
    }

    fn try_poll_available(&self) -> Result<Option<(i64, Vec<u8>)>> {
        let mut sf = self
            .state_file
            .lock()
            .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
        let next_ts = sf.next_poll_ts();
        let mut ds = self
            .dataset
            .lock()
            .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;

        let direct = ds.read(next_ts)?;
        let row = if direct.is_some() {
            direct
        } else {
            let entries = ds.query_index_entries(next_ts, i64::MAX)?;
            let mut found = None;
            for entry in entries {
                if entry.is_filler() || sf.is_in_pending(entry.timestamp) {
                    continue;
                }
                found = Some(ds.read_entry_at_index(&entry)?);
                break;
            }
            found
        };

        if let Some((ts, data)) = row {
            if !sf.is_in_pending(ts) {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                sf.add_pending(PendingEntry {
                    timestamp: ts,
                    start_time: now,
                    status: PENDING_STATUS_UNACKED,
                })?;
            }
            Ok(Some((ts, data)))
        } else {
            Ok(None)
        }
    }

    fn wait_for_data(&self, timeout: Duration) -> Result<Option<(i64, Vec<u8>)>> {
        let (lock, cvar) = (&self.notify.0, &self.notify.1);
        let mut notified = lock
            .lock()
            .map_err(|_| TmslError::InvalidData("notify mutex poisoned".into()))?;

        let deadline = Instant::now() + timeout;

        loop {
            if self.closed.load(Ordering::SeqCst) {
                return Err(TmslError::QueueClosed("queue is closed".into()));
            }

            if *notified {
                *notified = false;
                drop(notified);

                // Re-check for unacked pending
                {
                    let sf = self
                        .state_file
                        .lock()
                        .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
                    if let Some(entry) = sf.find_first_unacked() {
                        let ts = entry.timestamp;
                        drop(sf);
                        return self.read_record_at(ts);
                    }
                }

                if let Some(row) = self.try_poll_available()? {
                    return Ok(Some(row));
                } else {
                    notified = lock
                        .lock()
                        .map_err(|_| TmslError::InvalidData("notify mutex poisoned".into()))?;
                    continue;
                }
            }

            let remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(d) => d,
                None => return Ok(None), // Timeout
            };

            let (new_notified, timeout_result) = cvar.wait_timeout(notified, remaining).unwrap();
            notified = new_notified;
            if timeout_result.timed_out() {
                // One final check after timeout
                drop(notified);
                {
                    let sf = self
                        .state_file
                        .lock()
                        .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;
                    if let Some(entry) = sf.find_first_unacked() {
                        let ts = entry.timestamp;
                        drop(sf);
                        return self.read_record_at(ts);
                    }
                }
                return Ok(None);
            }
        }
    }

    fn read_record_at(&self, timestamp: i64) -> Result<Option<(i64, Vec<u8>)>> {
        let mut ds = self
            .dataset
            .lock()
            .map_err(|_| TmslError::InvalidData("dataset mutex poisoned".into()))?;
        ds.read(timestamp)
    }

    /// Ack a previously polled record.
    pub fn ack(&self, timestamp: i64) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TmslError::QueueClosed("queue is closed".into()));
        }

        let mut sf = self
            .state_file
            .lock()
            .map_err(|_| TmslError::InvalidData("state file mutex poisoned".into()))?;

        sf.ack_pending(timestamp)?;
        sf.cleanup_acked();
        Ok(())
    }
}

/// Queue directory path for a dataset.
pub(crate) fn queue_dir_for(base_dir: &Path) -> PathBuf {
    base_dir.join("queue")
}

/// Flush all consumer state files for a dataset (called by background task).
pub(crate) fn flush_queue_state_files(inner: &Arc<Mutex<QueueInner>>) -> Result<()> {
    let guard = inner
        .lock()
        .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;

    for sf in guard.consumers.values() {
        if let Ok(mut state) = sf.lock() {
            // Cleanup timeout entries first
            state.cleanup_timeout(DEFAULT_PENDING_TIMEOUT_SECS);
            state.sync_to_mmap()?;
            state.flush()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_queue_dir() -> PathBuf {
        let d = std::env::temp_dir().join("timslite_queue_tests");
        fs::create_dir_all(&d).unwrap();
        d.join(format!(
            "test_{:?}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn cleanup(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn pending_entry_round_trip() {
        let entry = PendingEntry {
            timestamp: 1700000000,
            start_time: 1700000005,
            status: PENDING_STATUS_UNACKED,
        };
        let mut buf = [0u8; PENDING_ENTRY_SIZE];
        entry.serialize_to(&mut buf);
        let restored = PendingEntry::deserialize_from(&buf);
        assert_eq!(restored.timestamp, entry.timestamp);
        assert_eq!(restored.start_time, entry.start_time);
        assert_eq!(restored.status, entry.status);
    }

    #[test]
    fn pending_entry_acked_status() {
        let entry = PendingEntry {
            timestamp: -42,
            start_time: 0,
            status: PENDING_STATUS_ACKED,
        };
        let mut buf = [0u8; PENDING_ENTRY_SIZE];
        entry.serialize_to(&mut buf);
        let restored = PendingEntry::deserialize_from(&buf);
        assert_eq!(restored.timestamp, -42);
        assert_eq!(restored.status, PENDING_STATUS_ACKED);
    }

    #[test]
    fn csf_create_and_reopen() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let sf = ConsumerStateFile::open_or_create(path.clone(), 42).unwrap();
        assert_eq!(sf.processed_ts(), 42);
        assert_eq!(sf.pending_count(), 0);
        drop(sf);

        let sf2 = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf2.processed_ts(), 42);
        assert_eq!(sf2.pending_count(), 0);
        drop(sf2);
        cleanup(&dir);
    }

    #[test]
    fn csf_create_parent_dirs() {
        let dir = temp_queue_dir();
        let path = dir.join("sub").join("deep").join("group1");
        let sf = ConsumerStateFile::open_or_create(path.clone(), 0).unwrap();
        assert!(path.exists());
        assert_eq!(sf.processed_ts(), 0);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_file_size() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let sf = ConsumerStateFile::open_or_create(path.clone(), 0).unwrap();
        drop(sf);
        let meta = fs::metadata(&path).unwrap();
        assert_eq!(meta.len() as usize, STATE_FILE_SIZE);
        cleanup(&dir);
    }

    #[test]
    fn csf_open_existing_with_pending() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        {
            let mut sf = ConsumerStateFile::open_or_create(path.clone(), 100).unwrap();
            sf.add_pending(PendingEntry {
                timestamp: 101,
                start_time: 1000,
                status: PENDING_STATUS_UNACKED,
            })
            .unwrap();
            sf.add_pending(PendingEntry {
                timestamp: 102,
                start_time: 1001,
                status: PENDING_STATUS_ACKED,
            })
            .unwrap();
            sf.sync_to_mmap().unwrap();
            sf.flush().unwrap();
        }
        let sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf.processed_ts(), 100);
        assert_eq!(sf.pending_count(), 2);
        assert!(sf.find_pending(101).is_some());
        assert!(sf.find_pending(102).is_some());
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_open_invalid_magic() {
        let dir = temp_queue_dir();
        let path = dir.join("bad");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            file.set_len(STATE_FILE_SIZE as u64).unwrap();
            let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
            mmap[0..4].copy_from_slice(b"BAD!");
            mmap.flush().unwrap();
        }
        let result = ConsumerStateFile::open_or_create(path, 0);
        assert!(matches!(result, Err(TmslError::InvalidMagic)));
        cleanup(&dir);
    }

    #[test]
    fn csf_open_invalid_version() {
        let dir = temp_queue_dir();
        let path = dir.join("bad");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            file.set_len(STATE_FILE_SIZE as u64).unwrap();
            let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
            mmap[0..4].copy_from_slice(QUEUE_STATE_MAGIC);
            mmap[4..8].copy_from_slice(&99u32.to_le_bytes());
            mmap.flush().unwrap();
        }
        let result = ConsumerStateFile::open_or_create(path, 0);
        assert!(matches!(result, Err(TmslError::InvalidVersion(v)) if v == 99));
        cleanup(&dir);
    }

    #[test]
    fn csf_add_pending_increments_count() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf.pending_count(), 0);
        sf.add_pending(PendingEntry {
            timestamp: 1,
            start_time: 0,
            status: PENDING_STATUS_UNACKED,
        })
        .unwrap();
        assert_eq!(sf.pending_count(), 1);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_add_pending_full() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        for i in 0..MAX_PENDING_ENTRIES {
            sf.add_pending(PendingEntry {
                timestamp: i as i64,
                start_time: 0,
                status: PENDING_STATUS_UNACKED,
            })
            .unwrap();
        }
        let result = sf.add_pending(PendingEntry {
            timestamp: 9999,
            start_time: 0,
            status: PENDING_STATUS_UNACKED,
        });
        assert!(matches!(result, Err(TmslError::PendingFull(_))));
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_ack_pending_marks_acked() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 42,
            start_time: 0,
            status: PENDING_STATUS_UNACKED,
        })
        .unwrap();
        sf.ack_pending(42).unwrap();
        assert!(sf.find_pending(42).unwrap().status == PENDING_STATUS_ACKED);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_ack_nonexistent_errors() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        let result = sf.ack_pending(999);
        assert!(result.is_err());
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_find_first_unacked() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 10,
            start_time: 0,
            status: PENDING_STATUS_ACKED,
        })
        .unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 11,
            start_time: 0,
            status: PENDING_STATUS_UNACKED,
        })
        .unwrap();
        let found = sf.find_first_unacked().unwrap();
        assert_eq!(found.timestamp, 11);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_find_first_unacked_all_acked() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 10,
            start_time: 0,
            status: PENDING_STATUS_ACKED,
        })
        .unwrap();
        assert!(sf.find_first_unacked().is_none());
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_cleanup_acked_consecutive() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 10,
            start_time: 0,
            status: PENDING_STATUS_ACKED,
        })
        .unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 11,
            start_time: 0,
            status: PENDING_STATUS_ACKED,
        })
        .unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 12,
            start_time: 0,
            status: PENDING_STATUS_UNACKED,
        })
        .unwrap();
        let cleaned = sf.cleanup_acked();
        assert_eq!(cleaned, 2);
        assert_eq!(sf.pending_count(), 1);
        assert_eq!(sf.processed_ts(), 11);
        assert_eq!(sf.pending_entries()[0].timestamp, 12);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_cleanup_acked_empty() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf.cleanup_acked(), 0);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_cleanup_acked_persists() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        {
            let mut sf = ConsumerStateFile::open_or_create(path.clone(), 10).unwrap();
            sf.add_pending(PendingEntry {
                timestamp: 11,
                start_time: 0,
                status: PENDING_STATUS_UNACKED,
            })
            .unwrap();
            sf.ack_pending(11).unwrap();
            sf.cleanup_acked();
            sf.sync_to_mmap().unwrap();
            sf.flush().unwrap();
        }
        let sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf.processed_ts(), 11);
        assert_eq!(sf.pending_count(), 0);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_cleanup_timeout_removes_expired() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 100,
            start_time: 1, // very old
            status: PENDING_STATUS_UNACKED,
        })
        .unwrap();
        let removed = sf.cleanup_timeout(300);
        assert_eq!(removed, 1);
        assert_eq!(sf.pending_count(), 0);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_cleanup_timeout_keeps_future() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 3600;
        sf.add_pending(PendingEntry {
            timestamp: 100,
            start_time: future,
            status: PENDING_STATUS_UNACKED,
        })
        .unwrap();
        let removed = sf.cleanup_timeout(300);
        assert_eq!(removed, 0);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_next_poll_ts_positive() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let sf = ConsumerStateFile::open_or_create(path, 100).unwrap();
        assert_eq!(sf.next_poll_ts(), 101);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_next_poll_ts_zero() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf.next_poll_ts(), 1);
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn csf_sync_persists_data() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        {
            let mut sf = ConsumerStateFile::open_or_create(path.clone(), 0).unwrap();
            sf.add_pending(PendingEntry {
                timestamp: 99,
                start_time: 88,
                status: PENDING_STATUS_UNACKED,
            })
            .unwrap();
            sf.sync_to_mmap().unwrap();
            sf.flush().unwrap();
        }
        let sf2 = ConsumerStateFile::open_or_create(path, 0).unwrap();
        assert_eq!(sf2.pending_count(), 1);
        assert_eq!(sf2.pending_entries()[0].timestamp, 99);
        assert_eq!(sf2.pending_entries()[0].start_time, 88);
        drop(sf2);
        cleanup(&dir);
    }

    #[test]
    fn csf_is_in_pending_and_find() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let mut sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        sf.add_pending(PendingEntry {
            timestamp: 42,
            start_time: 0,
            status: PENDING_STATUS_UNACKED,
        })
        .unwrap();
        assert!(sf.is_in_pending(42));
        assert!(!sf.is_in_pending(99));
        assert_eq!(sf.find_pending(42).unwrap().timestamp, 42);
        assert!(sf.find_pending(99).is_none());
        drop(sf);
        cleanup(&dir);
    }

    #[test]
    fn qi_new_empty() {
        let qi = QueueInner::new();
        assert!(!qi.is_closed());
        assert!(qi.consumers().is_empty());
    }

    #[test]
    fn qi_close() {
        let qi = QueueInner::new();
        qi.close();
        assert!(qi.is_closed());
    }

    #[test]
    fn qi_consumers_can_add() {
        let dir = temp_queue_dir();
        let path = dir.join("group1");
        let sf = ConsumerStateFile::open_or_create(path, 0).unwrap();
        let mut qi = QueueInner::new();
        qi.consumers_mut()
            .insert("group1".to_string(), Arc::new(Mutex::new(sf)));
        assert_eq!(qi.consumers().len(), 1);
        assert!(qi.consumers().contains_key("group1"));
        drop(qi);
        cleanup(&dir);
    }

    #[test]
    fn qi_multiple_groups() {
        let dir = temp_queue_dir();
        let sf1 = ConsumerStateFile::open_or_create(dir.join("g1"), 0).unwrap();
        let sf2 = ConsumerStateFile::open_or_create(dir.join("g2"), 0).unwrap();
        let mut qi = QueueInner::new();
        qi.consumers_mut()
            .insert("g1".to_string(), Arc::new(Mutex::new(sf1)));
        qi.consumers_mut()
            .insert("g2".to_string(), Arc::new(Mutex::new(sf2)));
        assert_eq!(qi.consumers().len(), 2);
        qi.close();
        assert!(qi.is_closed());
        drop(qi);
        cleanup(&dir);
    }
}
