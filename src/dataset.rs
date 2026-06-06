//! DataSet: aggregates DataSegmentSet + TimeIndex for a (name, type) pair.
//!
//! Lifecycle: `create` (explicit, with parameters) / `open` (reads from meta) / `close` / `drop_dataset`.
//! Dataset creation parameters are written to the meta file and are immutable.
//! Subsequent opens read those values from meta.

use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use crate::cache::BlockCache;
use crate::config::DataSetConfig;
use crate::error::{Result, TmslError};
use crate::index::segment::{last_entry_timestamp, IndexEntry, IndexSegment, BLOCK_OFFSET_FILLER};
use crate::index::TimeIndex;
use crate::meta::DataSetMeta;
use crate::query::iter::{QueryIterator, QuerySource};
use crate::queue::{flush_queue_state_files, queue_dir_for, QueueInner};
use crate::segment::data::{APPEND_MIGRATION_THRESHOLD, MAX_RECORD_DATA_SIZE, RECORD_HEADER_SIZE};
use crate::segment::DataSegmentSet;
use crate::segment::ReadIndexEntry;

type QueueCondvarPair = Arc<(Mutex<bool>, Condvar)>;

pub(crate) trait DataSetJournalSink: Send + Sync {
    fn record_write(&self, key: &DataSetKey, entry: IndexEntry) -> Result<()>;
    fn record_delete(&self, key: &DataSetKey, entry: IndexEntry) -> Result<()>;
    fn record_append(
        &self,
        key: &DataSetKey,
        entry: IndexEntry,
        data_offset: u32,
        data_len: u32,
    ) -> Result<()>;
}

#[derive(Clone, Default)]
pub(crate) struct DataSetRuntimeContext {
    block_cache: Option<Arc<BlockCache>>,
    journal: Option<Arc<dyn DataSetJournalSink>>,
    read_only: bool,
}

impl DataSetRuntimeContext {
    pub(crate) fn new(
        block_cache: Option<Arc<BlockCache>>,
        journal: Option<Arc<dyn DataSetJournalSink>>,
    ) -> Self {
        Self {
            block_cache,
            journal,
            read_only: false,
        }
    }

    pub(crate) fn read_only() -> Self {
        Self {
            block_cache: None,
            journal: None,
            read_only: true,
        }
    }
}

fn validate_record_data_len(data_len: usize) -> Result<()> {
    if data_len > MAX_RECORD_DATA_SIZE {
        return Err(TmslError::InvalidData(
            "record data_len exceeds 4MiB limit".into(),
        ));
    }
    Ok(())
}

fn data_len_u32(data_len: usize) -> Result<u32> {
    u32::try_from(data_len).map_err(|_| TmslError::InvalidData("data_len exceeds u32".into()))
}

/// Dataset key for identifying a (name, type) pair.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DataSetKey {
    pub name: String,
    pub dataset_type: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteBranch {
    Normal,
    Correction,
    OutOfOrder,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteOutcome {
    pub index_entry: IndexEntry,
    pub branch: WriteBranch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeleteOutcome {
    pub old_index_entry: IndexEntry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendOutcome {
    pub index_entry: IndexEntry,
    pub data_offset: u32,
    pub data_len: u32,
    pub migrated: bool,
}

pub struct DataSet {
    pub id: DataSetKey,
    pub base_dir: PathBuf,
    pub(crate) config: DataSetConfig,
    segments: DataSegmentSet,
    time_index: TimeIndex,
    last_used_at: Instant,
    latest_written_timestamp: i64, // Highest written timestamp, not latest valid record
    retention_window: u64,         // 0 = no limit (same unit as timestamp)
    queue_inner: Option<Arc<Mutex<QueueInner>>>,
    queue_notify: Option<Arc<(Mutex<bool>, Condvar)>>,
    runtime_context: DataSetRuntimeContext,
}

impl DataSet {
    /// Create a new dataset (explicit creation, errors if already exists).
    ///
    /// Parameters are written to the meta file and are **immutable**; they cannot be changed
    /// after creation.
    pub fn create(
        id: DataSetKey,
        base_dir: PathBuf,
        data_segment_size: u64,
        index_segment_size: u64,
        compress_level: u8,
        index_continuous: u8,
        initial_data_segment_size: u64,
        initial_index_segment_size: u64,
        retention_window: u64,
    ) -> Result<Self> {
        Self::create_with_compression(
            id,
            base_dir,
            data_segment_size,
            index_segment_size,
            compress_level,
            crate::compress::COMPRESS_TYPE_ZSTD,
            index_continuous,
            initial_data_segment_size,
            initial_index_segment_size,
            retention_window,
        )
    }

    pub fn create_with_compression(
        id: DataSetKey,
        base_dir: PathBuf,
        data_segment_size: u64,
        index_segment_size: u64,
        compress_level: u8,
        compress_type: u8,
        index_continuous: u8,
        initial_data_segment_size: u64,
        initial_index_segment_size: u64,
        retention_window: u64,
    ) -> Result<Self> {
        crate::compress::validate_compress_type(compress_type)?;
        let meta_path = base_dir.join("meta");
        if meta_path.exists() {
            return Err(TmslError::AlreadyExists(format!(
                "dataset already exists at {:?}",
                base_dir
            )));
        }

        // Ensure data/ subdirectory exists
        let data_dir = base_dir.join("data");
        std::fs::create_dir_all(&data_dir)?;
        // Ensure index/ subdirectory exists
        let index_dir = base_dir.join("index");
        std::fs::create_dir_all(&index_dir)?;

        // Write meta file (immutable config, written only once)
        let meta = DataSetMeta::new(
            data_segment_size,
            index_segment_size,
            compress_level,
            compress_type,
            index_continuous,
            initial_data_segment_size,
            initial_index_segment_size,
            retention_window,
        );
        meta.write_to_file(&meta_path)?;

        let segments = DataSegmentSet::new_with_compression(
            &base_dir,
            data_segment_size,
            initial_data_segment_size,
            compress_level,
            compress_type,
        )?;
        let time_index = TimeIndex::new_with_compression(
            &index_dir,
            index_segment_size,
            initial_index_segment_size,
            index_continuous != 0,
            compress_level,
            compress_type,
        )?;

        Ok(Self {
            id,
            base_dir,
            config: DataSetConfig {
                data_segment_size,
                index_segment_size,
                compress_level,
                compress_type,
                index_continuous,
                initial_data_segment_size,
                initial_index_segment_size,
                retention_window,
            },
            segments,
            time_index,
            last_used_at: Instant::now(),
            latest_written_timestamp: 0,
            retention_window,
            queue_inner: None,
            queue_notify: None,
            runtime_context: DataSetRuntimeContext::default(),
        })
    }

    /// Open an existing dataset (reads config from meta file).
    ///
    /// Fails if the dataset does not exist (no meta file).
    /// Segment sizes and compress_level are read from meta and cannot be overridden.
    pub fn open(id: DataSetKey, base_dir: PathBuf) -> Result<Self> {
        let meta_path = base_dir.join("meta");
        if !meta_path.exists() {
            return Err(TmslError::NotFound(format!(
                "dataset meta not found at {:?}",
                meta_path
            )));
        }

        // Read meta file (immutable config)
        let meta = DataSetMeta::read_from_file(&meta_path)?;

        let config = DataSetConfig {
            data_segment_size: meta.data_segment_size,
            index_segment_size: meta.index_segment_size,
            compress_level: meta.compress_level,
            compress_type: meta.compress_type,
            index_continuous: meta.index_continuous,
            initial_data_segment_size: meta.initial_data_segment_size,
            initial_index_segment_size: meta.initial_index_segment_size,
            retention_window: meta.retention_window,
        };
        let retention_window = meta.retention_window;

        let segments = DataSegmentSet::load_existing_with_compression(
            &base_dir,
            config.data_segment_size,
            meta.initial_data_segment_size,
            config.compress_level,
            config.compress_type,
        )?;
        let index_dir = base_dir.join("index");
        let time_index = TimeIndex::load_existing_with_compression(
            &index_dir,
            config.index_segment_size,
            meta.initial_index_segment_size,
            config.index_continuous != 0,
            config.compress_level,
            config.compress_type,
        )?;

        // Recover latest_written_timestamp from index segments
        let latest_written_timestamp = Self::recover_latest_timestamp(&time_index);

        Ok(Self {
            id,
            base_dir,
            config,
            segments,
            time_index,
            last_used_at: Instant::now(),
            latest_written_timestamp,
            retention_window,
            queue_inner: None,
            queue_notify: None,
            runtime_context: DataSetRuntimeContext::default(),
        })
    }

    pub(crate) fn set_runtime_context(&mut self, context: DataSetRuntimeContext) {
        self.runtime_context = context;
    }

    /// Delete an entire dataset directory (destructive, not recoverable).
    pub fn drop_dataset(base_dir: &std::path::Path) -> Result<()> {
        std::fs::remove_dir_all(base_dir)?;
        Ok(())
    }

    /// Write a record to this dataset.
    ///
    /// # Timestamp dispatch (both indexing modes)
    ///
    /// - `timestamp <= 0`: error.
    /// - `timestamp == latest_written_timestamp` (and latest > 0): **correction write**:
    ///   in-place overwrite of the data bytes in the last pending raw block of the latest
    ///   data segment. The index entry is unchanged. If the target block has already been
    ///   sealed and compressed, falls back to out-of-order write:
    ///   appends data to latest segment, updates index entry, and increments the old
    ///   segment's `invalid_record_count`.
    /// - `timestamp < latest_written_timestamp`: **out-of-order write**: appends data to
    ///   the latest data segment and updates the existing index position in place. In
    ///   continuous mode, sparse logical holes are materialized on demand.
    /// - `timestamp > latest_written_timestamp`: **normal write**: in continuous mode only
    ///   materializes filler entries in the previous and current edge index segments.
    pub fn write(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
        if self.runtime_context.read_only {
            return Err(TmslError::InvalidData(
                "read-only dataset cannot be written".into(),
            ));
        }
        let cache = self.runtime_context.block_cache.clone();
        let journal = self.runtime_context.journal.clone();
        let outcome = self.write_with_cache_outcome(timestamp, data, cache.as_deref())?;
        if let Some(journal) = journal.as_ref() {
            journal.record_write(&self.id, outcome.index_entry)?;
        }
        Ok(())
    }

    /// Write a record and invalidate global cache entries affected by
    /// correction/out-of-order index rewrites.
    pub(crate) fn write_with_cache(
        &mut self,
        timestamp: i64,
        data: &[u8],
        cache: Option<&BlockCache>,
    ) -> Result<()> {
        self.write_with_cache_outcome(timestamp, data, cache)
            .map(|_| ())
    }

    pub(crate) fn write_with_cache_outcome(
        &mut self,
        timestamp: i64,
        data: &[u8],
        cache: Option<&BlockCache>,
    ) -> Result<WriteOutcome> {
        validate_record_data_len(data.len())?;
        if timestamp <= 0 {
            return Err(TmslError::InvalidData("timestamp must be > 0".into()));
        }
        if self.is_timestamp_expired(timestamp) {
            return Err(self.expired_error(timestamp));
        }

        // Correction write: same timestamp as latest; in-place overwrite in
        // the last pending raw block of the latest data segment. Index unchanged.
        if self.latest_written_timestamp > 0 && timestamp == self.latest_written_timestamp {
            return self.correct_write(timestamp, data, cache);
        }

        // Out-of-order write: timestamp < latest; append to latest segment,
        // update existing index entry in place. May increment invalid_record_count
        // on the old data segment.
        if timestamp < self.latest_written_timestamp {
            return self.out_of_order_write(timestamp, data, cache);
        }

        // Normal write: timestamp > latest
        if self.config.index_continuous == 0 {
            let (seg_offset, block_rel_offset, in_block_offset) =
                self.segments.append(timestamp, data)?;
            let block_offset = seg_offset + block_rel_offset;
            self.time_index
                .add_entry(timestamp, block_offset, in_block_offset)?;
            self.latest_written_timestamp = timestamp;
            self.last_used_at = Instant::now();
            self.notify_queue();
            Ok(WriteOutcome {
                index_entry: IndexEntry::new(timestamp, block_offset, in_block_offset),
                branch: WriteBranch::Normal,
            })
        } else {
            let (seg_offset, block_rel_offset, in_block_offset) =
                self.segments.append(timestamp, data)?;
            let block_offset = seg_offset + block_rel_offset;
            self.time_index.add_sparse_continuous_entry(
                self.latest_written_timestamp,
                timestamp,
                block_offset,
                in_block_offset,
            )?;
            self.latest_written_timestamp = timestamp;
            self.last_used_at = Instant::now();
            self.notify_queue();
            Ok(WriteOutcome {
                index_entry: IndexEntry::new(timestamp, block_offset, in_block_offset),
                branch: WriteBranch::Normal,
            })
        }
    }

    pub fn append(&mut self, timestamp: i64, data: &[u8]) -> Result<()> {
        if self.runtime_context.read_only {
            return Err(TmslError::InvalidData(
                "read-only dataset cannot be appended".into(),
            ));
        }
        let cache = self.runtime_context.block_cache.clone();
        let journal = self.runtime_context.journal.clone();
        let outcome = self.append_with_cache_outcome(timestamp, data, cache.as_deref())?;
        if let (Some(outcome), Some(journal)) = (outcome, journal.as_ref()) {
            journal.record_append(
                &self.id,
                outcome.index_entry,
                outcome.data_offset,
                outcome.data_len,
            )?;
        }
        Ok(())
    }

    pub(crate) fn append_with_cache(
        &mut self,
        timestamp: i64,
        data: &[u8],
        cache: Option<&BlockCache>,
    ) -> Result<()> {
        self.append_with_cache_outcome(timestamp, data, cache)
            .map(|_| ())
    }

    pub(crate) fn append_with_cache_outcome(
        &mut self,
        timestamp: i64,
        data: &[u8],
        cache: Option<&BlockCache>,
    ) -> Result<Option<AppendOutcome>> {
        if timestamp <= 0 {
            return Err(TmslError::InvalidData("timestamp must be > 0".into()));
        }
        validate_record_data_len(data.len())?;
        if self.is_timestamp_expired(timestamp) {
            return Err(self.expired_error(timestamp));
        }
        if timestamp < self.latest_written_timestamp {
            return Err(TmslError::InvalidData(
                "append timestamp is older than latest_written_timestamp".into(),
            ));
        }
        if data.is_empty() {
            return Ok(None);
        }
        if timestamp > self.latest_written_timestamp {
            let outcome = self.write_with_cache_outcome(timestamp, data, cache)?;
            return Ok(Some(AppendOutcome {
                index_entry: outcome.index_entry,
                data_offset: 0,
                data_len: data_len_u32(data.len())?,
                migrated: false,
            }));
        }

        if self.latest_written_timestamp == 0 {
            let outcome = self.write_with_cache_outcome(timestamp, data, cache)?;
            return Ok(Some(AppendOutcome {
                index_entry: outcome.index_entry,
                data_offset: 0,
                data_len: data_len_u32(data.len())?,
                migrated: false,
            }));
        }

        let entry = self.time_index.find_entry(timestamp)?.ok_or_else(|| {
            TmslError::NotFound(format!("no index entry for append timestamp {}", timestamp))
        })?;
        if entry.block_offset == BLOCK_OFFSET_FILLER {
            return Err(TmslError::NotFound(format!(
                "latest index entry at timestamp {} is deleted",
                timestamp
            )));
        }

        let old_data = self
            .segments
            .read_mutable_tail_record(entry.block_offset, entry.in_block_offset)?;
        let final_len = old_data
            .len()
            .checked_add(data.len())
            .ok_or_else(|| TmslError::InvalidData("append data_len overflow".into()))?;
        validate_record_data_len(final_len)?;
        let final_record_size = RECORD_HEADER_SIZE
            .checked_add(final_len)
            .ok_or_else(|| TmslError::InvalidData("append record size overflow".into()))?;
        let data_offset = data_len_u32(old_data.len())?;
        let data_len = data_len_u32(data.len())?;

        if final_record_size > APPEND_MIGRATION_THRESHOLD {
            let mut full_data = Vec::with_capacity(final_len);
            full_data.extend_from_slice(&old_data);
            full_data.extend_from_slice(data);
            let (seg_offset, block_rel_offset, in_block_offset) =
                self.segments.append_single_record(timestamp, &full_data)?;
            let new_block_offset = seg_offset + block_rel_offset;
            let old_entry =
                self.time_index
                    .update_entry(timestamp, new_block_offset, in_block_offset)?;
            self.invalidate_cache_for_entry(&old_entry, cache);
            self.segments
                .increment_invalid_record_count(old_entry.block_offset)?;
            self.last_used_at = Instant::now();
            return Ok(Some(AppendOutcome {
                index_entry: IndexEntry::new(timestamp, new_block_offset, in_block_offset),
                data_offset,
                data_len,
                migrated: true,
            }));
        }

        let actual_offset =
            self.segments
                .append_to_last_record(entry.block_offset, entry.in_block_offset, data)?;
        self.last_used_at = Instant::now();
        Ok(Some(AppendOutcome {
            index_entry: entry,
            data_offset: actual_offset,
            data_len,
            migrated: false,
        }))
    }

    fn notify_queue(&self) {
        if let Some(ref notify) = self.queue_notify {
            let (lock, cvar) = (&notify.0, &notify.1);
            if let Ok(mut guard) = lock.lock() {
                *guard = true;
                cvar.notify_all();
            }
        }
    }

    /// Out-of-order write: timestamp < latest_written_timestamp (both modes).
    ///
    /// Appends data to the latest segment and updates the existing index entry
    /// in place with the new data location. If the old entry referenced real data,
    /// the old data segment's `invalid_record_count` is incremented.
    ///
    /// Non-continuous mode requires an existing index entry at `timestamp`.
    /// Continuous mode may update a real entry, replace a filler, or materialize
    /// a sparse logical hole on demand.
    fn out_of_order_write(
        &mut self,
        timestamp: i64,
        data: &[u8],
        cache: Option<&BlockCache>,
    ) -> Result<WriteOutcome> {
        let (seg_offset, block_rel_offset, in_block_offset) =
            self.segments.append(timestamp, data)?;
        let new_block_offset = seg_offset + block_rel_offset;

        let old_entry =
            self.time_index
                .update_entry(timestamp, new_block_offset, in_block_offset)?;

        if old_entry.block_offset != BLOCK_OFFSET_FILLER {
            self.invalidate_cache_for_entry(&old_entry, cache);
            self.segments
                .increment_invalid_record_count(old_entry.block_offset)?;
        }

        // latest_written_timestamp unchanged
        self.last_used_at = Instant::now();
        Ok(WriteOutcome {
            index_entry: IndexEntry::new(timestamp, new_block_offset, in_block_offset),
            branch: WriteBranch::OutOfOrder,
        })
    }

    /// Correction write: overwrite the data of an existing record in place.
    ///
    /// The record is located via the existing index entry, then its data bytes
    /// in the last pending raw block of the latest data segment are replaced.
    /// Supports variable data length; updates block + segment counters accordingly.
    ///
    /// If the target block has been sealed and compressed,
    /// falls back to out-of-order write: appends data to latest segment, updates
    /// the index entry, and increments `invalid_record_count` on the old segment.
    fn correct_write(
        &mut self,
        timestamp: i64,
        data: &[u8],
        cache: Option<&BlockCache>,
    ) -> Result<WriteOutcome> {
        match self.time_index.find_entry(timestamp)? {
            Some(entry) => {
                match self.segments.overwrite_in_last_block(
                    entry.block_offset,
                    entry.in_block_offset,
                    timestamp,
                    data,
                ) {
                    Ok(()) => {
                        // latest_written_timestamp unchanged; index unchanged.
                        self.invalidate_cache_for_entry(&entry, cache);
                        self.last_used_at = Instant::now();
                        Ok(WriteOutcome {
                            index_entry: entry,
                            branch: WriteBranch::Correction,
                        })
                    }
                    Err(_) => {
                        // Target block cannot be modified in place (sealed/compressed or
                        // not the last block/record); fall back to out-of-order write:
                        // append to latest segment, update index, increment invalid_record_count
                        self.out_of_order_write(timestamp, data, cache)
                    }
                }
            }
            None => Err(TmslError::NotFound(format!(
                "no index entry for correction timestamp {}",
                timestamp
            ))),
        }
    }

    /// Delete the record at the given timestamp.
    ///
    /// Marks the index entry as sentinel (block_offset = FILLER, in_block_offset = FILLER)
    /// and increments the data segment's `invalid_record_count` by 1.
    ///
    /// Returns `TmslError::NotFound` if:
    /// - `timestamp` is invalid (<= 0)
    /// - the dataset is empty
    /// - no entry exists at `timestamp`
    /// - the entry is already a filler (no real data)
    pub fn delete(&mut self, timestamp: i64) -> Result<()> {
        if self.runtime_context.read_only {
            return Err(TmslError::InvalidData(
                "read-only dataset cannot be deleted".into(),
            ));
        }
        let cache = self.runtime_context.block_cache.clone();
        let journal = self.runtime_context.journal.clone();
        let outcome = self.delete_with_cache_outcome(timestamp, cache.as_deref())?;
        if let Some(journal) = journal.as_ref() {
            journal.record_delete(&self.id, outcome.old_index_entry)?;
        }
        Ok(())
    }

    /// Delete a record and invalidate any global cache entry for the old block.
    pub(crate) fn delete_with_cache(
        &mut self,
        timestamp: i64,
        cache: Option<&BlockCache>,
    ) -> Result<()> {
        self.delete_with_cache_outcome(timestamp, cache).map(|_| ())
    }

    pub(crate) fn delete_with_cache_outcome(
        &mut self,
        timestamp: i64,
        cache: Option<&BlockCache>,
    ) -> Result<DeleteOutcome> {
        if timestamp <= 0 {
            return Err(TmslError::InvalidData("timestamp must be > 0".into()));
        }
        if self.latest_written_timestamp == 0 {
            return Err(TmslError::NotFound(format!(
                "no entry to delete at timestamp {} (dataset is empty)",
                timestamp
            )));
        }
        if self.is_timestamp_expired(timestamp) {
            return Err(self.expired_error(timestamp));
        }

        let old_entry = self.time_index.find_and_delete_entry(timestamp)?;
        self.invalidate_cache_for_entry(&old_entry, cache);
        // Old entry references real data; increment invalid_record_count on its segment
        self.segments
            .increment_invalid_record_count(old_entry.block_offset)?;

        self.last_used_at = Instant::now();
        Ok(DeleteOutcome {
            old_index_entry: old_entry,
        })
    }

    fn invalidate_cache_for_entry(&self, entry: &IndexEntry, cache: Option<&BlockCache>) {
        if entry.block_offset == BLOCK_OFFSET_FILLER {
            return;
        }
        if let Some(cache) = cache {
            let key = self
                .segments
                .cache_key_for_absolute_offset(entry.block_offset);
            cache.invalidate(&key);
        }
    }

    /// Read a single record by exact timestamp.
    ///
    /// Special case: `timestamp == -1` resolves to `latest_written_timestamp`.
    /// It does not search backward for the latest valid record, so deleted latest
    /// entries return `None`.
    ///
    /// Returns `Ok(Some((timestamp, data)))` if found, `Ok(None)` if not found
    /// or entry is a filler (deleted or never-written in continuous mode).
    pub fn read(&mut self, timestamp: i64) -> Result<Option<(i64, Vec<u8>)>> {
        let cache = self.runtime_context.block_cache.clone();
        self.read_with_cache(timestamp, cache.as_deref())
    }

    pub(crate) fn read_with_cache(
        &mut self,
        timestamp: i64,
        cache: Option<&BlockCache>,
    ) -> Result<Option<(i64, Vec<u8>)>> {
        let effective_ts = if timestamp == -1 {
            if self.latest_written_timestamp <= 0 {
                return Ok(None);
            }
            self.latest_written_timestamp
        } else {
            timestamp
        };

        if self.is_timestamp_expired(effective_ts) {
            return Ok(None);
        }

        let entry = match self.time_index.find_entry(effective_ts)? {
            Some(e) => e,
            None => return Ok(None),
        };
        if entry.block_offset == BLOCK_OFFSET_FILLER {
            return Ok(None);
        }
        let re = ReadIndexEntry {
            timestamp: entry.timestamp,
            block_offset: entry.block_offset,
            in_block_offset: entry.in_block_offset,
        };
        let (ts, data) = self.segments.read_at_index(&re, cache)?;
        self.last_used_at = Instant::now();
        Ok(Some((ts, data)))
    }

    /// Return a lazy query iterator for records in [start_ts, end_ts].
    #[allow(clippy::needless_lifetimes)]
    pub fn query_iter<'a>(&'a mut self, start_ts: i64, end_ts: i64) -> Result<QueryIterator<'a>> {
        let cache = self.runtime_context.block_cache.clone();
        self.query_iter_with_cache(start_ts, end_ts, cache)
    }

    pub(crate) fn query_iter_with_cache<'a>(
        &'a mut self,
        start_ts: i64,
        end_ts: i64,
        cache: Option<Arc<BlockCache>>,
    ) -> Result<QueryIterator<'a>> {
        let (start_ts, end_ts) = self.clamp_query_range(start_ts, end_ts);
        if start_ts > end_ts {
            return Ok(QueryIterator::new(vec![], &mut self.segments, cache));
        }
        let sources = self.time_index.prepare_query_sources(start_ts, end_ts)?;
        Ok(QueryIterator::new_with_sources(
            sources,
            &mut self.segments,
            cache,
        ))
    }

    /// Query records in the time range [start_ts, end_ts].
    /// Filler entries (sentinel block_offset) are skipped.
    #[allow(clippy::needless_lifetimes)]
    pub fn query(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<(i64, Vec<u8>)>> {
        let cache = self.runtime_context.block_cache.clone();
        let iter = self.query_iter_with_cache(start_ts, end_ts, cache)?;
        iter.collect_all()
    }

    pub fn query_index_entries(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<IndexEntry>> {
        let (start_ts, end_ts) = self.clamp_query_range(start_ts, end_ts);
        if start_ts > end_ts {
            return Ok(vec![]);
        }
        self.time_index.query(start_ts, end_ts)
    }

    pub fn query_sources(&mut self, start_ts: i64, end_ts: i64) -> Result<Vec<QuerySource>> {
        let (start_ts, end_ts) = self.clamp_query_range(start_ts, end_ts);
        if start_ts > end_ts {
            return Ok(Vec::new());
        }
        self.time_index.prepare_query_sources(start_ts, end_ts)
    }

    pub fn read_entry_at_index(&mut self, entry: &IndexEntry) -> Result<(i64, Vec<u8>)> {
        let cache = self.runtime_context.block_cache.clone();
        self.read_entry_at_index_with_cache(entry, cache.as_deref())
    }

    pub(crate) fn read_entry_at_index_with_cache(
        &mut self,
        entry: &IndexEntry,
        cache: Option<&BlockCache>,
    ) -> Result<(i64, Vec<u8>)> {
        if self.is_timestamp_expired(entry.timestamp) {
            return Err(self.expired_error(entry.timestamp));
        }
        let re = ReadIndexEntry {
            timestamp: entry.timestamp,
            block_offset: entry.block_offset,
            in_block_offset: entry.in_block_offset,
        };
        self.segments.read_at_index(&re, cache)
    }

    /// Flush all data.
    pub fn flush(&mut self) -> Result<()> {
        // Flush in-memory index buffer to disk
        self.time_index.flush_to_disk()?;
        self.segments.sync_all()?;
        self.time_index.sync_all()?;
        // Flush queue state files if open
        if let Some(ref inner) = self.queue_inner {
            if let Err(e) = flush_queue_state_files(inner) {
                log::warn!("[flush] queue state flush failed: {}", e);
            }
        }
        self.last_used_at = Instant::now();
        Ok(())
    }

    /// Close all segments.
    pub fn close(&mut self) -> Result<()> {
        self.flush()?;
        self.segments.idle_close_all()?;
        self.time_index.idle_close_all()?;
        Ok(())
    }

    /// Mark usage.
    pub fn touch(&mut self) {
        self.last_used_at = Instant::now();
    }

    /// Return the queue directory path.
    pub(crate) fn queue_dir(&self) -> PathBuf {
        queue_dir_for(&self.base_dir)
    }

    /// Open the queue subsystem for this dataset.
    ///
    /// Returns (QueueInner, CondvarPair) for the caller to construct DatasetQueue.
    pub fn open_queue(&mut self) -> Result<(Arc<Mutex<QueueInner>>, QueueCondvarPair)> {
        if self.queue_inner.is_some() {
            return Err(TmslError::QueueAlreadyOpen(format!(
                "queue already open for dataset {}",
                self.id.name
            )));
        }
        let q_dir = self.queue_dir();
        std::fs::create_dir_all(&q_dir)?;

        let inner = Arc::new(Mutex::new(QueueInner::new()));
        let pair: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
        self.queue_inner = Some(Arc::clone(&inner));
        self.queue_notify = Some(Arc::clone(&pair));
        Ok((inner, pair))
    }

    /// Close the queue subsystem.
    ///
    /// Syncs all consumer state files and marks the queue as closed.
    pub fn close_queue(&mut self) -> Result<()> {
        if let Some(inner) = self.queue_inner.take() {
            let guard = inner
                .lock()
                .map_err(|_| TmslError::InvalidData("queue inner mutex poisoned".into()))?;
            for sf in guard.consumers().values() {
                if let Ok(mut state) = sf.lock() {
                    let _ = state.sync_to_mmap();
                    let _ = state.flush();
                }
            }
            guard.close();
        }
        self.queue_inner = None;
        self.queue_notify = None;
        Ok(())
    }

    /// Recover the highest written timestamp from the newest materialized index
    /// position. Deleted/filler entries still define the written timestamp.
    fn recover_latest_timestamp(time_index: &TimeIndex) -> i64 {
        let latest_closed = time_index
            .closed_index_segments
            .iter()
            .filter(|meta| meta.wrote_count > 0)
            .max_by_key(|meta| meta.start_timestamp)
            .and_then(|meta| last_entry_timestamp(&meta.path).ok().flatten());

        let latest_open = time_index
            .index_segments
            .iter()
            .filter(|seg| seg.wrote_count > 0)
            .max_by_key(|seg| seg.start_timestamp)
            .and_then(Self::last_open_index_entry_timestamp);

        let latest_buffered = time_index
            .in_memory_buffer
            .iter()
            .map(|entry| entry.timestamp)
            .max();

        latest_closed
            .into_iter()
            .chain(latest_open)
            .chain(latest_buffered)
            .max()
            .unwrap_or(0)
    }

    fn last_open_index_entry_timestamp(seg: &IndexSegment) -> Option<i64> {
        if seg.wrote_count == 0 {
            return None;
        }
        let mmap = seg.mmap.as_ref()?;
        let pos = seg.header_size as usize + (seg.wrote_count - 1) * crate::index::INDEX_ENTRY_SIZE;
        let mmap_bytes = mmap.as_ref();
        if pos + 8 > mmap_bytes.len() {
            return None;
        }
        Some(i64::from_le_bytes(
            mmap_bytes[pos..pos + 8].try_into().unwrap(),
        ))
    }

    /// Get the base directory.
    pub fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }

    /// Get the last used time.
    pub fn last_used_at(&self) -> Instant {
        self.last_used_at
    }

    /// Data retention period (same unit as timestamp; 0 = no limit).
    pub fn retention_window(&self) -> u64 {
        self.retention_window
    }

    /// Latest successfully written timestamp (0 = dataset is empty).
    ///
    /// Recovered from index segments on `open`, then maintained in memory.
    /// Used by `read(-1)` shortcut and retention threshold calculation.
    pub fn latest_written_timestamp(&self) -> i64 {
        self.latest_written_timestamp
    }

    /// Clamp an inclusive query range to the data retention window.
    /// Returns (effective_start, effective_end). If retention is disabled
    /// or latest_written_timestamp is unknown, returns the original range.
    fn clamp_query_range(&self, start_ts: i64, end_ts: i64) -> (i64, i64) {
        if self.retention_window == 0 || self.latest_written_timestamp <= 0 {
            return (start_ts, end_ts);
        }
        let threshold = self
            .latest_written_timestamp
            .saturating_sub(self.retention_window as i64);
        (start_ts.max(threshold), end_ts)
    }

    /// Compute retention expiration threshold, or -1 if retention disabled / no data yet.
    fn retention_threshold(&self) -> i64 {
        if self.retention_window == 0 || self.latest_written_timestamp <= 0 {
            return -1;
        }
        self.latest_written_timestamp
            .saturating_sub(self.retention_window as i64)
    }

    fn is_timestamp_expired(&self, timestamp: i64) -> bool {
        let threshold = self.retention_threshold();
        threshold >= 0 && timestamp < threshold
    }

    fn expired_error(&self, timestamp: i64) -> TmslError {
        TmslError::Expired(format!(
            "timestamp {} is older than retention threshold {}",
            timestamp,
            self.retention_threshold()
        ))
    }

    /// Reclaim expired data & index segments whose entries fall entirely before the
    /// retention threshold. Closes the dataset first (all segments go to closed set).
    /// Returns the total number of segment files deleted.
    pub fn reclaim_expired_segments(&mut self) -> Result<usize> {
        let threshold = self.retention_threshold();
        if threshold < 0 {
            return Ok(0);
        }

        // Close all open segments so they appear in closed_segments / closed_index_segments
        self.close()?;

        // Reclaim index segments (read-only mmap per segment, released immediately)
        let idx_reclaimed = self
            .time_index
            .reclaim_expired_segments(threshold, self.config.index_segment_size)?;

        // Reclaim data segments (uses cached max_timestamp in closed_segments vec)
        let data_reclaimed = self.segments.reclaim_expired_segments(threshold)?;

        self.last_used_at = Instant::now();
        Ok(idx_reclaimed + data_reclaimed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join("timslite_dataset_test");
        let dir = d.join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn numeric_index_files(dir: &std::path::Path) -> Vec<i64> {
        let mut starts: Vec<i64> = std::fs::read_dir(dir.join("index"))
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.parse::<i64>().ok())
            })
            .collect();
        starts.sort_unstable();
        starts
    }

    fn make_cache_dataset(name: &str) -> DataSet {
        let dir = temp_dir(name);
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        DataSet::create(
            id,
            dir,
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap()
    }

    #[test]
    fn test_append_new_timestamp_creates_record() {
        let mut ds = make_cache_dataset("append_new_timestamp");

        let outcome = ds
            .append_with_cache_outcome(100, b"hello", None)
            .unwrap()
            .unwrap();

        assert_eq!(outcome.index_entry.timestamp, 100);
        assert_eq!(outcome.data_offset, 0);
        assert_eq!(outcome.data_len, 5);
        assert!(!outcome.migrated);
        assert_eq!(ds.latest_written_timestamp(), 100);
        assert_eq!(ds.read(100).unwrap().unwrap().1, b"hello");
    }

    #[test]
    fn test_append_latest_tail_in_place() {
        let mut ds = make_cache_dataset("append_latest_tail_in_place");
        ds.write(100, b"ab").unwrap();
        let before = {
            let seg = ds.segments.segments.last().unwrap();
            (
                seg.data_wrote_position,
                seg.pending_wrote_position,
                seg.total_uncompressed_size,
            )
        };

        let outcome = ds
            .append_with_cache_outcome(100, b"cd", None)
            .unwrap()
            .unwrap();

        assert_eq!(outcome.data_offset, 2);
        assert_eq!(outcome.data_len, 2);
        assert!(!outcome.migrated);
        assert_eq!(ds.latest_written_timestamp(), 100);
        assert_eq!(ds.read(100).unwrap().unwrap().1, b"abcd");
        let seg = ds.segments.segments.last().unwrap();
        assert_eq!(seg.data_wrote_position, before.0 + 2);
        assert_eq!(seg.pending_wrote_position, before.1 + 2);
        assert_eq!(seg.total_uncompressed_size, before.2 + 2);
        assert_eq!(seg.invalid_record_count, 0);
    }

    #[test]
    fn test_append_notifies_queue_only_when_creating_new_timestamp() {
        let mut ds = make_cache_dataset("append_notify_queue");
        let (_inner, notify) = ds.open_queue().unwrap();

        ds.append(100, b"first").unwrap();
        {
            let (lock, _) = &*notify;
            let mut flag = lock.lock().unwrap();
            assert!(*flag, "append creating a new timestamp must notify queue");
            *flag = false;
        }

        ds.append(100, b"_tail").unwrap();
        {
            let (lock, _) = &*notify;
            let flag = lock.lock().unwrap();
            assert!(
                !*flag,
                "append modifying the existing latest timestamp must not notify queue"
            );
        }
    }

    #[test]
    fn test_append_old_timestamp_returns_error() {
        let mut ds = make_cache_dataset("append_old_timestamp");
        ds.write(100, b"a").unwrap();
        ds.write(200, b"b").unwrap();

        assert!(ds.append(100, b"x").is_err());
        assert_eq!(ds.read(100).unwrap().unwrap().1, b"a");
    }

    #[test]
    fn test_empty_append_old_timestamp_returns_error_before_noop() {
        let mut ds = make_cache_dataset("empty_append_old_timestamp");
        ds.write(100, b"a").unwrap();
        ds.write(200, b"b").unwrap();

        assert!(
            ds.append(100, b"").is_err(),
            "empty append must still enforce timestamp ordering"
        );
        assert_eq!(ds.latest_written_timestamp(), 200);
        assert_eq!(ds.read(100).unwrap().unwrap().1, b"a");
    }

    #[test]
    fn test_append_new_timestamp_over_seventy_percent_uses_normal_write_path() {
        let mut ds = make_cache_dataset("append_new_over_70_percent");
        let data = vec![0x44; APPEND_MIGRATION_THRESHOLD + 1 - RECORD_HEADER_SIZE];

        let outcome = ds
            .append_with_cache_outcome(100, &data, None)
            .unwrap()
            .unwrap();

        assert!(!outcome.migrated);
        assert_eq!(outcome.data_offset, 0);
        assert_eq!(outcome.data_len, data.len() as u32);
        ds.write(200, b"tail").unwrap();
        let next = ds.time_index.find_entry(200).unwrap().unwrap();
        assert_eq!(next.block_offset, outcome.index_entry.block_offset);
        assert!(next.in_block_offset > 0);
    }

    #[test]
    fn test_append_compressed_latest_returns_error() {
        let mut ds = make_cache_dataset("append_compressed_latest");
        let data = vec![0xAB; 70_000];
        ds.write(100, &data).unwrap();

        assert!(ds.append(100, b"x").is_err());
        assert_eq!(ds.read(100).unwrap().unwrap().1, data);
    }

    #[test]
    fn test_append_crossing_threshold_migrates_to_single_record_block() {
        let mut ds = make_cache_dataset("append_threshold_migrates");
        let old_len = APPEND_MIGRATION_THRESHOLD - RECORD_HEADER_SIZE - 1;
        let old = vec![0x11; old_len];
        ds.write(100, &old).unwrap();

        let outcome = ds
            .append_with_cache_outcome(100, &[0x22, 0x33], None)
            .unwrap()
            .unwrap();

        assert!(outcome.migrated);
        assert_eq!(outcome.data_offset, old_len as u32);
        assert_eq!(outcome.data_len, 2);
        let mut expected = old;
        expected.extend_from_slice(&[0x22, 0x33]);
        assert_eq!(ds.read(100).unwrap().unwrap().1, expected);
        let seg = ds.segments.segments.last().unwrap();
        assert_eq!(seg.invalid_record_count, 1);
    }

    #[test]
    fn test_write_and_append_reject_record_over_4mib() {
        let mut ds = make_cache_dataset("append_4mib_limit");
        let too_large = vec![0u8; MAX_RECORD_DATA_SIZE + 1];
        assert!(ds.write(100, &too_large).is_err());

        ds.write(100, b"a").unwrap();
        let almost_too_large = vec![0u8; MAX_RECORD_DATA_SIZE];
        assert!(ds.append(100, &almost_too_large).is_err());
    }

    #[test]
    fn test_block_offset_routes_to_next_data_segment_after_rollover() {
        let dir = temp_dir("block_offset_segment_rollover");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        // Byte budget per segment:
        //   DATA_HEADER_SIZE = 116, BLOCK_HEADER_SIZE = 16, RECORD_OVERHEAD = 12
        //   Available payload = data_segment_size - DATA_HEADER_SIZE
        //   Per-record cost   = BLOCK_HEADER_SIZE + RECORD_OVERHEAD + data_len
        //                     = 16 + 12 + 32 = 60 bytes
        // With data_segment_size = 200:
        //   Available = 200 - 116 = 84 >= 60  (1st record fits, 24 bytes left)
        //   2nd record needs 60 > 24  → rollover to next segment.
        let data_segment_size = 200;
        let mut ds = DataSet::create(
            id,
            dir,
            data_segment_size,
            4 * 1024,
            6,
            0,
            data_segment_size,
            4 * 1024,
            0,
        )
        .unwrap();

        let first = vec![0x11; 32];
        let second = vec![0x22; 32];

        ds.write(10, &first).unwrap();
        ds.write(20, &second).unwrap();

        let index_entries = ds.query_index_entries(10, 20).unwrap();
        assert_eq!(index_entries.len(), 2);
        assert_eq!(index_entries[0].block_offset, 0);
        assert_eq!(index_entries[1].block_offset, data_segment_size);

        let rows = ds.query(10, 20).unwrap();
        assert_eq!(rows, vec![(10, first), (20, second)]);
    }

    #[test]
    fn test_continuous_first_write_does_not_fill_from_zero() {
        let dir = temp_dir("continuous_first_write_sparse");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();

        assert_eq!(ds.time_index.in_memory_buffer.len(), 1);
        assert_eq!(ds.time_index.in_memory_buffer[0].timestamp, 100);
        assert!(!ds.time_index.in_memory_buffer[0].is_filler());
        assert!(!dir.join("index").join("base").exists());
    }

    #[test]
    fn test_continuous_large_gap_filler_is_bounded_by_edge_segments() {
        let dir = temp_dir("continuous_large_gap_sparse");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let index_segment_size = 512;
        let segment_capacity = ((index_segment_size - crate::INDEX_HEADER_SIZE)
            / crate::INDEX_ENTRY_SIZE as u64) as usize;
        let first_ts = 10;
        let second_ts = first_ts + segment_capacity as i64 * 4 + 5;
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            index_segment_size,
            6,
            1,
            256 * 1024,
            128,
            0,
        )
        .unwrap();

        ds.write(first_ts, b"first").unwrap();
        ds.write(second_ts, b"second").unwrap();

        let filler_count = ds
            .time_index
            .in_memory_buffer
            .iter()
            .filter(|entry| entry.is_filler())
            .count();
        // Sparse continuous mode only materializes fillers at the edges of the
        // previous and current segments; middle segments are skipped entirely.
        //   prev_segment: fillers from (first_ts+1) to (prev_segment_start + capacity - 1)
        //   curr_segment: fillers from curr_segment_start to (second_ts - 1)
        let base = first_ts;
        let capacity = segment_capacity as i64;
        let prev_seg_start = base + ((first_ts - base) / capacity) * capacity;
        let curr_seg_start = base + ((second_ts - base) / capacity) * capacity;
        let prev_seg_end_fillers = (prev_seg_start + capacity - 1 - first_ts) as usize;
        let curr_seg_start_fillers = (second_ts - curr_seg_start) as usize;
        let expected_fillers = prev_seg_end_fillers + curr_seg_start_fillers;
        assert_eq!(
            filler_count, expected_fillers,
            "fillers should only cover edge-segment gaps, not the full gap"
        );
    }

    #[test]
    fn test_continuous_large_gap_flush_skips_middle_segments() {
        let dir = temp_dir("continuous_large_gap_flush");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let index_segment_size = 512;
        let segment_capacity = ((index_segment_size - crate::INDEX_HEADER_SIZE)
            / crate::INDEX_ENTRY_SIZE as u64) as i64;
        let first_ts = 10;
        let second_ts = first_ts + segment_capacity * 4 + 5;
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            index_segment_size,
            6,
            1,
            256 * 1024,
            128,
            0,
        )
        .unwrap();

        ds.write(first_ts, b"first").unwrap();
        ds.write(second_ts, b"second").unwrap();
        ds.flush().unwrap();

        assert!(!dir.join("index").join("base").exists());
        assert_eq!(
            numeric_index_files(&dir),
            vec![first_ts, first_ts + segment_capacity * 4]
        );
        assert!(ds.read(first_ts + segment_capacity * 2).unwrap().is_none());

        let entries = ds.query(first_ts, second_ts).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, first_ts);
        assert_eq!(entries[1].0, second_ts);
    }

    #[test]
    fn test_continuous_backfill_logical_hole_creates_target_segment() {
        let dir = temp_dir("continuous_backfill_logical_hole");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let index_segment_size = 512;
        let segment_capacity = ((index_segment_size - crate::INDEX_HEADER_SIZE)
            / crate::INDEX_ENTRY_SIZE as u64) as i64;
        let first_ts = 10;
        let hole_ts = first_ts + segment_capacity * 2;
        let second_ts = first_ts + segment_capacity * 4 + 5;

        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                index_segment_size,
                6,
                1,
                256 * 1024,
                128,
                0,
            )
            .unwrap();

            ds.write(first_ts, b"first").unwrap();
            ds.write(second_ts, b"second").unwrap();
            ds.flush().unwrap();
            ds.write(hole_ts, b"hole").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }

        assert_eq!(
            numeric_index_files(&dir),
            vec![
                first_ts,
                first_ts + segment_capacity * 2,
                first_ts + segment_capacity * 4
            ]
        );

        let mut ds = DataSet::open(id, dir.clone()).unwrap();
        assert_eq!(ds.latest_written_timestamp(), second_ts);
        let entries = ds.query(first_ts, second_ts).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, first_ts);
        assert_eq!(entries[1].0, hole_ts);
        assert_eq!(entries[1].1, b"hole");
        assert_eq!(entries[2].0, second_ts);
    }

    #[test]
    fn test_continuous_mode_filler_filling() {
        let dir = temp_dir("continuous_filler");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1,          // continuous
            256 * 1024, // initial_data_segment_size
            4 * 1024,   // initial_index_segment_size
            0,          // retention_window
        )
        .unwrap();

        // Write ts=100
        ds.write(100, b"hello").unwrap();
        assert_eq!(ds.latest_written_timestamp, 100);

        // Write ts=110 -> should fill ts=101..109 with filler
        ds.write(110, b"world").unwrap();
        assert_eq!(ds.latest_written_timestamp, 110);

        // Flush to disk
        ds.flush().unwrap();

        // Query should return only 2 real entries (filler filtered)
        let entries = ds.query(100, 110).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[1].0, 110);
    }

    #[test]
    fn test_continuous_mode_backfill_replaces_filler() {
        let dir = temp_dir("continuous_backfill");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1,          // continuous
            256 * 1024, // initial_data_segment_size
            4 * 1024,   // initial_index_segment_size
            0,          // retention_window
        )
        .unwrap();

        // Write ts=100
        ds.write(100, b"first").unwrap();
        // Write ts=150 -> fills ts=101..149
        ds.write(150, b"last").unwrap();

        // Back-fill ts=125 (replaces filler)
        ds.write(125, b"middle").unwrap();
        assert_eq!(ds.latest_written_timestamp, 150); // unchanged

        // Query should return 3 real entries
        let entries = ds.query(100, 150).unwrap();
        assert_eq!(entries.len(), 3);
        let ts_values: Vec<i64> = entries.iter().map(|e| e.0).collect();
        assert_eq!(ts_values, vec![100, 125, 150]);
    }

    #[test]
    fn test_correction_write_continuous_mode() {
        let dir = temp_dir("correction_continuous");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1, // continuous mode
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(150, b"second").unwrap();

        // Same ts=150: correction write (in-place overwrite)
        ds.write(150, b"corrected").unwrap();

        let entries = ds.query(100, 150).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].0, 150);
        assert_eq!(entries[1].1, b"corrected");
        // latest_written_timestamp should be unchanged
        assert_eq!(ds.latest_written_timestamp, 150);
    }

    #[test]
    fn test_correction_write_non_continuous_mode() {
        let dir = temp_dir("correction_noncontinuous");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0, // non-continuous mode
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(150, b"second").unwrap();

        // Same ts=150: correction write
        ds.write(150, b"corrected").unwrap();

        let entries = ds.query(100, 150).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].0, 150);
        assert_eq!(entries[1].1, b"corrected");
        assert_eq!(ds.latest_written_timestamp, 150);
    }

    #[test]
    fn test_correction_write_resize_larger() {
        let dir = temp_dir("correction_resize_larger");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"short").unwrap();

        // Resize to larger
        let big_data = vec![0xABu8; 200];
        ds.write(100, &big_data).unwrap();

        let entries = ds.query(100, 100).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.len(), 200);
        assert_eq!(entries[0].1, big_data);
    }

    #[test]
    fn test_correction_write_resize_smaller() {
        let dir = temp_dir("correction_resize_smaller");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        let big_data = vec![0xCDu8; 200];
        ds.write(100, &big_data).unwrap();

        // Resize to smaller
        ds.write(100, b"tiny").unwrap();

        let entries = ds.query(100, 100).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"tiny");
    }

    #[test]
    fn test_correction_write_multiple_times() {
        let dir = temp_dir("correction_multi");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"v1").unwrap();
        ds.write(100, b"v2_").unwrap();
        ds.write(100, b"v3__").unwrap();

        let entries = ds.query(100, 100).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"v3__");
    }

    #[test]
    fn test_correction_write_then_new_write() {
        let dir = temp_dir("correction_then_new");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(100, b"corrected_first").unwrap();
        ds.write(200, b"second").unwrap();

        let entries = ds.query(100, 200).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[0].1, b"corrected_first");
        assert_eq!(entries[1].0, 200);
        assert_eq!(entries[1].1, b"second");
    }

    #[test]
    fn test_correction_write_reopen_persistence() {
        let dir = temp_dir("correction_reopen");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                256 * 1024,
                4 * 1024,
                0,
            )
            .unwrap();

            ds.write(100, b"original").unwrap();
            ds.write(100, b"corrected").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }
        // Reopen and verify
        let mut ds = DataSet::open(id, dir.clone()).unwrap();
        let entries = ds.query(100, 100).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"corrected");
    }

    #[test]
    fn test_noncontinuous_mode_out_of_order_rejected_when_no_entry() {
        // In non-continuous mode, out-of-order write fails if there is no
        // existing index entry at the target timestamp.
        let dir = temp_dir("noncontinuous_ooo_no_entry");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0, // non-continuous
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(150, b"second").unwrap();

        // ts=120 was never written; no index entry; out-of-order write rejected
        let result = ds.write(120, b"middle");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("no index entry") || msg.contains("out-of-order"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_noncontinuous_mode_out_of_order_succeeds_with_existing_entry() {
        // In non-continuous mode, out-of-order write SUCCEEDS if an entry at
        // the target timestamp already exists. New data is appended; old data
        // becomes an orphan (invalid_record_count++).
        let dir = temp_dir("noncontinuous_ooo_with_entry");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0, // non-continuous
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(200, b"second").unwrap();
        // Out-of-order write at ts=100 (entry exists from earlier write)
        ds.write(100, b"updated_first").unwrap();

        assert_eq!(ds.latest_written_timestamp, 200); // unchanged
        let entries = ds.query(100, 200).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[0].1, b"updated_first");
        assert_eq!(entries[1].0, 200);
    }

    #[test]
    fn test_timestamp_zero_rejected() {
        let dir = temp_dir("ts_zero");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1,
            256 * 1024, // initial_data_segment_size
            4 * 1024,   // initial_index_segment_size
            0,          // retention_window
        )
        .unwrap();

        let result = ds.write(0, b"invalid");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("timestamp must be > 0"));

        // Also negative
        let result = ds.write(-1, b"invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_out_of_order_write_overwrites_real_entry() {
        // Out-of-order write at an existing real entry succeeds: data is
        // appended to latest segment, index entry is updated in place, and
        // the old data segment's invalid_record_count is incremented.
        let dir = temp_dir("ooo_overwrite_real");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1, // continuous
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(150, b"last").unwrap();

        // Out-of-order at ts=100 (real entry); succeeds via out_of_order_write
        ds.write(100, b"updated_first").unwrap();
        assert_eq!(ds.latest_written_timestamp, 150); // unchanged

        // Query should still return ts=100 and ts=150 with updated data
        let entries = ds.query(100, 150).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[0].1, b"updated_first");
        assert_eq!(entries[1].0, 150);
    }

    #[test]
    fn test_out_of_order_increments_invalid_record_count() {
        // Out-of-order writes that replace real data increment invalid_record_count
        // on the old data segment, and the count is persisted across reopen.
        let dir = temp_dir("ooo_invalid_count");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                1,
                256 * 1024,
                4 * 1024,
                0,
            )
            .unwrap();

            ds.write(100, b"v1").unwrap();
            ds.write(200, b"latest").unwrap();

            // Two out-of-order writes at ts=100; each increments invalid_record_count
            ds.write(100, b"v2").unwrap();
            ds.write(100, b"v3").unwrap();

            // The old data segment (only one segment here, everything fits) should have
            // invalid_record_count = 2 after two out-of-order writes.
            let seg = ds.segments.segments.last().unwrap();
            assert_eq!(
                seg.invalid_record_count, 2,
                "expected invalid_record_count=2, got {}",
                seg.invalid_record_count
            );

            ds.flush().unwrap();
            ds.close().unwrap();
        }
        // Reopen and verify the count persists. Trigger segment open via query.
        let mut ds2 = DataSet::open(id, dir).unwrap();
        // Query forces segment open; after open, invalid_record_count is read from file header.
        let entries = ds2.query(100, 200).unwrap();
        assert_eq!(entries.len(), 2); // ts=100 ("v3") and ts=200 ("latest")
        let seg2 = ds2.segments.segments.last().unwrap();
        assert_eq!(seg2.invalid_record_count, 2);
    }

    #[test]
    fn test_continuous_backfill_not_found() {
        let dir = temp_dir("backfill_nofound");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1,
            256 * 1024, // initial_data_segment_size
            4 * 1024,   // initial_index_segment_size
            0,          // retention_window
        )
        .unwrap();

        // Write ts=1 (filler range is empty since latest=0)
        ds.write(1, b"first").unwrap();
        // Write ts=10 -> fillers for ts=2..9
        ds.write(10, b"last").unwrap();

        // Backfill at ts=2 (which IS a filler) should succeed
        ds.write(2, b"filled").unwrap();
        assert_eq!(ds.latest_written_timestamp, 10); // unchanged

        // Verify 3 real entries
        let entries = ds.query(1, 10).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_continuous_open_recovery_latest_timestamp() {
        let dir = temp_dir("continuous_reopen");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };

        // Create and write
        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                1,
                256 * 1024, // initial_data_segment_size
                4 * 1024,   // initial_index_segment_size
                0,          // retention_window
            )
            .unwrap();
            ds.write(100, b"first").unwrap();
            ds.write(150, b"last").unwrap();
            ds.close().unwrap();
        }

        // Open and check latest_written_timestamp recovered
        let ds2 = DataSet::open(id, dir).unwrap();
        assert_eq!(ds2.latest_written_timestamp, 150);
    }

    #[test]
    fn test_retention_window_no_reclaim_when_zero() {
        let dir = temp_dir("retention_no_reclaim");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0, // retention_window = 0 (no limit)
        )
        .unwrap();

        // Write old data, then idle-close all segments
        ds.write(100, b"old").unwrap();
        ds.flush().unwrap();
        ds.segments.idle_close_all().unwrap();
        ds.time_index.idle_close_all().unwrap();

        // Write new data to force different segment
        ds.write(200, b"new").unwrap();

        // reclaim should do nothing because retention_window = 0
        let reclaimed = ds.reclaim_expired_segments().unwrap();
        assert_eq!(reclaimed, 0);
        assert!(ds.retention_window() == 0);
    }

    #[test]
    fn test_retention_window_stored_and_roundtrip() {
        let dir = temp_dir("retention_stored");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        // data_segment_size=188 forces one record per segment (same as reclaim test).
        // retention_window=15 → threshold = latest_ts(30) - 15 = 15.
        let data_segment_size = 188u64;
        let ret = 15u64;
        let mut ds = DataSet::create(
            id.clone(),
            dir.clone(),
            data_segment_size,
            4096,
            0,
            0,
            data_segment_size,
            4096,
            ret,
        )
        .unwrap();
        assert_eq!(ds.retention_window(), ret);

        // Write 3 records, each forcing a new segment
        ds.write(10, &[0xAA; 32]).unwrap();
        ds.write(20, &[0xBB; 32]).unwrap();
        ds.write(30, &[0xCC; 32]).unwrap();

        // Segment 0 (max_ts=10): 10 < 15 → expired
        // Segment 1 (max_ts=20): 20 >= 15 → kept
        // Segment 2 (max_ts=30): 30 >= 15 → kept
        let data_dir = dir.join("data");
        let count_before = std::fs::read_dir(&data_dir).unwrap().count();
        assert_eq!(count_before, 3);

        let reclaimed = ds.reclaim_expired_segments().unwrap();
        assert_eq!(reclaimed, 1, "segment with max_ts=10 should be expired");

        let count_after = std::fs::read_dir(&data_dir).unwrap().count();
        assert_eq!(count_after, 2, "one segment file should be deleted");

        // Reopen and verify retention_window persists
        let ds2 = DataSet::open(id, dir).unwrap();
        assert_eq!(ds2.retention_window(), ret);
    }

    #[test]
    fn test_retention_reclaim_basic() {
        let dir = temp_dir("retention_basic");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        // retention = 50 (same unit as timestamps)
        let mut ds = DataSet::create(
            id.clone(),
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            50,
        )
        .unwrap();

        // Write multiple records ALL within retention window of latest (200)
        // threshold = 200 - 50 = 150. All of [150, 200] must be queryable.
        ds.write(150, b"a").unwrap();
        ds.write(180, b"b").unwrap();
        ds.write(200, b"c").unwrap();

        assert_eq!(ds.latest_written_timestamp, 200);

        // Query [150, 200]; clamp to [max(150,150)=150, 200]; 3 records
        let entries = ds.query(150, 200).unwrap();
        assert_eq!(entries.len(), 3);

        // Query [100, 200]; clamp to [max(100,150)=150, 200]; 3 records
        let entries_before = ds.query(100, 200).unwrap();
        assert_eq!(entries_before.len(), 3);

        // Single data segment with max_ts=200 >= threshold; no reclaim
        let reclaimed = ds.reclaim_expired_segments().unwrap();
        assert_eq!(reclaimed, 0);

        // After reclaim, still queryable
        let entries_after = ds.query(150, 200).unwrap();
        assert_eq!(entries_after.len(), 3);
    }

    #[test]
    fn test_retention_reclaim_removes_all_when_expired() {
        // This test confirms that when retention is 0 (no limit), nothing is reclaimed
        // regardless of how old the data is.
        let dir = temp_dir("retention_zero_no_reclaim");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id.clone(),
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0, // retention_window = 0
        )
        .unwrap();

        ds.write(100, b"a").unwrap();
        ds.write(130, b"b").unwrap();
        ds.write(500, b"c").unwrap();

        let reclaimed = ds.reclaim_expired_segments().unwrap();
        assert_eq!(reclaimed, 0);

        let entries = ds.query(100, 500).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_retention_reclaim_actually_deletes_expired_segments() {
        let dir = temp_dir("retention_actual_reclaim");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        // data_segment_size=188, initial=188: each 32-byte record fills one segment.
        // total_needed = BLOCK_HEADER_SIZE(16) + RECORD_OVERHEAD(12) + 32 = 60.
        // Available = 188 - 124 = 64 >= 60 (fits), but 2nd record triggers rollover.
        let data_segment_size = 188u64;
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            data_segment_size,
            4096,              // index_segment_size
            0,                 // compress_level
            0,                 // index_continuous
            data_segment_size, // initial = segment_size
            4096,              // initial_index_segment_size
            15,                // retention_window: threshold = latest_ts - 15
        )
        .unwrap();

        // Write 3 records, each forcing a new segment
        ds.write(10, &[0xAA; 32]).unwrap();
        ds.write(20, &[0xBB; 32]).unwrap();
        ds.write(30, &[0xCC; 32]).unwrap();

        let data_dir = dir.join("data");
        let count_before = std::fs::read_dir(&data_dir).unwrap().count();
        assert_eq!(count_before, 3, "should have 3 data segment files");

        // retention_threshold = 30 - 15 = 15
        // Segment 0 (max_ts=10): 10 < 15 → expired, deleted
        // Segment 1 (max_ts=20): 20 >= 15 → kept
        // Segment 2 (max_ts=30): 30 >= 15 → kept
        let reclaimed = ds.reclaim_expired_segments().unwrap();
        assert_eq!(reclaimed, 1, "exactly 1 segment should be reclaimed");

        let count_after = std::fs::read_dir(&data_dir).unwrap().count();
        assert_eq!(
            count_after, 2,
            "should have 2 data segment files after reclaim"
        );

        // Verify the correct segment was deleted (segment at offset 0)
        let expired_path = data_dir.join(format!("{:020}", 0));
        assert!(
            !expired_path.exists(),
            "expired segment file should be physically deleted"
        );

        // Verify remaining segments are still present
        assert!(
            data_dir.join(format!("{:020}", data_segment_size)).exists(),
            "segment at offset {} should remain",
            data_segment_size
        );
        assert!(
            data_dir
                .join(format!("{:020}", data_segment_size * 2))
                .exists(),
            "segment at offset {} should remain",
            data_segment_size * 2
        );
    }

    #[test]
    fn test_retention_query_clamped() {
        let dir = temp_dir("retention_clamped");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            50, // retention_window = 50
        )
        .unwrap();

        // Write old ts=100, then new ts=200
        ds.write(100, b"old").unwrap();
        ds.write(200, b"new").unwrap();

        // threshold = 200 - 50 = 150
        // Query [100, 200] should be clamped to [150, 200], returning only 1 record
        let entries = ds.query(100, 200).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 200);

        // Query entirely within expired range; empty
        let empty = ds.query(50, 130).unwrap();
        assert!(empty.is_empty());

        // Query fully within valid range
        let valid = ds.query(180, 200).unwrap();
        assert_eq!(valid.len(), 1);
    }

    #[test]
    fn test_retention_read_expired_returns_none() {
        let dir = temp_dir("retention_read_expired");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir,
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            50,
        )
        .unwrap();

        ds.write(100, b"old").unwrap();
        ds.write(200, b"new").unwrap();

        assert!(ds.read(100).unwrap().is_none());
        assert_eq!(ds.read(200).unwrap().unwrap().1, b"new");
    }

    #[test]
    fn test_retention_read_entry_at_index_rejects_expired_entry() {
        let dir = temp_dir("retention_read_entry_expired");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir,
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            50,
        )
        .unwrap();

        ds.write(100, b"old").unwrap();
        let old_entry = ds.time_index.find_entry(100).unwrap().unwrap();
        ds.write(200, b"new").unwrap();

        let err = ds.read_entry_at_index(&old_entry).unwrap_err();
        assert!(matches!(err, TmslError::Expired(_)));
    }

    #[test]
    fn test_retention_delete_expired_rejected() {
        let dir = temp_dir("retention_delete_expired");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir,
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            50,
        )
        .unwrap();

        ds.write(100, b"old").unwrap();
        ds.write(200, b"new").unwrap();

        let err = ds.delete(100).unwrap_err();
        assert!(matches!(err, TmslError::Expired(_)));
        assert!(ds.read(100).unwrap().is_none());
    }

    #[test]
    fn test_retention_out_of_order_rewrite_expired_rejected() {
        let dir = temp_dir("retention_ooo_expired");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir,
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            50,
        )
        .unwrap();

        ds.write(100, b"old").unwrap();
        ds.write(200, b"new").unwrap();

        let err = ds.write(100, b"rewrite").unwrap_err();
        assert!(matches!(err, TmslError::Expired(_)));
        assert!(ds.read(100).unwrap().is_none());
    }

    #[test]
    fn test_delete_existing_entry() {
        let dir = temp_dir("delete_existing");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"a").unwrap();
        ds.write(200, b"b").unwrap();

        ds.delete(100).unwrap();

        // Query should return only ts=200
        let entries = ds.query(100, 200).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 200);
        assert_eq!(entries[0].1, b"b");
    }

    #[test]
    fn test_delete_filler_entry_error() {
        // In continuous mode, a filler position has no real data.
        // Delete should reject it with NotFound.
        let dir = temp_dir("delete_filler");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1, // continuous
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(1, b"first").unwrap();
        ds.write(5, b"last").unwrap();
        // ts=3 is a filler
        let result = ds.delete(3);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("filler"),
            "expected filler error"
        );
    }

    #[test]
    fn test_delete_nonexistent_error() {
        let dir = temp_dir("delete_nonexistent");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"a").unwrap();
        ds.write(200, b"b").unwrap();

        let result = ds.delete(999);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_idempotent_error() {
        // Deleting the same timestamp twice; second delete errors.
        let dir = temp_dir("delete_idempotent");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"a").unwrap();
        ds.delete(100).unwrap();

        // Second delete on same timestamp should fail
        let result = ds.delete(100);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_increments_invalid_record_count() {
        let dir = temp_dir("delete_increments_count");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"a").unwrap();
        ds.write(200, b"b").unwrap();
        ds.write(300, b"c").unwrap();

        ds.delete(100).unwrap();
        ds.delete(200).unwrap();

        // Both deletes target the same segment; count = 2
        let seg = ds.segments.segments.last().unwrap();
        assert_eq!(seg.invalid_record_count, 2);

        // Only ts=300 should remain queryable
        let entries = ds.query(100, 300).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 300);
    }

    #[test]
    fn test_delete_then_out_of_order_rewrite() {
        // After delete(ts), rewrite at ts becomes out-of-order; replaces filler.
        // invalid_record_count should NOT increase on the rewrite (filler to real).
        let dir = temp_dir("delete_then_ooo");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        // Continuous: writes ts=100 and ts=150 with filler in between
        ds.write(100, b"first").unwrap();
        ds.write(150, b"last").unwrap();

        ds.delete(100).unwrap();
        // After delete: entry at 100 is filler, invalid_record_count=1

        // Rewrite at ts=100: out-of-order, replaces filler (FILLER to real):
        // invalid_record_count unchanged
        ds.write(100, b"replaced").unwrap();

        let seg = ds.segments.segments.last().unwrap();
        assert_eq!(
            seg.invalid_record_count, 1,
            "expected 1, got {}",
            seg.invalid_record_count
        );

        let entries = ds.query(100, 150).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 100);
        assert_eq!(entries[0].1, b"replaced");
    }

    #[test]
    fn test_delete_persists_across_reopen() {
        let dir = temp_dir("delete_reopen");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                256 * 1024,
                4 * 1024,
                0,
            )
            .unwrap();
            ds.write(100, b"a").unwrap();
            ds.write(200, b"b").unwrap();
            ds.delete(100).unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }
        // Reopen
        let mut ds2 = DataSet::open(id, dir).unwrap();
        let entries = ds2.query(100, 200).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 200);

        // Reopened segment should preserve invalid_record_count
        let seg2 = ds2.segments.segments.last().unwrap();
        assert_eq!(seg2.invalid_record_count, 1);
    }

    #[test]
    fn test_correction_write_preserves_pending_after_reopen() {
        // close/open preserves pending raw state, so same-timestamp correction can
        // still overwrite in place after reopen.
        let dir = temp_dir("correction_pending_reopen");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                256 * 1024,
                4 * 1024,
                0,
            )
            .unwrap();
            ds.write(100, b"original").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }
        {
            let mut ds = DataSet::open(id, dir.clone()).unwrap();
            ds.write(100, b"corrected").unwrap();

            // Query should return the corrected data
            let entries = ds.query(100, 100).unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, 100);
            assert_eq!(entries[0].1, b"corrected");

            // latest_written_timestamp unchanged
            assert_eq!(ds.latest_written_timestamp, 100);

            let seg = ds.segments.segments.last().unwrap();
            assert_eq!(seg.invalid_record_count, 0);
        }
    }

    #[test]
    fn test_correction_write_falls_back_on_compressed_block() {
        // When a correction write targets a SEALED+COMPRESSED single-record block,
        // in-place overwrite fails and falls back to an out-of-order write.
        let dir = temp_dir("correction_fallback_compressed");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        // Exceed the fixed block payload limit to force a compressed single-record block.
        let compressible = vec![0xAB_u8; 70_000];
        ds.write(100, &compressible).unwrap();

        // Correction write at ts=100 targets a SEALED+COMPRESSED block and falls back.
        let corrected = vec![0xCD_u8; 70_100];
        ds.write(100, &corrected).unwrap();

        // Query should return the corrected data
        let entries = ds.query(100, 100).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, corrected);

        // latest_written_timestamp unchanged
        assert_eq!(ds.latest_written_timestamp, 100);

        // invalid_record_count should be 1 (old data orphaned)
        let seg = ds.segments.segments.last().unwrap();
        assert_eq!(seg.invalid_record_count, 1);
    }

    #[test]
    fn test_correction_fallback_invalidates_cached_compressed_block() {
        let mut ds = make_cache_dataset("cache_correction_fallback");
        let cache = BlockCache::new(1024 * 1024);
        let original = vec![0xAB_u8; 70_000];
        let corrected = vec![0xCD_u8; 70_100];

        ds.write(100, &original).unwrap();
        assert_eq!(
            ds.read_with_cache(100, Some(&cache)).unwrap().unwrap().1,
            original
        );
        assert_eq!(cache.stats().entry_count, 1);

        ds.write_with_cache(100, &corrected, Some(&cache)).unwrap();

        assert_eq!(cache.stats().entry_count, 0);
        assert_eq!(ds.read(100).unwrap().unwrap().1, corrected);
    }

    #[test]
    fn test_out_of_order_invalidates_cached_compressed_block() {
        let mut ds = make_cache_dataset("cache_out_of_order");
        let cache = BlockCache::new(1024 * 1024);
        let original = vec![0xAB_u8; 70_000];
        let updated = vec![0xEF_u8; 70_100];

        ds.write(100, &original).unwrap();
        ds.write(200, b"latest").unwrap();
        assert_eq!(
            ds.read_with_cache(100, Some(&cache)).unwrap().unwrap().1,
            original
        );
        assert_eq!(cache.stats().entry_count, 1);

        ds.write_with_cache(100, &updated, Some(&cache)).unwrap();

        assert_eq!(cache.stats().entry_count, 0);
        assert_eq!(ds.read(100).unwrap().unwrap().1, updated);
    }

    #[test]
    fn test_delete_invalidates_cached_compressed_block() {
        let mut ds = make_cache_dataset("cache_delete");
        let cache = BlockCache::new(1024 * 1024);
        let original = vec![0xAB_u8; 70_000];

        ds.write(100, &original).unwrap();
        assert_eq!(
            ds.read_with_cache(100, Some(&cache)).unwrap().unwrap().1,
            original
        );
        assert_eq!(cache.stats().entry_count, 1);

        ds.delete_with_cache(100, Some(&cache)).unwrap();

        assert_eq!(cache.stats().entry_count, 0);
        assert!(ds.read(100).unwrap().is_none());
    }

    #[test]
    fn test_correction_write_fallback_reopen_persistence() {
        // Correction-write fallback result must persist across close+reopen.
        let dir = temp_dir("correction_fallback_persist");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        // Phase 1: write multiple records, then close
        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                256 * 1024,
                4 * 1024,
                0,
            )
            .unwrap();
            ds.write(100, b"first").unwrap();
            ds.write(200, b"last").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }
        // Phase 2: reopen and rewrite ts=100. This is out-of-order because latest is 200.
        {
            let mut ds = DataSet::open(id.clone(), dir.clone()).unwrap();
            ds.write(100, b"corrected_100").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }
        // Phase 3: reopen and verify
        {
            let mut ds = DataSet::open(id, dir).unwrap();
            let entries = ds.query(100, 200).unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].0, 100);
            assert_eq!(entries[0].1, b"corrected_100");
            assert_eq!(entries[1].0, 200);
            assert_eq!(entries[1].1, b"last");

            let seg = ds.segments.segments.last().unwrap();
            assert_eq!(seg.invalid_record_count, 1);
        }
    }

    #[test]
    fn test_read_found() {
        let dir = temp_dir("read_found");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"hello").unwrap();
        ds.write(200, b"world").unwrap();

        // Read existing timestamp
        let result = ds.read(100).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 100);
        assert_eq!(data, b"hello");

        // Read second timestamp
        let result = ds.read(200).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 200);
        assert_eq!(data, b"world");
    }

    #[test]
    fn test_read_not_found() {
        let dir = temp_dir("read_not_found");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"hello").unwrap();

        // Read non-existent timestamp; None
        let result = ds.read(999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_deleted_returns_none() {
        let dir = temp_dir("read_deleted");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"hello").unwrap();
        ds.write(200, b"world").unwrap();

        // Delete ts=100
        ds.delete(100).unwrap();

        // Read deleted timestamp; None (filler)
        let result = ds.read(100).unwrap();
        assert!(result.is_none());

        // Other timestamp still readable
        let result = ds.read(200).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"world");
    }

    #[test]
    fn test_read_continuous_filler_returns_none() {
        let dir = temp_dir("read_continuous_filler");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            1, // continuous mode
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"hello").unwrap();
        ds.write(110, b"world").unwrap();
        ds.flush().unwrap();

        // Filler positions (101..109); None
        let result = ds.read(105).unwrap();
        assert!(result.is_none());

        // Real positions (100, 110); Some
        let result = ds.read(100).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"hello");

        let result = ds.read(110).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().1, b"world");
    }

    #[test]
    fn test_read_after_reopen() {
        let dir = temp_dir("read_reopen");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };

        // Phase 1: write + close
        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                256 * 1024,
                4 * 1024,
                0,
            )
            .unwrap();
            ds.write(100, b"persistent").unwrap();
            ds.write(200, b"data").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }

        // Phase 2: reopen and read
        {
            let mut ds = DataSet::open(id, dir).unwrap();

            let result = ds.read(100).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap().1, b"persistent");

            let result = ds.read(200).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap().1, b"data");

            let result = ds.read(999).unwrap();
            assert!(result.is_none());
        }
    }

    #[test]
    fn test_latest_written_timestamp_after_writes() {
        let dir = temp_dir("latest_ts_writes");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        // Empty dataset
        assert_eq!(ds.latest_written_timestamp(), 0);

        // First write sets latest
        ds.write(100, b"a").unwrap();
        assert_eq!(ds.latest_written_timestamp(), 100);

        ds.write(150, b"b").unwrap();
        assert_eq!(ds.latest_written_timestamp(), 150);

        // Correction write at 150 (== latest) keeps latest unchanged
        ds.write(150, b"corrected").unwrap();
        assert_eq!(ds.latest_written_timestamp(), 150);

        // Out-of-order write at an existing timestamp keeps latest unchanged
        ds.write(100, b"ooo_at_100").unwrap();
        assert_eq!(ds.latest_written_timestamp(), 150);
    }

    #[test]
    fn test_latest_written_timestamp_after_reopen() {
        let dir = temp_dir("latest_ts_reopen");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };

        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                256 * 1024,
                4 * 1024,
                0,
            )
            .unwrap();
            ds.write(100, b"a").unwrap();
            ds.write(250, b"b").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }

        // Reopen; latest_written_timestamp recovered from index
        {
            let ds = DataSet::open(id, dir).unwrap();
            assert_eq!(ds.latest_written_timestamp(), 250);
        }
    }

    #[test]
    fn test_read_minus_one_empty_dataset() {
        let dir = temp_dir("read_minus_one_empty");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        assert!(ds.read(-1).unwrap().is_none());
    }

    #[test]
    fn test_read_minus_one_returns_latest() {
        let dir = temp_dir("read_minus_one_latest");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id,
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(200, b"second").unwrap();
        ds.write(300, b"latest").unwrap();

        let result = ds.read(-1).unwrap();
        assert!(result.is_some());
        let (ts, data) = result.unwrap();
        assert_eq!(ts, 300);
        assert_eq!(data, b"latest");
    }

    #[test]
    fn test_read_minus_one_after_delete_latest() {
        // After deleting the latest, latest_written_timestamp still points to it
        // but the index entry is a filler, so read(-1) returns None.
        let dir = temp_dir("read_minus_one_deleted_latest");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let mut ds = DataSet::create(
            id.clone(),
            dir.clone(),
            64 * 1024 * 1024,
            4 * 1024 * 1024,
            6,
            0,
            256 * 1024,
            4 * 1024,
            0,
        )
        .unwrap();

        ds.write(100, b"first").unwrap();
        ds.write(200, b"later").unwrap();
        ds.delete(200).unwrap();

        assert_eq!(ds.latest_written_timestamp(), 200);
        assert!(ds.read(-1).unwrap().is_none());

        // Earlier record still reachable via explicit timestamp
        let r = ds.read(100).unwrap().unwrap();
        assert_eq!(r.1, b"first");

        ds.close().unwrap();

        let mut reopened = DataSet::open(id, dir).unwrap();
        assert_eq!(reopened.latest_written_timestamp(), 200);
        assert!(reopened.read(-1).unwrap().is_none());
    }

    #[test]
    fn test_retention_with_epoch_second_timestamps() {
        // Validates retention semantics using realistic Unix-epoch-second timestamps.
        // retention_window is in timestamp units; when timestamps are epoch seconds,
        // the window must also be in seconds.
        let dir = temp_dir("retention_epoch_secs");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };
        let data_segment_size = 188u64;
        let retention_window = 86_400u64; // 1 day in seconds
        let mut ds = DataSet::create(
            id.clone(),
            dir.clone(),
            data_segment_size,
            4096,
            0,
            0,
            data_segment_size,
            4096,
            retention_window,
        )
        .unwrap();

        // Day 1 (Nov 14 2023 ~10:00 UTC)
        let day1 = 1_700_000_000i64;
        ds.write(day1, &[0xAA; 32]).unwrap();

        // Day 2 (Nov 15 2023 ~10:00 UTC)
        let day2 = day1 + 86_400;
        ds.write(day2, &[0xBB; 32]).unwrap();

        // Day 3 (Nov 16 2023 ~10:00 UTC)
        let day3 = day2 + 86_400;
        ds.write(day3, &[0xCC; 32]).unwrap();

        // threshold = latest(=day3) - 86400 = day2
        // day1 segment has max_ts = day1 < day2 → expired
        let reclaimed = ds.reclaim_expired_segments().unwrap();
        assert!(reclaimed >= 1, "day-1 segment should be expired");

        // Reopen and verify day-1 data is gone but day-2 and day-3 survive
        let mut ds2 = DataSet::open(id, dir).unwrap();
        assert!(
            ds2.read(day1).unwrap().is_none(),
            "day-1 data should be reclaimed"
        );
        assert!(
            ds2.read(day2).unwrap().is_some(),
            "day-2 data should survive"
        );
        assert!(
            ds2.read(day3).unwrap().is_some(),
            "day-3 data should survive"
        );
    }

    #[test]
    fn test_read_minus_one_after_reopen() {
        let dir = temp_dir("read_minus_one_reopen");
        let id = DataSetKey {
            name: "test".into(),
            dataset_type: "data".into(),
        };

        {
            let mut ds = DataSet::create(
                id.clone(),
                dir.clone(),
                64 * 1024 * 1024,
                4 * 1024 * 1024,
                6,
                0,
                256 * 1024,
                4 * 1024,
                0,
            )
            .unwrap();
            ds.write(100, b"a").unwrap();
            ds.write(500, b"latest").unwrap();
            ds.flush().unwrap();
            ds.close().unwrap();
        }

        {
            let mut ds = DataSet::open(id, dir).unwrap();
            assert_eq!(ds.latest_written_timestamp(), 500);

            let r = ds.read(-1).unwrap().unwrap();
            assert_eq!(r.0, 500);
            assert_eq!(r.1, b"latest");
        }
    }
}
