use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use memmap2::MmapMut;

use crate::error::{Result, TmslError};
use crate::header::{TIMESTAMP_MAX_SENTINEL, TIMESTAMP_MIN_SENTINEL};
use crate::segment::SegmentStats;
use crate::util::{read_i64_from_mmap, read_u32_from_mmap, read_u64_from_mmap};

pub(crate) const DATASET_STATE_MAGIC: &[u8; 4] = b"DSSF";
pub(crate) const DATASET_STATE_VERSION: u32 = 2;
pub(crate) const DATASET_STATE_FILE_SIZE: u64 = 72;
/// Version-1 state files lack the trailing `retention_floor` slot.
const DATASET_STATE_FILE_SIZE_V1: u64 = 64;
const DATASET_STATE_VERSION_V1: u32 = 1;

const MAGIC_OFF: usize = 0;
const VERSION_OFF: usize = 4;
const ARCHIVED_UNTIL_OFF: usize = 8;
const MIN_TIMESTAMP_OFF: usize = 16;
const MAX_TIMESTAMP_OFF: usize = 24;
const TOTAL_RECORD_COUNT_OFF: usize = 32;
const TOTAL_DATA_SIZE_OFF: usize = 40;
const TOTAL_UNCOMPRESSED_SIZE_OFF: usize = 48;
const TOTAL_INVALID_RECORD_COUNT_OFF: usize = 56;
const RETENTION_FLOOR_OFF: usize = 64;

#[derive(Clone, Copy, Debug)]
pub(crate) struct DatasetStateSnapshot {
    pub archived_until_offset: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub total_record_count: u64,
    pub total_data_size: u64,
    pub total_uncompressed_size: u64,
    pub total_invalid_record_count: u64,
    /// Monotonic wall-clock retention floor; TIMESTAMP_MIN_SENTINEL = never advanced.
    pub retention_floor: i64,
}

impl Default for DatasetStateSnapshot {
    fn default() -> Self {
        Self {
            archived_until_offset: 0,
            min_timestamp: TIMESTAMP_MIN_SENTINEL,
            max_timestamp: TIMESTAMP_MAX_SENTINEL,
            total_record_count: 0,
            total_data_size: 0,
            total_uncompressed_size: 0,
            total_invalid_record_count: 0,
            retention_floor: TIMESTAMP_MIN_SENTINEL,
        }
    }
}

pub(crate) struct DatasetStateFile {
    path: PathBuf,
    mmap: Option<MmapMut>,
    snapshot: DatasetStateSnapshot,
    is_flushed: bool,
    read_only: bool,
}

impl DatasetStateFile {
    pub(crate) fn open_or_create(dataset_dir: &Path) -> Result<Self> {
        let path = dataset_dir.join("state");
        if path.exists() {
            match Self::open_existing(path.clone()) {
                Ok(state) => return Ok(state),
                Err(e) => {
                    log::warn!(
                        "[dataset-state] recreating invalid state file {:?}: {}",
                        path,
                        e
                    );
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        Self::create_new(path)
    }

    fn create_new(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(DATASET_STATE_FILE_SIZE)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let snapshot = DatasetStateSnapshot::default();
        Self::write_snapshot_to_mmap(&mut mmap, snapshot);
        mmap.flush()?;
        Ok(Self {
            path,
            mmap: Some(mmap),
            snapshot,
            is_flushed: true,
            read_only: false,
        })
    }

    fn open_existing(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let file_len = file.metadata()?.len();
        if file_len != DATASET_STATE_FILE_SIZE_V1 && file_len != DATASET_STATE_FILE_SIZE {
            return Err(TmslError::InvalidData(
                "invalid dataset state file size".into(),
            ));
        }
        if file_len == DATASET_STATE_FILE_SIZE_V1 {
            file.set_len(DATASET_STATE_FILE_SIZE)?;
        }
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        if &mmap[MAGIC_OFF..MAGIC_OFF + 4] != DATASET_STATE_MAGIC {
            return Err(TmslError::InvalidMagic);
        }
        let version = read_u32_from_mmap(&mmap, VERSION_OFF);
        if version != DATASET_STATE_VERSION && version != DATASET_STATE_VERSION_V1 {
            return Err(TmslError::InvalidData(format!(
                "unsupported dataset state version {}",
                version
            )));
        }
        // A 72-byte file still stamped version 1 is an interrupted v1->v2
        // upgrade: the set_len zero-filled extend landed but the v2 stamp
        // was lost. Recover the v1 payload and finish the upgrade instead
        // of rejecting, which recreates the state and loses all stats.
        let needs_v2_stamp = Self::is_legacy_content(version, file_len);
        let snapshot = Self::read_snapshot(&mmap, needs_v2_stamp);
        if needs_v2_stamp {
            // Initialize the new slot and stamp version 2 before any further use.
            Self::write_snapshot_to_mmap(&mut mmap, snapshot);
            mmap.flush()?;
        }
        Ok(Self {
            path,
            mmap: Some(mmap),
            snapshot,
            is_flushed: true,
            read_only: false,
        })
    }

    /// True when the retention floor slot at offset 64 carries no v2 data:
    /// either a version-1 payload or a truncated 64-byte v2 image. Reading
    /// the raw slot there would fabricate floor 0 from set_len zero-fill.
    fn is_legacy_content(version: u32, file_len: u64) -> bool {
        version == DATASET_STATE_VERSION_V1 || file_len == DATASET_STATE_FILE_SIZE_V1
    }

    fn read_snapshot(bytes: &[u8], legacy_content: bool) -> DatasetStateSnapshot {
        DatasetStateSnapshot {
            archived_until_offset: read_u64_from_mmap(bytes, ARCHIVED_UNTIL_OFF),
            min_timestamp: read_i64_from_mmap(bytes, MIN_TIMESTAMP_OFF),
            max_timestamp: read_i64_from_mmap(bytes, MAX_TIMESTAMP_OFF),
            total_record_count: read_u64_from_mmap(bytes, TOTAL_RECORD_COUNT_OFF),
            total_data_size: read_u64_from_mmap(bytes, TOTAL_DATA_SIZE_OFF),
            total_uncompressed_size: read_u64_from_mmap(bytes, TOTAL_UNCOMPRESSED_SIZE_OFF),
            total_invalid_record_count: read_u64_from_mmap(bytes, TOTAL_INVALID_RECORD_COUNT_OFF),
            retention_floor: if legacy_content {
                TIMESTAMP_MIN_SENTINEL
            } else {
                read_i64_from_mmap(bytes, RETENTION_FLOOR_OFF)
            },
        }
    }

    /// Parse a state image without any file mutation: read-only mode must
    /// never upgrade v1 files nor complete interrupted v1->v2 upgrades.
    fn parse_snapshot_in_place(bytes: &[u8]) -> Result<DatasetStateSnapshot> {
        let file_len = bytes.len() as u64;
        if file_len != DATASET_STATE_FILE_SIZE_V1 && file_len != DATASET_STATE_FILE_SIZE {
            return Err(TmslError::InvalidData(
                "invalid dataset state file size".into(),
            ));
        }
        if &bytes[MAGIC_OFF..MAGIC_OFF + 4] != DATASET_STATE_MAGIC {
            return Err(TmslError::InvalidMagic);
        }
        let version = read_u32_from_mmap(bytes, VERSION_OFF);
        if version != DATASET_STATE_VERSION && version != DATASET_STATE_VERSION_V1 {
            return Err(TmslError::InvalidData(format!(
                "unsupported dataset state version {}",
                version
            )));
        }
        Ok(Self::read_snapshot(
            bytes,
            Self::is_legacy_content(version, file_len),
        ))
    }

    pub(crate) fn open_read_only_or_default(dataset_dir: &Path) -> Result<Self> {
        let path = dataset_dir.join("state");
        if !path.exists() {
            return Ok(Self {
                path,
                mmap: None,
                snapshot: DatasetStateSnapshot::default(),
                is_flushed: true,
                read_only: true,
            });
        }
        let bytes = std::fs::read(&path)?;
        let snapshot = Self::parse_snapshot_in_place(&bytes)?;
        Ok(Self {
            path,
            mmap: None,
            snapshot,
            is_flushed: true,
            read_only: true,
        })
    }

    pub(crate) fn snapshot(&self) -> DatasetStateSnapshot {
        self.snapshot
    }

    pub(crate) fn archived_until_offset(&self) -> u64 {
        self.snapshot.archived_until_offset
    }

    pub(crate) fn is_dirty(&self) -> bool {
        !self.is_flushed
    }

    pub(crate) fn archive_data_segments(
        &mut self,
        archived_until_offset: u64,
        segments: &[SegmentStats],
    ) -> Result<()> {
        if archived_until_offset <= self.snapshot.archived_until_offset && segments.is_empty() {
            return Ok(());
        }
        for stats in segments {
            self.snapshot.total_record_count = self
                .snapshot
                .total_record_count
                .saturating_add(stats.record_count);
            self.snapshot.total_data_size = self
                .snapshot
                .total_data_size
                .saturating_add(stats.data_size);
            self.snapshot.total_uncompressed_size = self
                .snapshot
                .total_uncompressed_size
                .saturating_add(stats.total_uncompressed_size);
            self.snapshot.total_invalid_record_count = self
                .snapshot
                .total_invalid_record_count
                .saturating_add(stats.invalid_record_count);
        }
        self.snapshot.archived_until_offset = archived_until_offset;
        self.flush_snapshot()?;
        Ok(())
    }

    pub(crate) fn subtract_data_segment(&mut self, stats: SegmentStats) -> Result<()> {
        self.snapshot.total_record_count = self
            .snapshot
            .total_record_count
            .saturating_sub(stats.record_count);
        self.snapshot.total_data_size = self
            .snapshot
            .total_data_size
            .saturating_sub(stats.data_size);
        self.snapshot.total_uncompressed_size = self
            .snapshot
            .total_uncompressed_size
            .saturating_sub(stats.total_uncompressed_size);
        self.snapshot.total_invalid_record_count = self
            .snapshot
            .total_invalid_record_count
            .saturating_sub(stats.invalid_record_count);
        self.flush_snapshot()?;
        Ok(())
    }

    /// Raise the persisted retention floor to `candidate` (monotonic) and flush
    /// synchronously. `TIMESTAMP_MIN_SENTINEL` means "no floor" and never
    /// advances or lowers the stored value. Returns the effective floor.
    pub(crate) fn advance_retention_floor(&mut self, candidate: i64) -> Result<i64> {
        if self.read_only {
            return Err(TmslError::InvalidData(
                "read-only dataset state cannot advance retention floor".into(),
            ));
        }
        let current = self.snapshot.retention_floor;
        let next = if candidate == TIMESTAMP_MIN_SENTINEL {
            current
        } else if current == TIMESTAMP_MIN_SENTINEL {
            candidate
        } else {
            current.max(candidate)
        };
        if next != current {
            self.snapshot.retention_floor = next;
            self.flush_snapshot()?;
        }
        Ok(next)
    }

    pub(crate) fn add_invalid_record(&mut self) -> Result<()> {
        self.snapshot.total_invalid_record_count =
            self.snapshot.total_invalid_record_count.saturating_add(1);
        self.flush_snapshot()?;
        Ok(())
    }

    pub(crate) fn set_timestamp_range(
        &mut self,
        min_timestamp: i64,
        max_timestamp: i64,
    ) -> Result<()> {
        if self.snapshot.min_timestamp == min_timestamp
            && self.snapshot.max_timestamp == max_timestamp
        {
            return Ok(());
        }
        self.snapshot.min_timestamp = min_timestamp;
        self.snapshot.max_timestamp = max_timestamp;
        self.flush_snapshot()?;
        Ok(())
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        if self.read_only {
            self.is_flushed = true;
            return Ok(());
        }
        if !self.is_flushed {
            let mmap = self
                .mmap
                .as_mut()
                .ok_or_else(|| TmslError::MmapError("dataset state file is not mapped".into()))?;
            Self::write_snapshot_to_mmap(mmap, self.snapshot);
            mmap.flush()?;
            self.is_flushed = true;
        }
        Ok(())
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        self.sync()?;
        self.mmap = None;
        Ok(())
    }

    fn flush_snapshot(&mut self) -> Result<()> {
        if self.read_only {
            self.is_flushed = true;
            return Ok(());
        }
        let mmap = self
            .mmap
            .as_mut()
            .ok_or_else(|| TmslError::MmapError("dataset state file is not mapped".into()))?;
        Self::write_snapshot_to_mmap(mmap, self.snapshot);
        mmap.flush()?;
        self.is_flushed = true;
        Ok(())
    }

    fn write_snapshot_to_mmap(mmap: &mut [u8], snapshot: DatasetStateSnapshot) {
        mmap[MAGIC_OFF..MAGIC_OFF + 4].copy_from_slice(DATASET_STATE_MAGIC);
        mmap[VERSION_OFF..VERSION_OFF + 4].copy_from_slice(&DATASET_STATE_VERSION.to_le_bytes());
        mmap[ARCHIVED_UNTIL_OFF..ARCHIVED_UNTIL_OFF + 8]
            .copy_from_slice(&snapshot.archived_until_offset.to_le_bytes());
        mmap[MIN_TIMESTAMP_OFF..MIN_TIMESTAMP_OFF + 8]
            .copy_from_slice(&snapshot.min_timestamp.to_le_bytes());
        mmap[MAX_TIMESTAMP_OFF..MAX_TIMESTAMP_OFF + 8]
            .copy_from_slice(&snapshot.max_timestamp.to_le_bytes());
        mmap[TOTAL_RECORD_COUNT_OFF..TOTAL_RECORD_COUNT_OFF + 8]
            .copy_from_slice(&snapshot.total_record_count.to_le_bytes());
        mmap[TOTAL_DATA_SIZE_OFF..TOTAL_DATA_SIZE_OFF + 8]
            .copy_from_slice(&snapshot.total_data_size.to_le_bytes());
        mmap[TOTAL_UNCOMPRESSED_SIZE_OFF..TOTAL_UNCOMPRESSED_SIZE_OFF + 8]
            .copy_from_slice(&snapshot.total_uncompressed_size.to_le_bytes());
        mmap[TOTAL_INVALID_RECORD_COUNT_OFF..TOTAL_INVALID_RECORD_COUNT_OFF + 8]
            .copy_from_slice(&snapshot.total_invalid_record_count.to_le_bytes());
        mmap[RETENTION_FLOOR_OFF..RETENTION_FLOOR_OFF + 8]
            .copy_from_slice(&snapshot.retention_floor.to_le_bytes());
    }
}

impl Drop for DatasetStateFile {
    fn drop(&mut self) {
        if let Err(e) = self.sync() {
            log::error!(
                "[dataset-state] failed to flush state file {:?} on drop: {}",
                self.path,
                e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dataset_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "timslite_dataset_state_{name}_{:?}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn state_mutations_flush_immediately() {
        let dir = temp_dataset_dir("immediate_flush");
        let mut state = DatasetStateFile::open_or_create(&dir).unwrap();

        state.set_timestamp_range(10, 20).unwrap();
        assert!(!state.is_dirty());

        let reopened = DatasetStateFile::open_or_create(&dir).unwrap();
        let snapshot = reopened.snapshot();
        assert_eq!(snapshot.min_timestamp, 10);
        assert_eq!(snapshot.max_timestamp, 20);
    }

    #[test]
    fn retention_floor_defaults_to_no_floor_sentinel() {
        let dir = temp_dataset_dir("floor_default");
        let state = DatasetStateFile::open_or_create(&dir).unwrap();
        assert_eq!(state.snapshot().retention_floor, TIMESTAMP_MIN_SENTINEL);
    }

    #[test]
    fn retention_floor_survives_state_reopen() {
        let dir = temp_dataset_dir("floor_reopen");
        {
            let mut state = DatasetStateFile::open_or_create(&dir).unwrap();
            assert_eq!(state.advance_retention_floor(100).unwrap(), 100);
            assert!(!state.is_dirty(), "advance must flush synchronously");
        }
        let reopened = DatasetStateFile::open_or_create(&dir).unwrap();
        assert_eq!(reopened.snapshot().retention_floor, 100);
    }

    #[test]
    fn retention_floor_never_moves_backward() {
        let dir = temp_dataset_dir("floor_monotonic");
        let mut state = DatasetStateFile::open_or_create(&dir).unwrap();
        assert_eq!(state.advance_retention_floor(100).unwrap(), 100);
        assert_eq!(state.advance_retention_floor(99).unwrap(), 100);
        assert_eq!(
            state
                .advance_retention_floor(TIMESTAMP_MIN_SENTINEL)
                .unwrap(),
            100
        );
        assert_eq!(state.snapshot().retention_floor, 100);
    }

    #[test]
    fn retention_floor_read_only_advance_is_rejected() {
        let dir = temp_dataset_dir("floor_read_only");
        {
            let mut state = DatasetStateFile::open_or_create(&dir).unwrap();
            state.advance_retention_floor(100).unwrap();
        }
        let mut ro = DatasetStateFile::open_read_only_or_default(&dir).unwrap();
        assert!(ro.advance_retention_floor(500).is_err());
        assert_eq!(ro.snapshot().retention_floor, 100);
        drop(ro);
        let after = DatasetStateFile::open_or_create(&dir).unwrap();
        assert_eq!(after.snapshot().retention_floor, 100);
    }

    /// Snapshot with non-default stats so accidental recreate/drop is visible.
    fn sample_stats_snapshot() -> DatasetStateSnapshot {
        DatasetStateSnapshot {
            archived_until_offset: 4096,
            min_timestamp: 100,
            max_timestamp: 900,
            total_record_count: 7,
            total_data_size: 700,
            total_uncompressed_size: 1400,
            total_invalid_record_count: 2,
            retention_floor: TIMESTAMP_MIN_SENTINEL,
        }
    }

    fn assert_sample_stats(snap: &DatasetStateSnapshot) {
        assert_eq!(snap.archived_until_offset, 4096);
        assert_eq!(snap.min_timestamp, 100);
        assert_eq!(snap.max_timestamp, 900);
        assert_eq!(snap.total_record_count, 7);
        assert_eq!(snap.total_data_size, 700);
        assert_eq!(snap.total_uncompressed_size, 1400);
        assert_eq!(snap.total_invalid_record_count, 2);
        // v1 payloads have no floor slot; recovery must default to the
        // sentinel, never the raw zero-fill which would mean floor = 0.
        assert_eq!(snap.retention_floor, TIMESTAMP_MIN_SENTINEL);
    }

    /// 72-byte image of an interrupted v1->v2 upgrade: `set_len` landed and
    /// zero-filled the trailing slot, but the v2 stamp was never flushed,
    /// so the header still reads version 1.
    fn interrupted_upgrade_bytes() -> Vec<u8> {
        let mut buf = vec![0u8; DATASET_STATE_FILE_SIZE as usize];
        DatasetStateFile::write_snapshot_to_mmap(&mut buf, sample_stats_snapshot());
        buf[VERSION_OFF..VERSION_OFF + 4].copy_from_slice(&DATASET_STATE_VERSION_V1.to_le_bytes());
        buf[RETENTION_FLOOR_OFF..].fill(0);
        buf
    }

    fn write_state_file(dir: &Path, bytes: &[u8]) {
        std::fs::write(dir.join("state"), bytes).unwrap();
    }

    fn read_state_file(dir: &Path) -> Vec<u8> {
        std::fs::read(dir.join("state")).unwrap()
    }

    #[test]
    fn interrupted_v1_to_v2_extend_recovers_stats_on_writable_open() {
        let dir = temp_dataset_dir("interrupted_extend");
        write_state_file(&dir, &interrupted_upgrade_bytes());

        let state = DatasetStateFile::open_or_create(&dir).unwrap();
        assert_sample_stats(&state.snapshot());
        drop(state);

        // Recovery must complete the upgrade in place, not recreate the file.
        let after = read_state_file(&dir);
        assert_eq!(after.len(), DATASET_STATE_FILE_SIZE as usize);
        assert_eq!(&after[MAGIC_OFF..MAGIC_OFF + 4], DATASET_STATE_MAGIC);
        assert_eq!(
            read_u32_from_mmap(&after, VERSION_OFF),
            DATASET_STATE_VERSION
        );
        assert_eq!(
            read_i64_from_mmap(&after, RETENTION_FLOOR_OFF),
            TIMESTAMP_MIN_SENTINEL
        );
    }

    #[test]
    fn v1_sized_file_still_migrates_on_writable_open() {
        let dir = temp_dataset_dir("v1_sized_migrate");
        let mut v1 = interrupted_upgrade_bytes();
        v1.truncate(DATASET_STATE_FILE_SIZE_V1 as usize);
        write_state_file(&dir, &v1);

        let state = DatasetStateFile::open_or_create(&dir).unwrap();
        assert_sample_stats(&state.snapshot());
        drop(state);

        let after = read_state_file(&dir);
        assert_eq!(after.len(), DATASET_STATE_FILE_SIZE as usize);
        assert_eq!(
            read_u32_from_mmap(&after, VERSION_OFF),
            DATASET_STATE_VERSION
        );
    }

    #[test]
    fn read_only_open_never_mutates_v1_file() {
        let dir = temp_dataset_dir("read_only_v1");
        let mut v1 = interrupted_upgrade_bytes();
        v1.truncate(DATASET_STATE_FILE_SIZE_V1 as usize);
        write_state_file(&dir, &v1);

        let state = DatasetStateFile::open_read_only_or_default(&dir).unwrap();
        assert_sample_stats(&state.snapshot());
        drop(state);

        // Read-only open must not upgrade, rewrite or resize the file.
        assert_eq!(read_state_file(&dir), v1);
    }

    #[test]
    fn read_only_open_recovers_interrupted_upgrade_without_writing() {
        let dir = temp_dataset_dir("read_only_interrupted");
        let broken = interrupted_upgrade_bytes();
        write_state_file(&dir, &broken);

        let state = DatasetStateFile::open_read_only_or_default(&dir).unwrap();
        assert_sample_stats(&state.snapshot());
        drop(state);

        assert_eq!(read_state_file(&dir), broken);
    }
}
