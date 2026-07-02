use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use memmap2::MmapMut;

use crate::error::{Result, TmslError};
use crate::header::{TIMESTAMP_MAX_SENTINEL, TIMESTAMP_MIN_SENTINEL};
use crate::segment::SegmentStats;
use crate::util::{read_i64_from_mmap, read_u32_from_mmap, read_u64_from_mmap};

pub(crate) const DATASET_STATE_MAGIC: &[u8; 4] = b"DSSF";
pub(crate) const DATASET_STATE_VERSION: u32 = 1;
pub(crate) const DATASET_STATE_FILE_SIZE: u64 = 64;

const MAGIC_OFF: usize = 0;
const VERSION_OFF: usize = 4;
const ARCHIVED_UNTIL_OFF: usize = 8;
const MIN_TIMESTAMP_OFF: usize = 16;
const MAX_TIMESTAMP_OFF: usize = 24;
const TOTAL_RECORD_COUNT_OFF: usize = 32;
const TOTAL_DATA_SIZE_OFF: usize = 40;
const TOTAL_UNCOMPRESSED_SIZE_OFF: usize = 48;
const TOTAL_INVALID_RECORD_COUNT_OFF: usize = 56;

#[derive(Clone, Copy, Debug)]
pub(crate) struct DatasetStateSnapshot {
    pub archived_until_offset: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub total_record_count: u64,
    pub total_data_size: u64,
    pub total_uncompressed_size: u64,
    pub total_invalid_record_count: u64,
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
        if file.metadata()?.len() != DATASET_STATE_FILE_SIZE {
            return Err(TmslError::InvalidData(
                "invalid dataset state file size".into(),
            ));
        }
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        if &mmap[MAGIC_OFF..MAGIC_OFF + 4] != DATASET_STATE_MAGIC {
            return Err(TmslError::InvalidMagic);
        }
        let version = read_u32_from_mmap(&mmap, VERSION_OFF);
        if version != DATASET_STATE_VERSION {
            return Err(TmslError::InvalidData(format!(
                "unsupported dataset state version {}",
                version
            )));
        }
        let snapshot = DatasetStateSnapshot {
            archived_until_offset: read_u64_from_mmap(&mmap, ARCHIVED_UNTIL_OFF),
            min_timestamp: read_i64_from_mmap(&mmap, MIN_TIMESTAMP_OFF),
            max_timestamp: read_i64_from_mmap(&mmap, MAX_TIMESTAMP_OFF),
            total_record_count: read_u64_from_mmap(&mmap, TOTAL_RECORD_COUNT_OFF),
            total_data_size: read_u64_from_mmap(&mmap, TOTAL_DATA_SIZE_OFF),
            total_uncompressed_size: read_u64_from_mmap(&mmap, TOTAL_UNCOMPRESSED_SIZE_OFF),
            total_invalid_record_count: read_u64_from_mmap(&mmap, TOTAL_INVALID_RECORD_COUNT_OFF),
        };
        Ok(Self {
            path,
            mmap: Some(mmap),
            snapshot,
            is_flushed: true,
            read_only: false,
        })
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
        let mut state = Self::open_existing(path)?;
        state.read_only = true;
        Ok(state)
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
}
