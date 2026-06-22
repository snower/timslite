use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::StoreConfig;
use crate::error::{Result, TmslError};
use crate::journal::segment::JournalSegment;

const JOURNAL_META_FILE: &str = "meta";
const JOURNAL_DATA_DIR: &str = "data";

pub(crate) struct JournalLog {
    base_dir: PathBuf,
    data_dir: PathBuf,
    segments: BTreeMap<i64, JournalSegment>,
    next_sequence: i64,
    segment_size: u64,
    initial_segment_size: u64,
    compress_type: u8,
    compress_level: u8,
}

impl JournalLog {
    pub(crate) fn open_or_create(base_dir: PathBuf, config: &StoreConfig) -> Result<Self> {
        fs::create_dir_all(&base_dir)?;
        let data_dir = base_dir.join(JOURNAL_DATA_DIR);
        fs::create_dir_all(&data_dir)?;
        let meta_path = base_dir.join(JOURNAL_META_FILE);
        if !meta_path.exists() {
            fs::write(&meta_path, b"timslite journal v1\n")?;
        }

        let mut segments = BTreeMap::new();
        for entry in fs::read_dir(&data_dir)? {
            let path = entry?.path();
            if !path.is_file() {
                continue;
            }
            let Some(base_sequence) = parse_segment_file_name(&path) else {
                log::warn!("[journal] skipping invalid segment filename: {:?}", path);
                continue;
            };
            let segment = JournalSegment::open(&path, base_sequence, config.data_segment_size)?;
            segments.insert(base_sequence, segment);
        }

        let mut next_sequence = 1;
        if let Some((base, segment)) = segments.iter().next_back() {
            next_sequence = base
                .checked_add(i64::try_from(segment.record_count()).map_err(|_| {
                    TmslError::InvalidData("journal segment record_count exceeds i64".into())
                })?)
                .ok_or_else(|| TmslError::InvalidData("journal next_sequence overflow".into()))?;
        }

        Ok(Self {
            base_dir,
            data_dir,
            segments,
            next_sequence,
            segment_size: config.data_segment_size,
            initial_segment_size: config.initial_data_segment_size,
            compress_type: config.compress_type,
            compress_level: config.compress_level,
        })
    }

    pub(crate) fn open_read_only(base_dir: PathBuf, config: &StoreConfig) -> Result<Option<Self>> {
        let data_dir = base_dir.join(JOURNAL_DATA_DIR);
        if !base_dir.exists() || !data_dir.exists() {
            return Ok(None);
        }

        let mut segments = BTreeMap::new();
        for entry in fs::read_dir(&data_dir)? {
            let path = entry?.path();
            if !path.is_file() {
                continue;
            }
            let Some(base_sequence) = parse_segment_file_name(&path) else {
                log::warn!("[journal] skipping invalid segment filename: {:?}", path);
                continue;
            };
            let segment =
                JournalSegment::open_read_only(&path, base_sequence, config.data_segment_size)?;
            segments.insert(base_sequence, segment);
        }

        let mut next_sequence = 1;
        if let Some((base, segment)) = segments.iter().next_back() {
            next_sequence = base
                .checked_add(i64::try_from(segment.record_count()).map_err(|_| {
                    TmslError::InvalidData("journal segment record_count exceeds i64".into())
                })?)
                .ok_or_else(|| TmslError::InvalidData("journal next_sequence overflow".into()))?;
        }

        Ok(Some(Self {
            base_dir,
            data_dir,
            segments,
            next_sequence,
            segment_size: config.data_segment_size,
            initial_segment_size: config.initial_data_segment_size,
            compress_type: config.compress_type,
            compress_level: config.compress_level,
        }))
    }

    pub(crate) fn append(&mut self, payload: &[u8]) -> Result<i64> {
        if self.next_sequence <= 0 || self.next_sequence == i64::MAX {
            return Err(TmslError::InvalidData("journal sequence overflow".into()));
        }
        let sequence = self.next_sequence;
        if self.segments.is_empty() {
            self.create_segment(sequence)?;
        }
        let result = self
            .segments
            .last_entry()
            .ok_or_else(|| TmslError::InvalidData("journal has no writable segment".into()))?
            .get_mut()
            .append_record(sequence, payload);

        match result {
            Ok(()) => {}
            Err(TmslError::SegmentFull) => {
                if let Some((_, previous)) = self.segments.iter_mut().next_back() {
                    previous.sync()?;
                }
                self.create_segment(sequence)?;
                self.segments
                    .last_entry()
                    .ok_or_else(|| {
                        TmslError::InvalidData("journal has no segment after rollover".into())
                    })?
                    .get_mut()
                    .append_record(sequence, payload)?;
            }
            Err(err) => return Err(err),
        }

        self.next_sequence += 1;
        Ok(sequence)
    }

    pub(crate) fn read(&mut self, sequence: i64) -> Result<Option<(i64, Vec<u8>)>> {
        if sequence <= 0 || sequence >= self.next_sequence {
            return Ok(None);
        }
        let Some((_, segment)) = self.segments.range_mut(..=sequence).next_back() else {
            return Ok(None);
        };
        if sequence >= segment.base_sequence + segment.record_count() as i64 {
            return Ok(None);
        }
        segment
            .read(sequence)
            .map(|row| row.map(|data| (sequence, data)))
    }

    pub(crate) fn query(&mut self, start: i64, end: i64) -> Result<Vec<(i64, Vec<u8>)>> {
        if start > end {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        let mut sequence = start.max(1);
        let last = end.min(self.next_sequence - 1);
        while sequence <= last {
            if let Some(row) = self.read(sequence)? {
                rows.push(row);
            }
            sequence += 1;
        }
        Ok(rows)
    }

    pub(crate) fn latest_sequence(&self) -> Option<i64> {
        (self.next_sequence > 1).then_some(self.next_sequence - 1)
    }

    pub(crate) fn next_sequence(&self) -> i64 {
        self.next_sequence
    }

    pub(crate) fn flush_dirty(&mut self) -> Result<()> {
        for segment in self.segments.values_mut() {
            if !segment.is_flushed {
                segment.sync()?;
            }
        }
        Ok(())
    }

    fn create_segment(&mut self, base_sequence: i64) -> Result<()> {
        let path = self.data_dir.join(format_segment_file_name(base_sequence));
        let initial = self.initial_segment_size.min(self.segment_size);
        let segment = JournalSegment::create(
            &path,
            base_sequence,
            initial,
            self.segment_size,
            self.compress_level,
            self.compress_type,
        )?;
        self.segments.insert(base_sequence, segment);
        Ok(())
    }
}

fn format_segment_file_name(base_sequence: i64) -> String {
    format!("{base_sequence:020}")
}

fn parse_segment_file_name(path: &Path) -> Option<i64> {
    let name = path.file_name()?.to_str()?;
    if name.len() != 20 || !name.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let parsed = name.parse::<i64>().ok()?;
    (parsed > 0).then_some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StoreConfig;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "timslite_journal_log_{name}_{:?}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn small_config() -> StoreConfig {
        StoreConfig {
            data_segment_size: 420,
            initial_data_segment_size: 420,
            ..StoreConfig::default()
        }
    }

    #[test]
    fn journal_log_first_sequence_is_one() {
        let dir = temp_dir("first_sequence");
        let mut log = JournalLog::open_or_create(dir, &small_config()).unwrap();

        assert_eq!(log.latest_sequence(), None);
        assert_eq!(log.next_sequence(), 1);
        assert_eq!(log.append(b"first").unwrap(), 1);
        assert_eq!(log.latest_sequence(), Some(1));
        assert_eq!(log.next_sequence(), 2);
    }

    #[test]
    fn journal_log_recovers_next_sequence_from_latest_segment() {
        let dir = temp_dir("recover_next");
        {
            let mut log = JournalLog::open_or_create(dir.clone(), &small_config()).unwrap();
            assert_eq!(log.append(b"one").unwrap(), 1);
            assert_eq!(log.append(b"two").unwrap(), 2);
            log.flush_dirty().unwrap();
        }

        let mut reopened = JournalLog::open_or_create(dir, &small_config()).unwrap();

        assert_eq!(reopened.next_sequence(), 3);
        assert_eq!(reopened.read(2).unwrap().unwrap().1, b"two");
        assert_eq!(reopened.append(b"three").unwrap(), 3);
    }

    #[test]
    fn journal_log_reads_across_segments_without_index() {
        let dir = temp_dir("cross_segments");
        let mut log = JournalLog::open_or_create(dir.clone(), &small_config()).unwrap();
        let payload = vec![7u8; 180];

        for expected in 1..=4 {
            assert_eq!(log.append(&payload).unwrap(), expected);
        }

        assert!(dir.join("data").exists());
        assert!(!dir.join("index").exists());
        assert!(std::fs::read_dir(dir.join("data")).unwrap().count() > 1);
        assert_eq!(log.read(1).unwrap().unwrap().1, payload);
        assert_eq!(log.read(4).unwrap().unwrap().1, payload);
    }
}
