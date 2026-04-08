use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use memmap2::MmapMut;
use parking_lot::RwLock;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::{Error, Result};

const FILE_MAGIC: &[u8; 4] = b"TMSL";

#[derive(Debug, Clone, Copy)]
pub struct FileHeader {
    pub version: u32,
    pub created_at: i64,
    pub file_size: u64,
    pub compress_type: u8,
    pub compress_level: u8,
}

impl FileHeader {
    pub fn new(file_size: u64, compress: bool, compress_level: u8) -> Self {
        Self {
            version: 1,
            created_at: chrono::Utc::now().timestamp(),
            file_size,
            compress_type: if compress { 1 } else { 0 },
            compress_level,
        }
    }

    pub const fn size() -> usize {
        26
    }
    pub const fn data_position() -> usize {
        Self::size() + 16
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::size());
        buf.extend_from_slice(FILE_MAGIC);
        buf.write_u32::<LittleEndian>(self.version).unwrap();
        buf.write_i64::<LittleEndian>(self.created_at).unwrap();
        buf.write_u64::<LittleEndian>(self.file_size).unwrap();
        buf.write_u8(self.compress_type).unwrap();
        buf.write_u8(self.compress_level).unwrap();
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::size() || &bytes[0..4] != FILE_MAGIC {
            return Err(Error::DataCorrupted("invalid header".into()));
        }
        let mut cursor = io::Cursor::new(&bytes[4..]);
        Ok(Self {
            version: cursor.read_u32::<LittleEndian>()?,
            created_at: cursor.read_i64::<LittleEndian>()?,
            file_size: cursor.read_u64::<LittleEndian>()?,
            compress_type: cursor.read_u8()?,
            compress_level: cursor.read_u8()?,
        })
    }
}

pub struct MappedFile {
    path: PathBuf,
    header: FileHeader,
    mmap: RwLock<Option<MmapMut>>,
    wrote_position: RwLock<u64>,
    data_size: RwLock<u64>,
    is_full: RwLock<bool>,
}

impl MappedFile {
    pub fn create(path: &Path, header: FileHeader) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(header.file_size)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let header_bytes = header.to_bytes();
        mmap[0..header_bytes.len()].copy_from_slice(&header_bytes);
        let state_offset = FileHeader::size();
        mmap[state_offset..state_offset + 8].copy_from_slice(&0u64.to_le_bytes());
        mmap[state_offset + 8..state_offset + 16].copy_from_slice(&0u64.to_le_bytes());
        mmap.flush()?;
        Ok(Self {
            path: path.to_path_buf(),
            header,
            mmap: RwLock::new(Some(mmap)),
            wrote_position: RwLock::new(FileHeader::data_position() as u64),
            data_size: RwLock::new(0),
            is_full: RwLock::new(false),
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let header = FileHeader::from_bytes(&mmap)?;
        let state_offset = FileHeader::size();
        let wrote_position =
            u64::from_le_bytes(mmap[state_offset..state_offset + 8].try_into().unwrap());
        let data_size = u64::from_le_bytes(
            mmap[state_offset + 8..state_offset + 16]
                .try_into()
                .unwrap(),
        );
        Ok(Self {
            path: path.to_path_buf(),
            header,
            mmap: RwLock::new(Some(mmap)),
            wrote_position: RwLock::new(wrote_position),
            data_size: RwLock::new(data_size),
            is_full: RwLock::new(false),
        })
    }

    pub fn file_from_offset(&self) -> i64 {
        self.path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    pub fn append(&self, data: &[u8]) -> Result<i64> {
        let mut mmap = self.mmap.write();
        let mmap = mmap
            .as_mut()
            .ok_or_else(|| Error::MappingError("file not open".into()))?;
        let wrote_position = *self.wrote_position.read();
        let needed = if self.header.compress_type == 1 {
            4 + data.len() + data.len() / 10
        } else {
            4 + data.len()
        };
        if wrote_position + needed as u64 > self.header.file_size {
            *self.is_full.write() = true;
            return Err(Error::DatasetFull);
        }
        let offset = wrote_position as i64;
        if self.header.compress_type == 1 {
            let mut encoder = DeflateEncoder::new(
                Vec::new(),
                Compression::new(self.header.compress_level as u32),
            );
            encoder.write_all(data)?;
            let compressed = encoder.finish()?;
            let size = compressed.len() as u32;
            mmap[wrote_position as usize..]
                .write_u32::<LittleEndian>(size)
                .unwrap();
            mmap[wrote_position as usize + 4..wrote_position as usize + 4 + compressed.len()]
                .copy_from_slice(&compressed);
            *self.wrote_position.write() = wrote_position + 4 + compressed.len() as u64;
            *self.data_size.write() += 4 + data.len() as u64;
        } else {
            mmap[wrote_position as usize..]
                .write_u32::<LittleEndian>(data.len() as u32)
                .unwrap();
            mmap[wrote_position as usize + 4..wrote_position as usize + 4 + data.len()]
                .copy_from_slice(data);
            *self.wrote_position.write() = wrote_position + 4 + data.len() as u64;
            *self.data_size.write() += 4 + data.len() as u64;
        }
        let state_offset = FileHeader::size();
        mmap[state_offset..]
            .write_u64::<LittleEndian>(*self.wrote_position.read())
            .unwrap();
        mmap[state_offset + 8..]
            .write_u64::<LittleEndian>(*self.data_size.read())
            .unwrap();
        Ok(offset)
    }

    pub fn read(&self, offset: u64) -> Result<Vec<u8>> {
        let mmap = self.mmap.read();
        let mmap = mmap
            .as_ref()
            .ok_or_else(|| Error::MappingError("file not open".into()))?;
        let wrote_position = *self.wrote_position.read();
        if offset >= wrote_position {
            return Err(Error::InvalidOffset(offset as i64));
        }
        let size = u32::from_le_bytes(
            mmap[offset as usize..offset as usize + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        if offset + 4 + size as u64 > wrote_position {
            return Err(Error::DataCorrupted("read beyond wrote position".into()));
        }
        let data = &mmap[offset as usize + 4..offset as usize + 4 + size];
        if self.header.compress_type == 1 {
            let mut decoder = DeflateDecoder::new(data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            Ok(decompressed)
        } else {
            Ok(data.to_vec())
        }
    }

    pub fn flush(&self) -> Result<()> {
        if let Some(mmap) = self.mmap.read().as_ref() {
            mmap.flush()?;
        }
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.flush()?;
        *self.mmap.write() = None;
        Ok(())
    }

    pub fn is_full(&self) -> bool {
        *self.is_full.read()
    }
    pub fn wrote_position(&self) -> u64 {
        *self.wrote_position.read()
    }
    pub fn header(&self) -> &FileHeader {
        &self.header
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn mmap(&self) -> &RwLock<Option<MmapMut>> {
        &self.mmap
    }
}

pub fn offset_to_filename(offset: i64) -> String {
    format!("{:020}", offset)
}
pub fn filename_to_offset(filename: &str) -> Option<i64> {
    filename.parse().ok()
}
