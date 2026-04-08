use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    pub default_file_size: usize,
    pub compression_level: u32,
    pub enable_compression: bool,
    pub flush_interval_secs: u64,
    pub max_idle_secs: u64,
    pub expiration_days: u32,
}

impl Config {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            default_file_size: 64 * 1024 * 1024,
            compression_level: 7,
            enable_compression: true,
            flush_interval_secs: 30,
            max_idle_secs: 1800,
            expiration_days: 0,
        }
    }

    pub fn set_file_size(mut self, size: usize) -> Self {
        self.default_file_size = size;
        self
    }

    pub fn set_compression_level(mut self, level: u32) -> Self {
        self.compression_level = level.clamp(0, 9);
        self
    }

    pub fn set_expiration_days(mut self, days: u32) -> Self {
        self.expiration_days = days;
        self
    }

    pub fn enable_compression(mut self, enable: bool) -> Self {
        self.enable_compression = enable;
        self
    }

    pub fn validate(&self) -> crate::Result<()> {
        if !self.data_dir.is_absolute() {
            return Err(crate::Error::InvalidConfig(
                "data_dir must be absolute".into(),
            ));
        }
        if self.compression_level > 9 {
            return Err(crate::Error::InvalidConfig(
                "compression_level must be 0-9".into(),
            ));
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new("/tmp/timslite")
    }
}
