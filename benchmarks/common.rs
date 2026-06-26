use rand::Rng;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

pub struct LogData {
    pub raw_lines: Vec<String>,
}

impl LogData {
    pub fn load() -> Self {
        let log_path = Path::new(".github/data/access.log");

        let raw_lines = if log_path.exists() {
            let content =
                fs::read_to_string(log_path).expect("Failed to read .github/data/access.log");
            content
                .lines()
                .map(|l| l.trim().to_string())
                .filter(|l| !l.is_empty())
                .collect()
        } else {
            Self::generate_random_lines(1000)
        };

        LogData { raw_lines }
    }

    fn generate_random_lines(count: usize) -> Vec<String> {
        let mut rng = rand::thread_rng();
        let methods = ["GET", "POST", "PUT", "DELETE"];
        let paths = [
            "/api/users",
            "/api/data",
            "/index.html",
            "/api/search",
            "/api/login",
        ];
        let statuses = [200, 201, 301, 304, 400, 404, 500];
        let user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "curl/7.68.0",
        ];

        (0..count)
            .map(|_| {
                let ip = format!(
                    "{}.{}.{}.{}",
                    rng.gen_range(1..255),
                    rng.gen_range(0..255),
                    rng.gen_range(0..255),
                    rng.gen_range(1..255)
                );
                let method = methods[rng.gen_range(0..methods.len())];
                let path = paths[rng.gen_range(0..paths.len())];
                let status = statuses[rng.gen_range(0..statuses.len())];
                let size = rng.gen_range(0..10000);
                let ua = user_agents[rng.gen_range(0..user_agents.len())];
                format!(
                    r#"{} - - [26/Jun/2026:09:13:48 +0800] "{} /{} HTTP/2.0" {} {} "-" "{}""#,
                    ip, method, path, status, size, ua
                )
            })
            .collect()
    }

    pub fn random_raw_line(&self) -> &str {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..self.raw_lines.len());
        &self.raw_lines[index]
    }
}

/// Performance metrics collected during benchmark.
#[derive(Debug, Default)]
pub struct BenchmarkMetrics {
    pub write_ops: u64,
    pub write_bytes: u64,
    pub write_duration: Duration,
    pub read_sequential_ops: u64,
    pub read_sequential_bytes: u64,
    pub read_sequential_duration: Duration,
    pub read_random_ops: u64,
    pub read_random_bytes: u64,
    pub read_random_duration: Duration,
    pub total_data_size: u64,
    pub total_uncompressed_size: u64,
}

impl BenchmarkMetrics {
    /// Create new empty metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Calculate write throughput in ops/sec.
    pub fn write_ops_per_sec(&self) -> f64 {
        if self.write_duration.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.write_ops as f64 / self.write_duration.as_secs_f64()
    }

    /// Calculate write bandwidth in bytes/sec.
    pub fn write_bytes_per_sec(&self) -> f64 {
        if self.write_duration.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.write_bytes as f64 / self.write_duration.as_secs_f64()
    }

    /// Calculate sequential read throughput in ops/sec.
    pub fn read_sequential_ops_per_sec(&self) -> f64 {
        if self.read_sequential_duration.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.read_sequential_ops as f64 / self.read_sequential_duration.as_secs_f64()
    }

    /// Calculate random read throughput in ops/sec.
    pub fn read_random_ops_per_sec(&self) -> f64 {
        if self.read_random_duration.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.read_random_ops as f64 / self.read_random_duration.as_secs_f64()
    }

    pub fn read_sequential_bytes_per_sec(&self) -> f64 {
        if self.read_sequential_duration.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.read_sequential_bytes as f64 / self.read_sequential_duration.as_secs_f64()
    }

    pub fn read_random_bytes_per_sec(&self) -> f64 {
        if self.read_random_duration.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.read_random_bytes as f64 / self.read_random_duration.as_secs_f64()
    }

    /// Calculate compression ratio.
    pub fn compression_ratio(&self) -> f64 {
        if self.total_data_size == 0 {
            return 0.0;
        }
        self.total_uncompressed_size as f64 / self.total_data_size as f64
    }

    pub fn print_results(&self, label: &str) {
        println!("\n=== {} ===", label);
        println!("\nWrite Performance:");
        println!("  Operations: {} writes", self.write_ops);
        println!("  Duration: {:.3}s", self.write_duration.as_secs_f64());
        println!("  Throughput: {:.2} ops/sec", self.write_ops_per_sec());
        println!(
            "  Bandwidth: {:.2} KB/sec",
            self.write_bytes_per_sec() / 1024.0
        );

        println!("\nRead Performance (Sequential):");
        println!("  Operations: {} reads", self.read_sequential_ops);
        println!(
            "  Duration: {:.3}s",
            self.read_sequential_duration.as_secs_f64()
        );
        println!(
            "  Throughput: {:.2} ops/sec",
            self.read_sequential_ops_per_sec()
        );
        println!(
            "  Bandwidth: {:.2} KB/sec",
            self.read_sequential_bytes_per_sec() / 1024.0
        );

        println!("\nRead Performance (Random):");
        println!("  Operations: {} reads", self.read_random_ops);
        println!(
            "  Duration: {:.3}s",
            self.read_random_duration.as_secs_f64()
        );
        println!(
            "  Throughput: {:.2} ops/sec",
            self.read_random_ops_per_sec()
        );
        println!(
            "  Bandwidth: {:.2} KB/sec",
            self.read_random_bytes_per_sec() / 1024.0
        );

        println!("\nStorage Efficiency:");
        println!(
            "  Uncompressed Size: {:.2} KB",
            self.total_uncompressed_size as f64 / 1024.0
        );
        println!(
            "  Compressed Size: {:.2} KB",
            self.total_data_size as f64 / 1024.0
        );
        println!("  Compression Ratio: {:.2}:1", self.compression_ratio());
    }
}

pub fn create_temp_dir() -> TempDir {
    TempDir::new().expect("Failed to create temporary directory")
}
