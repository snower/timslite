use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CacheKey {
    pub segment_file_offset: u64,
    pub block_offset: u64,
}
impl CacheKey {
    pub fn new(s: u64, b: u64) -> Self {
        Self {
            segment_file_offset: s,
            block_offset: b,
        }
    }
}

const ENTRY_OVERHEAD: usize = 96;
fn entry_footprint(data_len: usize) -> usize {
    data_len.saturating_add(ENTRY_OVERHEAD)
}

struct CacheEntry {
    data: Vec<u8>,
    last_access_at: Instant,
    access_count: u64,
    footprint: usize,
}

#[derive(Clone, Debug)]
pub struct CacheStats {
    pub entry_count: usize,
    pub used_memory: usize,
    pub hit_count: u64,
    pub miss_count: u64,
}

pub struct BlockCache {
    max_memory: usize,
    used_memory: AtomicUsize,
    entries: RwLock<HashMap<CacheKey, CacheEntry>>,
    cache_hit_count: AtomicU64,
    cache_miss_count: AtomicU64,
}

impl BlockCache {
    pub fn new(max_memory: usize) -> Self {
        Self {
            max_memory,
            used_memory: AtomicUsize::new(0),
            entries: RwLock::new(HashMap::new()),
            cache_hit_count: AtomicU64::new(0),
            cache_miss_count: AtomicU64::new(0),
        }
    }
    pub fn is_enabled(&self) -> bool {
        self.max_memory > 0
    }
    pub fn max_memory(&self) -> usize {
        self.max_memory
    }

    pub fn get(&self, key: &CacheKey) -> Option<Vec<u8>> {
        if !self.is_enabled() {
            return None;
        }
        let mut guard = self.entries.write().unwrap();
        match guard.get_mut(key) {
            Some(entry) => {
                entry.last_access_at = Instant::now();
                entry.access_count += 1;
                self.cache_hit_count.fetch_add(1, Ordering::Relaxed);
                Some(entry.data.clone())
            }
            None => {
                self.cache_miss_count.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn put(&self, key: CacheKey, data: Vec<u8>) {
        if !self.is_enabled() {
            return;
        }
        let footprint = entry_footprint(data.len());
        let mut guard = self.entries.write().unwrap();
        if guard.contains_key(&key) {
            return;
        }
        let target = (self.max_memory as f64 * 0.85) as usize;
        let current: usize = guard.values().map(|v| v.footprint).sum();
        if current + footprint > self.max_memory {
            let needed = target.saturating_sub(footprint);
            let freed = Self::evict_lru(&mut guard, needed);
            self.used_memory.fetch_sub(freed, Ordering::Relaxed);
        }
        guard.insert(
            key,
            CacheEntry {
                data,
                last_access_at: Instant::now(),
                access_count: 0,
                footprint,
            },
        );
        self.used_memory.fetch_add(footprint, Ordering::Relaxed);
    }

    fn evict_lru(guard: &mut HashMap<CacheKey, CacheEntry>, target_used: usize) -> usize {
        let current: usize = guard.values().map(|v| v.footprint).sum();
        if current <= target_used {
            return 0;
        }
        let need = current - target_used;
        let mut sorted: Vec<_> = guard
            .iter()
            .map(|(k, v)| (k.clone(), v.last_access_at, v.footprint))
            .collect();
        sorted.sort_by(|a, b| a.1.cmp(&b.1));
        let mut freed = 0usize;
        for (k, _ts, _fp) in sorted.iter() {
            if freed >= need {
                break;
            }
            if let Some(entry) = guard.remove(k) {
                freed += entry.footprint;
            }
        }
        freed
    }

    pub fn evict_idle(&self, idle_timeout: Duration) -> usize {
        if !self.is_enabled() {
            return 0;
        }
        let mut guard = self.entries.write().unwrap();
        let now = Instant::now();
        let mut count = 0;
        let mut freed = 0usize;
        guard.retain(|_key, entry| {
            if now.duration_since(entry.last_access_at) >= idle_timeout {
                freed += entry.footprint;
                count += 1;
                false
            } else {
                true
            }
        });
        if freed > 0 {
            self.used_memory.fetch_sub(freed, Ordering::Relaxed);
        }
        count
    }

    pub fn clear(&self) {
        let mut guard = self.entries.write().unwrap();
        let freed = guard.values().map(|e| e.footprint).sum::<usize>();
        guard.clear();
        self.used_memory.fetch_sub(freed, Ordering::Relaxed);
    }

    pub fn stats(&self) -> CacheStats {
        let guard = self.entries.read().unwrap();
        CacheStats {
            entry_count: guard.len(),
            used_memory: self.used_memory.load(Ordering::Relaxed),
            hit_count: self.cache_hit_count.load(Ordering::Relaxed),
            miss_count: self.cache_miss_count.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_cache_disabled_when_zero() {
        let c = BlockCache::new(0);
        assert!(!c.is_enabled());
        let k = CacheKey::new(0, 0);
        c.put(k.clone(), vec![1, 2, 3]);
        assert!(c.get(&k).is_none());
        assert_eq!(c.stats().entry_count, 0);
    }
    #[test]
    fn test_put_get_roundtrip() {
        let c = BlockCache::new(1024);
        let k = CacheKey::new(0, 0);
        let d = vec![10u8; 100];
        c.put(k.clone(), d.clone());
        assert_eq!(c.get(&k).unwrap(), d);
        assert_eq!(c.stats().entry_count, 1);
        assert_eq!(c.stats().hit_count, 1);
    }
    #[test]
    fn test_cache_miss_count() {
        let c = BlockCache::new(1024);
        let k = CacheKey::new(999, 0);
        assert!(c.get(&k).is_none());
        assert_eq!(c.stats().miss_count, 1);
    }
    #[test]
    fn test_lru_eviction_at_watermark() {
        let c = BlockCache::new(1000);
        for i in 0..3 {
            c.put(CacheKey::new(i, 0), vec![0u8; 200]);
        }
        assert_eq!(c.stats().entry_count, 3);
        c.put(CacheKey::new(3, 0), vec![0u8; 200]);
        assert!(
            c.stats().used_memory <= 850,
            "used {} exceeds 850",
            c.stats().used_memory
        );
    }
    #[test]
    fn test_idle_eviction() {
        let c = BlockCache::new(10240);
        let k = CacheKey::new(0, 0);
        c.put(k.clone(), vec![1u8; 100]);
        assert_eq!(c.evict_idle(Duration::from_secs(10)), 0);
        {
            let mut g = c.entries.write().unwrap();
            if let Some(e) = g.get_mut(&k) {
                e.last_access_at = Instant::now() - Duration::from_secs(20);
            }
        }
        assert_eq!(c.evict_idle(Duration::from_secs(10)), 1);
        assert_eq!(c.stats().entry_count, 0);
    }
    #[test]
    fn test_clear() {
        let c = BlockCache::new(10240);
        for i in 0..5 {
            c.put(CacheKey::new(i, 0), vec![0u8; 50]);
        }
        assert_eq!(c.stats().entry_count, 5);
        c.clear();
        assert_eq!(c.stats().entry_count, 0);
        assert_eq!(c.stats().used_memory, 0);
    }
    #[test]
    fn test_skip_duplicate_insertion() {
        let c = BlockCache::new(10240);
        let k = CacheKey::new(0, 0);
        c.put(k.clone(), vec![1u8; 10]);
        c.put(k.clone(), vec![2u8; 10]);
        assert_eq!(c.get(&k).unwrap(), vec![1u8; 10]);
        assert_eq!(c.stats().entry_count, 1);
    }
}
