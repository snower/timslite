use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::{Duration, Instant};

use crate::error::{Result, TmslError};
use crate::util::{read_i64_le, read_u32_le};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CacheKey {
    pub cache_scope_id: u64,
    pub segment_file_offset: u64,
    pub block_offset: u64,
}
impl CacheKey {
    pub fn new(cache_scope_id: u64, s: u64, b: u64) -> Self {
        Self {
            cache_scope_id,
            segment_file_offset: s,
            block_offset: b,
        }
    }
}

// Approximate per-entry metadata: HashMap slot, CacheKey, Arc header.
const ENTRY_OVERHEAD: usize = 96;
fn entry_footprint(data_len: usize) -> usize {
    data_len.saturating_add(ENTRY_OVERHEAD)
}

struct CacheEntry {
    data: CachedBlockData,
    last_access_tick: AtomicU64,
    last_access_seq: AtomicU64,
    footprint: usize,
}

/// Nanoseconds since the cache epoch, saturating at `u64::MAX`.
fn duration_ticks(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

pub(crate) type CachedBlockData = Arc<Vec<u8>>;

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
    /// Monotonic reference point for access ticks.
    clock_epoch: Instant,
    /// Global sequence used to order accesses that share a tick.
    access_seq: AtomicU64,
}

impl BlockCache {
    pub fn new(max_memory: usize) -> Self {
        Self {
            max_memory,
            used_memory: AtomicUsize::new(0),
            entries: RwLock::new(HashMap::new()),
            cache_hit_count: AtomicU64::new(0),
            cache_miss_count: AtomicU64::new(0),
            clock_epoch: Instant::now(),
            access_seq: AtomicU64::new(1),
        }
    }
    pub fn is_enabled(&self) -> bool {
        self.max_memory > 0
    }
    pub fn max_memory(&self) -> usize {
        self.max_memory
    }

    fn now_tick(&self) -> u64 {
        duration_ticks(self.clock_epoch.elapsed())
    }

    fn next_access_seq(&self) -> u64 {
        self.access_seq.fetch_add(1, Ordering::Relaxed)
    }

    pub fn get(&self, key: &CacheKey) -> Option<CachedBlockData> {
        if !self.is_enabled() {
            return None;
        }
        let guard = self.entries.read().unwrap();
        match guard.get(key) {
            Some(entry) => {
                // A hit may resume after a newer hit; max keeps metadata monotonic.
                entry
                    .last_access_tick
                    .fetch_max(self.now_tick(), Ordering::Relaxed);
                entry
                    .last_access_seq
                    .fetch_max(self.next_access_seq(), Ordering::Relaxed);
                self.cache_hit_count.fetch_add(1, Ordering::Relaxed);
                Some(Arc::clone(&entry.data))
            }
            None => {
                self.cache_miss_count.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn put(&self, key: CacheKey, data: Vec<u8>) {
        let _ = self.put_shared(key, Arc::new(data));
    }

    pub(crate) fn put_shared(
        &self,
        key: CacheKey,
        data: CachedBlockData,
    ) -> Option<CachedBlockData> {
        if !self.is_enabled() {
            return None;
        }
        let footprint = entry_footprint(data.len());
        // Oversized entries are never cached or accounted; the caller still gets the payload.
        if footprint > self.max_memory {
            return Some(data);
        }
        let mut guard = self.entries.write().unwrap();
        if let Some(existing) = guard.get(&key) {
            return Some(Arc::clone(&existing.data));
        }
        let target = (self.max_memory as f64 * 0.85) as usize;
        let current = self.used_memory.load(Ordering::Relaxed);
        if current.saturating_add(footprint) > self.max_memory {
            let needed = target.saturating_sub(footprint);
            let freed = Self::evict_lru(&mut guard, current, needed);
            self.used_memory.fetch_sub(freed, Ordering::Relaxed);
        }
        guard.insert(
            key,
            CacheEntry {
                data: Arc::clone(&data),
                last_access_tick: AtomicU64::new(self.now_tick()),
                last_access_seq: AtomicU64::new(self.next_access_seq()),
                footprint,
            },
        );
        self.used_memory.fetch_add(footprint, Ordering::Relaxed);
        Some(data)
    }

    pub fn invalidate(&self, key: &CacheKey) -> bool {
        if !self.is_enabled() {
            return false;
        }
        let mut guard = self.entries.write().unwrap();
        if let Some(entry) = guard.remove(key) {
            self.used_memory
                .fetch_sub(entry.footprint, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn invalidate_scope(&self, cache_scope_id: u64) -> usize {
        if !self.is_enabled() {
            return 0;
        }
        let mut guard = self.entries.write().unwrap();
        let mut removed = 0usize;
        let mut freed = 0usize;
        guard.retain(|key, entry| {
            if key.cache_scope_id == cache_scope_id {
                removed += 1;
                freed += entry.footprint;
                false
            } else {
                true
            }
        });
        if freed > 0 {
            self.used_memory.fetch_sub(freed, Ordering::Relaxed);
        }
        removed
    }

    fn evict_lru(
        guard: &mut HashMap<CacheKey, CacheEntry>,
        current_used: usize,
        target_used: usize,
    ) -> usize {
        if current_used <= target_used {
            return 0;
        }
        let need = current_used - target_used;
        let mut keys = Vec::with_capacity(guard.len());
        let mut order = Vec::with_capacity(guard.len());
        for (index, (key, entry)) in guard.iter().enumerate() {
            keys.push(key.clone());
            order.push(Reverse((
                entry.last_access_tick.load(Ordering::Relaxed),
                entry.last_access_seq.load(Ordering::Relaxed),
                index,
            )));
        }
        // O(n) heapify, then O(k log n) for k evictions; no persistent heap.
        let mut heap = BinaryHeap::from(order);
        let mut freed = 0usize;
        while freed < need {
            let Some(Reverse((_tick, _seq, index))) = heap.pop() else {
                break;
            };
            if let Some(entry) = guard.remove(&keys[index]) {
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
        let now = self.now_tick();
        let idle_ticks = duration_ticks(idle_timeout);
        let mut count = 0;
        let mut freed = 0usize;
        guard.retain(|_key, entry| {
            let idle = now.saturating_sub(entry.last_access_tick.load(Ordering::Relaxed));
            if idle >= idle_ticks {
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
        guard.clear();
        self.used_memory.store(0, Ordering::Relaxed);
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
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn test_cache_disabled_when_zero() {
        let c = BlockCache::new(0);
        assert!(!c.is_enabled());
        let k = CacheKey::new(1, 0, 0);
        c.put(k.clone(), vec![1, 2, 3]);
        assert!(c.get(&k).is_none());
        assert_eq!(c.stats().entry_count, 0);
    }
    #[test]
    fn test_put_get_roundtrip() {
        let c = BlockCache::new(1024);
        let k = CacheKey::new(1, 0, 0);
        let d = vec![10u8; 100];
        c.put(k.clone(), d.clone());
        assert_eq!(c.get(&k).unwrap().as_slice(), d.as_slice());
        assert_eq!(c.stats().entry_count, 1);
        assert_eq!(c.stats().hit_count, 1);
    }
    #[test]
    fn test_get_returns_shared_payload() {
        let c = BlockCache::new(1024);
        let k = CacheKey::new(1, 0, 0);
        c.put(k.clone(), vec![10u8; 100]);
        let first = c.get(&k).unwrap();
        let second = c.get(&k).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }
    #[test]
    fn test_cache_miss_count() {
        let c = BlockCache::new(1024);
        let k = CacheKey::new(1, 999, 0);
        assert!(c.get(&k).is_none());
        assert_eq!(c.stats().miss_count, 1);
    }
    #[test]
    fn test_lru_eviction_at_watermark() {
        let c = BlockCache::new(1000);
        for i in 0..3 {
            c.put(CacheKey::new(1, i, 0), vec![0u8; 200]);
        }
        assert_eq!(c.stats().entry_count, 3);
        c.put(CacheKey::new(1, 3, 0), vec![0u8; 200]);
        assert!(
            c.stats().used_memory <= 850,
            "used {} exceeds 850",
            c.stats().used_memory
        );
    }
    #[test]
    fn test_idle_eviction() {
        let mut c = BlockCache::new(10240);
        let k = CacheKey::new(1, 0, 0);
        c.put(k.clone(), vec![1u8; 100]);
        assert_eq!(c.evict_idle(Duration::from_secs(10)), 0);
        // Age every access by shifting the tick epoch 20s into the past.
        c.clock_epoch = Instant::now() - Duration::from_secs(20);
        assert_eq!(c.evict_idle(Duration::from_secs(10)), 1);
        assert_eq!(c.stats().entry_count, 0);
    }
    #[test]
    fn test_clear() {
        let c = BlockCache::new(10240);
        for i in 0..5 {
            c.put(CacheKey::new(1, i, 0), vec![0u8; 50]);
        }
        assert_eq!(c.stats().entry_count, 5);
        c.clear();
        assert_eq!(c.stats().entry_count, 0);
        assert_eq!(c.stats().used_memory, 0);
    }

    #[test]
    fn test_invalidate_removes_entry_and_updates_memory() {
        let c = BlockCache::new(10240);
        let k = CacheKey::new(1, 12, 34);
        c.put(k.clone(), vec![1u8; 100]);
        assert_eq!(c.stats().entry_count, 1);
        assert!(c.stats().used_memory > 0);

        assert!(c.invalidate(&k));
        assert_eq!(c.stats().entry_count, 0);
        assert_eq!(c.stats().used_memory, 0);
        assert!(!c.invalidate(&k));
    }

    #[test]
    fn test_invalidate_scope_removes_only_matching_scope() {
        let c = BlockCache::new(10240);
        c.put(CacheKey::new(1, 0, 0), vec![1u8; 100]);
        c.put(CacheKey::new(1, 0, 1), vec![2u8; 100]);
        c.put(CacheKey::new(2, 0, 0), vec![3u8; 100]);

        assert_eq!(c.invalidate_scope(1), 2);
        assert_eq!(c.stats().entry_count, 1);
        assert!(c.get(&CacheKey::new(1, 0, 0)).is_none());
        assert_eq!(
            c.get(&CacheKey::new(2, 0, 0)).unwrap().as_slice(),
            &[3u8; 100]
        );
    }

    #[test]
    fn test_skip_duplicate_insertion() {
        let c = BlockCache::new(10240);
        let k = CacheKey::new(1, 0, 0);
        c.put(k.clone(), vec![1u8; 10]);
        c.put(k.clone(), vec![2u8; 10]);
        assert_eq!(c.get(&k).unwrap().as_slice(), &[1u8; 10]);
        assert_eq!(c.stats().entry_count, 1);
    }

    #[test]
    fn test_oversized_entry_is_not_cached() {
        let c = BlockCache::new(1024);
        let k = CacheKey::new(1, 0, 0);
        c.put(k.clone(), vec![1u8; 2048]);
        let stats = c.stats();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.used_memory, 0);
        assert!(c.get(&k).is_none());
    }

    #[test]
    fn test_put_shared_oversized_returns_payload_uncached() {
        let c = BlockCache::new(1024);
        let k = CacheKey::new(1, 0, 0);
        let payload = Arc::new(vec![3u8; 4096]);
        let returned = c
            .put_shared(k.clone(), Arc::clone(&payload))
            .expect("oversized put must still return a usable payload");
        assert!(Arc::ptr_eq(&returned, &payload));
        assert_eq!(c.stats().entry_count, 0);
        assert_eq!(c.stats().used_memory, 0);
    }

    #[test]
    fn test_oversized_put_preserves_existing_entries_and_accounting() {
        let c = BlockCache::new(1000);
        c.put(CacheKey::new(1, 0, 0), vec![1u8; 100]);
        c.put(CacheKey::new(1, 1, 0), vec![2u8; 100]);
        let before = c.stats();

        c.put(CacheKey::new(1, 2, 0), vec![3u8; 5000]);

        let after = c.stats();
        assert_eq!(after.entry_count, before.entry_count);
        assert_eq!(after.used_memory, before.used_memory);
        assert_eq!(
            c.get(&CacheKey::new(1, 0, 0)).unwrap().as_slice(),
            &[1u8; 100]
        );
    }

    #[test]
    fn test_used_memory_matches_retained_footprints() {
        let c = BlockCache::new(2000);
        for i in 0..20u64 {
            let len = (i as usize % 5 + 1) * 100;
            c.put(CacheKey::new(1, i, i * 7), vec![0u8; len]);
        }
        let expected: usize = {
            let guard = c.entries.read().unwrap();
            guard.values().map(|entry| entry.footprint).sum()
        };
        let stats = c.stats();
        assert_eq!(stats.used_memory, expected);
        assert!(stats.used_memory <= 2000);
    }

    #[test]
    fn test_lru_same_tick_uses_access_sequence() {
        let c = BlockCache::new(1000);
        let first = CacheKey::new(1, 0, 0);
        let second = CacheKey::new(1, 1, 0);
        let third = CacheKey::new(1, 2, 0);
        c.put(first.clone(), vec![0u8; 200]);
        c.put(second.clone(), vec![0u8; 200]);
        c.put(third.clone(), vec![0u8; 200]);

        let tick = c.now_tick();
        {
            let mut guard = c.entries.write().unwrap();
            let first_entry = guard.get_mut(&first).unwrap();
            first_entry.last_access_tick.store(tick, Ordering::Relaxed);
            first_entry.last_access_seq.store(1, Ordering::Relaxed);
            let second_entry = guard.get_mut(&second).unwrap();
            second_entry.last_access_tick.store(tick, Ordering::Relaxed);
            second_entry.last_access_seq.store(2, Ordering::Relaxed);
        }

        // Forces eviction; the shared-tick entries must be ordered by sequence.
        c.put(CacheKey::new(1, 3, 0), vec![0u8; 200]);

        assert!(c.get(&first).is_none());
        assert!(c.get(&second).is_some());
    }

    #[test]
    fn test_late_hit_does_not_regress_access_metadata() {
        let c = BlockCache::new(1024);
        let k = CacheKey::new(1, 0, 0);
        c.put(k.clone(), vec![1u8; 10]);

        // Emulate a hit that already stored newer metadata before a stale hit
        // resumes with an older tick/sequence and updates the same entry.
        let newer_tick = c.now_tick() + 1_000_000_000;
        let newer_seq = u64::MAX - 1;
        {
            let mut guard = c.entries.write().unwrap();
            let entry = guard.get_mut(&k).unwrap();
            entry.last_access_tick.store(newer_tick, Ordering::Relaxed);
            entry.last_access_seq.store(newer_seq, Ordering::Relaxed);
        }

        assert!(c.get(&k).is_some());

        let guard = c.entries.read().unwrap();
        let entry = guard.get(&k).unwrap();
        assert_eq!(entry.last_access_tick.load(Ordering::Relaxed), newer_tick);
        assert_eq!(entry.last_access_seq.load(Ordering::Relaxed), newer_seq);
    }

    fn test_entry(tick: u64, seq: u64, footprint: usize) -> CacheEntry {
        CacheEntry {
            data: Arc::new(Vec::new()),
            last_access_tick: AtomicU64::new(tick),
            last_access_seq: AtomicU64::new(seq),
            footprint,
        }
    }

    #[test]
    fn test_evict_lru_matches_sorted_oracle() {
        let mut entries = HashMap::new();
        let specs = [
            (10u64, 1u64, 100usize),
            (10, 3, 200),
            (10, 2, 300),
            (5, 9, 150),
            (20, 0, 250),
        ];
        for (index, (tick, seq, footprint)) in specs.iter().enumerate() {
            entries.insert(
                CacheKey::new(1, index as u64, 0),
                test_entry(*tick, *seq, *footprint),
            );
        }
        let current_used: usize = specs.iter().map(|spec| spec.2).sum();
        let target_used = current_used - 350;

        // Oracle: full sort by (tick, seq), remove until enough is freed.
        let mut ordered: Vec<_> = entries
            .iter()
            .map(|(key, entry)| {
                (
                    key.clone(),
                    entry.last_access_tick.load(Ordering::Relaxed),
                    entry.last_access_seq.load(Ordering::Relaxed),
                    entry.footprint,
                )
            })
            .collect();
        ordered.sort_by_key(|item| (item.1, item.2));
        let mut expected_freed = 0usize;
        let mut expected_removed = Vec::new();
        for (key, _tick, _seq, footprint) in ordered {
            if expected_freed >= 350 {
                break;
            }
            expected_freed += footprint;
            expected_removed.push(key);
        }

        let freed = BlockCache::evict_lru(&mut entries, current_used, target_used);

        assert_eq!(freed, expected_freed);
        assert_eq!(entries.len(), specs.len() - expected_removed.len());
        for key in &expected_removed {
            assert!(!entries.contains_key(key), "oracle oldest entry survived");
        }
        for (index, _) in specs.iter().enumerate() {
            let key = CacheKey::new(1, index as u64, 0);
            if !expected_removed.contains(&key) {
                assert!(entries.contains_key(&key), "oracle survivor was evicted");
            }
        }
    }

    #[test]
    fn test_invalidate_accounting_is_exact() {
        let c = BlockCache::new(10240);
        c.put(CacheKey::new(1, 0, 0), vec![0u8; 100]);
        c.put(CacheKey::new(1, 0, 1), vec![0u8; 200]);
        assert_eq!(
            c.stats().used_memory,
            entry_footprint(100) + entry_footprint(200)
        );

        assert!(c.invalidate(&CacheKey::new(1, 0, 0)));
        assert_eq!(c.stats().used_memory, entry_footprint(200));

        assert_eq!(c.invalidate_scope(1), 1);
        assert_eq!(c.stats().used_memory, 0);
    }

    #[test]
    fn test_concurrent_hits_are_consistent() {
        let c = Arc::new(BlockCache::new(1024));
        let k = CacheKey::new(1, 0, 0);
        c.put(k.clone(), vec![7u8; 100]);

        let workers: Vec<_> = (0..8)
            .map(|_| {
                let c = Arc::clone(&c);
                let k = k.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        let hit = c.get(&k).expect("entry must stay cached");
                        assert!(hit.iter().all(|&b| b == 7));
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }

        let stats = c.stats();
        assert_eq!(stats.hit_count, 800);
        assert_eq!(stats.miss_count, 0);
        assert_eq!(stats.entry_count, 1);
        assert_eq!(stats.used_memory, entry_footprint(100));
    }

    #[test]
    fn test_get_uses_shared_lock() {
        let c = Arc::new(BlockCache::new(1024));
        let k = CacheKey::new(1, 0, 0);
        c.put(k.clone(), vec![1u8; 10]);

        let guard = c.entries.read().unwrap();
        let (tx, rx) = mpsc::channel();
        let worker_cache = Arc::clone(&c);
        let worker_key = k.clone();
        let worker = thread::spawn(move || {
            let _ = tx.send(worker_cache.get(&worker_key).is_some());
        });

        let observed = rx.recv_timeout(Duration::from_secs(2));
        drop(guard);
        worker.join().unwrap();

        assert!(
            observed.expect("get must not require the exclusive entries lock"),
            "get must not require the exclusive entries lock"
        );
    }
}

// ─── HotBlockCache ──────────────────────────────────────────────────────────

/// Per-query local Block cache, no lock contention.
pub struct HotBlockCache {
    pub(crate) current_key: Option<CacheKey>,
    /// Decompressed block payload: [data_len:4][ts:8][data:N]...
    pub(crate) current_data: Weak<Vec<u8>>,
}

const RECORD_HEADER_SIZE: usize = 12;

impl HotBlockCache {
    pub fn new() -> Self {
        Self {
            current_key: None,
            current_data: Weak::new(),
        }
    }

    #[inline]
    pub fn is_hit(&self, cache_scope_id: u64, seg_offset: u64, block_offset: u64) -> bool {
        self.hit_data(cache_scope_id, seg_offset, block_offset)
            .is_some()
    }

    #[inline]
    pub fn hit_data(
        &self,
        cache_scope_id: u64,
        seg_offset: u64,
        block_offset: u64,
    ) -> Option<CachedBlockData> {
        if self.current_key.as_ref()
            == Some(&CacheKey::new(cache_scope_id, seg_offset, block_offset))
        {
            self.current_data.upgrade()
        } else {
            None
        }
    }

    pub fn fill(&mut self, key: CacheKey, data: &CachedBlockData) {
        self.current_key = Some(key);
        self.current_data = Arc::downgrade(data);
    }

    /// Extract a single record from the cached block payload.
    pub fn extract_record(&self, in_block_offset: u16) -> Result<(i64, Vec<u8>)> {
        let data = self
            .current_data
            .upgrade()
            .ok_or_else(|| TmslError::InvalidData("hot block: cached block expired".into()))?;
        Self::extract_record_from(data.as_slice(), in_block_offset)
    }

    /// `in_block_offset` is in 4-byte units (index-entry semantics); the byte
    /// position within the block payload is `in_block_offset * 4`.
    pub fn extract_record_from(block_data: &[u8], in_block_offset: u16) -> Result<(i64, Vec<u8>)> {
        let pos = in_block_offset as usize * crate::block::RECORD_OFFSET_UNIT_BYTES;
        if pos + RECORD_HEADER_SIZE > block_data.len() {
            return Err(TmslError::InvalidData(
                "hot block: record index out of bounds".into(),
            ));
        }

        let data_len = read_u32_le(
            block_data[pos..pos + 4]
                .try_into()
                .map_err(|_| TmslError::InvalidData("hot block: cannot read data_len".into()))?,
        ) as usize;

        let timestamp = read_i64_le(
            block_data[pos + 4..pos + 12]
                .try_into()
                .map_err(|_| TmslError::InvalidData("hot block: cannot read timestamp".into()))?,
        );

        if pos + RECORD_HEADER_SIZE + data_len > block_data.len() {
            return Err(TmslError::InvalidData(
                "hot block: record data out of bounds".into(),
            ));
        }

        let data =
            block_data[pos + RECORD_HEADER_SIZE..pos + RECORD_HEADER_SIZE + data_len].to_vec();
        Ok((timestamp, data))
    }

    /// `in_block_offset` is in 4-byte units (index-entry semantics).
    pub fn read_data_len_from(block_data: &[u8], in_block_offset: u16) -> Result<u32> {
        let pos = in_block_offset as usize * crate::block::RECORD_OFFSET_UNIT_BYTES;
        if pos + RECORD_HEADER_SIZE > block_data.len() {
            return Err(TmslError::InvalidData(
                "hot block: record index out of bounds".into(),
            ));
        }
        Ok(read_u32_le(block_data[pos..pos + 4].try_into().map_err(
            |_| TmslError::InvalidData("hot block: cannot read data_len".into()),
        )?))
    }

    pub fn clear(&mut self) {
        self.current_key = None;
        self.current_data = Weak::new();
    }
}

impl Default for HotBlockCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod hot_block_tests {
    use super::*;

    fn test_block_data() -> Vec<u8> {
        let mut data = Vec::new();
        // record 1 at byte 0 (units 0): data_len=5, ts=100, span=align_up(17,4)=20
        data.extend_from_slice(&5u32.to_le_bytes());
        data.extend_from_slice(&100i64.to_le_bytes());
        data.extend_from_slice(&[1, 2, 3, 4, 5]);
        data.extend_from_slice(&[0u8; 3]);
        // record 2 at byte 20 (units 5): data_len=3, ts=200, span=align_up(15,4)=16
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(&200i64.to_le_bytes());
        data.extend_from_slice(&[6, 7, 8]);
        data.extend_from_slice(&[0u8; 1]);
        data
    }

    fn make_cache() -> (BlockCache, HotBlockCache) {
        let block_cache = BlockCache::new(1024);
        let key = CacheKey::new(1, 0, 0);
        let data = Arc::new(test_block_data());
        let stored = block_cache.put_shared(key.clone(), data).unwrap();
        let mut cache = HotBlockCache::new();
        cache.fill(key, &stored);
        (block_cache, cache)
    }

    #[test]
    fn test_hot_block_hit_miss() {
        let (_block_cache, cache) = make_cache();
        assert!(cache.is_hit(1, 0, 0));
        assert!(!cache.is_hit(1, 0, 100));
        assert!(!cache.is_hit(1, 100, 0));
        assert!(!cache.is_hit(2, 0, 0));
    }

    #[test]
    fn test_extract_first_record() {
        let (_block_cache, cache) = make_cache();
        let (ts, data) = cache.extract_record(0).unwrap();
        assert_eq!(ts, 100);
        assert_eq!(data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_extract_second_record() {
        let (_block_cache, cache) = make_cache();
        // units 5 -> byte 20: second record start
        let (ts, data) = cache.extract_record(5).unwrap();
        assert_eq!(ts, 200);
        assert_eq!(data, vec![6, 7, 8]);
    }

    #[test]
    fn test_extract_out_of_bounds() {
        let (_block_cache, cache) = make_cache();
        assert!(cache.extract_record(100).is_err());
    }

    #[test]
    fn test_extract_record_from_shared_payload() {
        let data = test_block_data();
        let (ts, record_data) = HotBlockCache::extract_record_from(&data, 5).unwrap();
        assert_eq!(ts, 200);
        assert_eq!(record_data, vec![6, 7, 8]);
    }

    #[test]
    fn test_default_is_empty() {
        let cache = HotBlockCache::new();
        assert!(cache.current_key.is_none());
        assert!(!cache.is_hit(1, 0, 0));
    }

    #[test]
    fn test_clear() {
        let (_block_cache, mut cache) = make_cache();
        assert!(cache.is_hit(1, 0, 0));
        cache.clear();
        assert!(!cache.is_hit(1, 0, 0));
        assert!(cache.current_data.upgrade().is_none());
    }

    #[test]
    fn test_hot_reference_expires_after_global_invalidate() {
        let block_cache = BlockCache::new(1024);
        let key = CacheKey::new(1, 0, 0);
        let stored = block_cache
            .put_shared(key.clone(), Arc::new(test_block_data()))
            .unwrap();
        let mut hot_cache = HotBlockCache::new();
        hot_cache.fill(key.clone(), &stored);
        drop(stored);

        assert!(hot_cache.is_hit(1, 0, 0));
        assert!(block_cache.invalidate(&key));
        assert!(!hot_cache.is_hit(1, 0, 0));
    }
}
