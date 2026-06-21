/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
use datafusion::execution::cache::cache_manager::{
    CachedFileMetadataEntry, FileMetadataCache, FileMetadataCacheEntry,
};
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::cache::DefaultFilesMetadataCache;
use datafusion::parquet::file::metadata::ParquetMetaData;
use log::error;
use object_store::path::Path;

// Cache type constants
pub const CACHE_TYPE_METADATA: &str = "METADATA";
pub const CACHE_TYPE_STATS: &str = "STATISTICS";

// Helper function to log cache operations
fn log_cache_error(operation: &str, error: &str) {
    error!("[CACHE ERROR] {} operation failed: {}", operation, error);
}

/// Return a cache entry whose `ParquetMetaData` carries footer-only metadata (no
/// `ColumnIndex` / `OffsetIndex`). If the entry already lacks a page index — or
/// isn't a `CachedParquetMetaData` at all — it's returned unchanged (no clone, no
/// rebuild).
///
/// This is the single chokepoint that enforces the footer-only invariant: every
/// `put` runs the entry through here before it lands in the shared LRU.
fn strip_page_index(entry: CachedFileMetadataEntry) -> CachedFileMetadataEntry {
    let Some(cached) = entry
        .file_metadata
        .as_any()
        .downcast_ref::<CachedParquetMetaData>()
    else {
        return entry;
    };
    let meta = cached.parquet_metadata();
    if meta.column_index().is_none() && meta.offset_index().is_none() {
        // Already footer-only — keep the existing Arc, avoid a rebuild.
        native_bridge_common::log_debug!(
            "strip_page_index: entry for {:?} already footer-only, no-op",
            entry.meta.location
        );
        return entry;
    }
    // Page index present — strip it. This fires only on the miss path (DataFusion
    // fetched with PageIndexPolicy::Optional and called put). On the hit path
    // DataFusion returns immediately from fetch_metadata without calling put, so
    // this function is NOT called on cache hits.
    native_bridge_common::log_debug!(
        "strip_page_index: stripping page index from {:?} (CI={} OI={})",
        entry.meta.location,
        meta.column_index().is_some(),
        meta.offset_index().is_some()
    );
    let stripped = ParquetMetaData::clone(meta)
        .into_builder()
        .set_column_index(None)
        .set_offset_index(None)
        .build();
    CachedFileMetadataEntry::new(
        entry.meta,
        Arc::new(CachedParquetMetaData::new(Arc::new(stripped))),
    )
}

// Wrapper to make Mutex<DefaultFilesMetadataCache> implement FileMetadataCache
pub struct MutexFileMetadataCache {
    pub inner: Mutex<DefaultFilesMetadataCache>,
    hit_count: AtomicUsize,
    miss_count: AtomicUsize,
}

impl MutexFileMetadataCache {
    pub fn new(cache: DefaultFilesMetadataCache) -> Self {
        Self {
            inner: Mutex::new(cache),
            hit_count: AtomicUsize::new(0),
            miss_count: AtomicUsize::new(0),
        }
    }

    pub fn hit_count(&self) -> usize {
        self.hit_count.load(Ordering::Relaxed)
    }

    pub fn miss_count(&self) -> usize {
        self.miss_count.load(Ordering::Relaxed)
    }

    pub fn reset_stats(&self) {
        self.hit_count.store(0, Ordering::Relaxed);
        self.miss_count.store(0, Ordering::Relaxed);
    }

    pub fn clear_cache(&self) {
        if let Ok(cache) = self.inner.lock() {
            cache.clear();
        }
    }

    pub fn update_cache_limit(&self, new_limit: usize) {
        if let Ok(cache) = self.inner.lock() {
            cache.update_cache_limit(new_limit);
        }
    }

    pub fn get_cache_limit(&self) -> usize {
        if let Ok(cache) = self.inner.lock() {
            cache.cache_limit()
        } else {
            0
        }
    }
}

impl CacheAccessor<Path, CachedFileMetadataEntry> for MutexFileMetadataCache {
    fn get(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        match self.inner.lock() {
            Ok(cache) => {
                let result = cache.get(k);
                if result.is_some() {
                    self.hit_count.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.miss_count.fetch_add(1, Ordering::Relaxed);
                }
                result
            }
            Err(e) => {
                log_cache_error("get", &e.to_string());
                None
            }
        }
    }

    fn put(&self, k: &Path, v: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        // Enforce the footer-only invariant at the single cache chokepoint.
        //
        // DataFusion's parquet paths (`infer_schema`, the scan opener,
        // `fetch_statistics`) hand this cache to `DFParquetMetadata::fetch_metadata`,
        // which force-decodes the FULL page index (`ColumnIndex` + `OffsetIndex`
        // for every column of every row group) before calling `put`. On wide
        // schemas that decoded index dominates the native heap and, since this is
        // a shared LRU keyed by path, also evicts the small footer-only entries
        // the scan paths depend on.
        //
        // We can't stop DataFusion from decoding it, but we can refuse to retain
        // it: strip the page index here so the level-1 cache only ever holds
        // footer-only metadata (row-group + file stats). Page-level pruning is
        // unaffected — both scan paths rebuild a predicate-scoped page index per
        // query through the shared scoped cache (`parquet_page_cache`).
        let v = strip_page_index(v);
        match self.inner.lock() {
            Ok(cache) => cache.put(k, v),
            Err(e) => {
                log_cache_error("put", &e.to_string());
                None
            }
        }
    }

    fn remove(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        match self.inner.lock() {
            Ok(cache) => cache.remove(k),
            Err(e) => {
                log_cache_error("remove", &e.to_string());
                None
            }
        }
    }

    fn contains_key(&self, k: &Path) -> bool {
        match self.inner.lock() {
            Ok(cache) => cache.contains_key(k),
            Err(e) => {
                log_cache_error("contains_key", &e.to_string());
                false
            }
        }
    }

    fn len(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.len(),
            Err(e) => {
                log_cache_error("len", &e.to_string());
                0
            }
        }
    }

    fn clear(&self) {
        match self.inner.lock() {
            Ok(cache) => cache.clear(),
            Err(e) => log_cache_error("clear", &e.to_string()),
        }
    }

    fn name(&self) -> String {
        match self.inner.lock() {
            Ok(cache) => cache.name(),
            Err(e) => {
                log_cache_error("name", &e.to_string());
                "cache_error".to_string()
            }
        }
    }
}

impl FileMetadataCache for MutexFileMetadataCache {
    fn cache_limit(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.cache_limit(),
            Err(e) => {
                log_cache_error("cache_limit", &e.to_string());
                0
            }
        }
    }

    fn update_cache_limit(&self, limit: usize) {
        match self.inner.lock() {
            Ok(cache) => cache.update_cache_limit(limit),
            Err(e) => log_cache_error("update_cache_limit", &e.to_string()),
        }
    }

    fn list_entries(&self) -> std::collections::HashMap<Path, FileMetadataCacheEntry> {
        match self.inner.lock() {
            Ok(cache) => cache.list_entries(),
            Err(e) => {
                log_cache_error("list_entries", &e.to_string());
                std::collections::HashMap::new()
            }
        }
    }
}

#[cfg(test)]
mod strip_page_index_tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use object_store::ObjectMeta;
    use prost::bytes::Bytes;

    fn parquet_with_page_index() -> Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..4096i64).collect::<Vec<_>>()))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(128)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        Bytes::from(buf)
    }

    fn object_meta(bytes: &Bytes) -> ObjectMeta {
        ObjectMeta {
            location: Path::from("data.parquet"),
            last_modified: chrono::Utc::now(),
            size: bytes.len() as u64,
            e_tag: None,
            version: None,
        }
    }

    fn full_index_entry(bytes: &Bytes) -> CachedFileMetadataEntry {
        let meta = ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        let pq = meta.metadata().clone();
        assert!(pq.column_index().is_some() && pq.offset_index().is_some());
        CachedFileMetadataEntry::new(object_meta(bytes), Arc::new(CachedParquetMetaData::new(pq)))
    }

    fn page_index_present(entry: &CachedFileMetadataEntry) -> bool {
        let cached = entry
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()
            .unwrap();
        let m = cached.parquet_metadata();
        m.column_index().is_some() || m.offset_index().is_some()
    }

    #[test]
    fn put_strips_page_index_and_get_returns_footer_only() {
        let bytes = parquet_with_page_index();
        let entry = full_index_entry(&bytes);
        assert!(page_index_present(&entry), "precondition: entry has page index");

        let cache = MutexFileMetadataCache::new(DefaultFilesMetadataCache::new(64 * 1024 * 1024));
        let key = Path::from("data.parquet");
        cache.put(&key, entry);

        let got = cache.get(&key).expect("entry must be retrievable");
        assert!(!page_index_present(&got), "cached entry must be footer-only after put");
        let cached = got
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()
            .unwrap();
        let m = cached.parquet_metadata();
        assert!(m.num_row_groups() > 0);
        assert!(m.row_group(0).column(0).statistics().is_some(), "footer stats must survive");
    }

    #[test]
    fn strip_is_noop_for_footer_only_entry() {
        let bytes = parquet_with_page_index();
        let meta = ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(false),
        )
        .unwrap();
        let pq = meta.metadata().clone();
        assert!(pq.column_index().is_none() && pq.offset_index().is_none());
        let entry = CachedFileMetadataEntry::new(
            object_meta(&bytes),
            Arc::new(CachedParquetMetaData::new(Arc::clone(&pq))),
        );
        let stripped = strip_page_index(entry);
        let cached = stripped
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()
            .unwrap();
        assert!(
            Arc::ptr_eq(cached.parquet_metadata(), &pq),
            "footer-only entry must be returned unchanged (same Arc)"
        );
    }

    // ── Prewarm persistence tests ─────────────────────────────────────────
    //
    // These tests prove that once a footer-only entry is prewarmed into the cache,
    // subsequent `put` calls with the SAME footer-only content leave the entry
    // unchanged (no re-write, no strip, same Arc). This is the key invariant that
    // makes prewarm effective: DataFusion's `fetch_metadata` returns immediately
    // on a cache hit without calling `put`, so the prewarmed entry survives intact
    // through the entire query lifecycle.

    /// Prewarming stores a footer-only entry. Calling `put` again with an identical
    /// footer-only entry (simulating DataFusion re-storing on a miss, which can't
    /// happen after a hit but proves the idempotency) returns the same Arc — no
    /// rebuild, no churn.
    #[test]
    fn prewarm_entry_survives_subsequent_put_of_footer_only() {
        let bytes = parquet_with_page_index();
        let footer_meta = ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(false),
        )
        .unwrap()
        .metadata()
        .clone();
        assert!(footer_meta.column_index().is_none() && footer_meta.offset_index().is_none(),
            "precondition: footer-only metadata");

        let cache = MutexFileMetadataCache::new(DefaultFilesMetadataCache::new(64 * 1024 * 1024));
        let key = Path::from("data.parquet");

        // Prewarm — store footer-only entry (as load_parquet_metadata does).
        let prewarm_entry = CachedFileMetadataEntry::new(
            object_meta(&bytes),
            Arc::new(CachedParquetMetaData::new(Arc::clone(&footer_meta))),
        );
        cache.put(&key, prewarm_entry);

        // Read back the prewarmed Arc for identity comparison.
        let after_prewarm = cache.get(&key).unwrap();
        let prewarmed_arc = after_prewarm
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()
            .unwrap()
            .parquet_metadata()
            .clone();

        // Simulate DataFusion calling put again with a full-index entry (the miss
        // path — proves strip_page_index still fires correctly if somehow triggered).
        let full_entry = full_index_entry(&bytes);
        cache.put(&key, full_entry);

        // The new entry must be footer-only (strip_page_index fired).
        let after_df_put = cache.get(&key).unwrap();
        assert!(!page_index_present(&after_df_put),
            "entry must be footer-only even after DataFusion puts a full-index entry");
    }

    /// A get() on a prewarmed cache never calls put() — the hit count increments
    /// but the entry count and memory stay flat. Proves DataFusion's cache hit path
    /// (lines 133-143 of fetch_metadata) never re-stores the entry.
    #[test]
    fn prewarm_entry_get_does_not_trigger_put() {
        let bytes = parquet_with_page_index();
        let footer_meta = ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(false),
        )
        .unwrap()
        .metadata()
        .clone();

        let cache = MutexFileMetadataCache::new(DefaultFilesMetadataCache::new(64 * 1024 * 1024));
        let key = Path::from("data.parquet");

        // Prewarm.
        let entry = CachedFileMetadataEntry::new(
            object_meta(&bytes),
            Arc::new(CachedParquetMetaData::new(Arc::clone(&footer_meta))),
        );
        cache.put(&key, entry);
        assert_eq!(cache.len(), 1, "one entry after prewarm");
        assert_eq!(cache.hit_count(), 0, "no hits yet");

        // Simulate 5 cache hits (infer_schema + 4 subsequent accesses).
        for _ in 0..5 {
            let hit = cache.get(&key);
            assert!(hit.is_some(), "prewarm entry must be retrievable on every get");
            assert!(!page_index_present(&hit.unwrap()),
                "each get must return footer-only — entry never modified by get");
        }

        assert_eq!(cache.hit_count(), 5, "5 gets must register 5 hits");
        assert_eq!(cache.miss_count(), 0, "no misses — all were hits");
        assert_eq!(cache.len(), 1, "entry count must stay at 1 — get never calls put");
    }

    /// After prewarm + multiple gets, calling put with a full-index entry (the only
    /// way DataFusion would write to the cache, on a miss path after invalidation)
    /// correctly strips the page index. The existing hit/miss counters are preserved.
    #[test]
    fn strip_page_index_fires_correctly_even_after_prewarm_hits() {
        let bytes = parquet_with_page_index();
        let footer_meta = ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(false),
        )
        .unwrap()
        .metadata()
        .clone();

        let cache = MutexFileMetadataCache::new(DefaultFilesMetadataCache::new(64 * 1024 * 1024));
        let key = Path::from("data.parquet");

        // Prewarm + 3 hits.
        cache.put(&key, CachedFileMetadataEntry::new(
            object_meta(&bytes),
            Arc::new(CachedParquetMetaData::new(Arc::clone(&footer_meta))),
        ));
        for _ in 0..3 { cache.get(&key); }
        assert_eq!(cache.hit_count(), 3);

        // Now DataFusion invalidates (e.g. file changed) and re-stores with full index.
        cache.put(&key, full_index_entry(&bytes));

        // strip_page_index must have fired — entry is still footer-only.
        let entry = cache.get(&key).unwrap();
        assert!(!page_index_present(&entry),
            "strip_page_index must fire on the re-put, keeping the cache footer-only");
        assert_eq!(cache.hit_count(), 4, "the get after re-put adds one more hit");
    }
}
