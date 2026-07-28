/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cache infrastructure for the analytics backend.
//!
//! # Structure
//!
//! - [`eviction_policy`] — pluggable eviction policy trait (`CachePolicy`) and
//!   built-in implementations (`LruPolicy`, `LfuPolicy`). Add new policies here
//!   (e.g. S3-FIFO) without touching the cache implementations.
//! - [`metadata_cache`] — `MutexFileMetadataCache`: wraps DataFusion's
//!   `DefaultFilesMetadataCache` with hit/miss counters and enforces the
//!   footer-only invariant via `strip_page_index` at every `put`.
//! - [`statistics_cache`] — `CustomStatisticsCache`: byte-bounded LRU cache
//!   for per-file `Statistics` (row-group min/max/null-count).
//! - [`custom_manager`] — `CustomCacheManager`: ties the metadata and
//!   statistics caches together for pre-warming and lifecycle management.
//! - [`page_index`] — scoped parquet page-index caches (ColumnIndex +
//!   OffsetIndex), cell-granular and backed by `BoundedCache` /
//!   `Box<dyn CachePolicy>`.

pub mod custom_cache_manager;
pub mod eviction_policy;
pub mod metadata_cache;
pub mod page_index;
pub mod statistics_cache;

// Flat re-exports so existing call sites keep working without path changes.
pub use custom_cache_manager::CustomCacheManager;
pub use eviction_policy::{create_policy, CachePolicy, CacheResult, PolicyType};
pub use metadata_cache::{
    DefaultFilesMetadataCache, MutexFileMetadataCache, CACHE_TYPE_COLUMN_INDEX,
    CACHE_TYPE_METADATA, CACHE_TYPE_OFFSET_INDEX, CACHE_TYPE_STATS,
};
pub use statistics_cache::{compute_parquet_statistics, CustomStatisticsCache};

use datafusion::execution::cache::cache_manager::{CachedFileList, TableScopedPath};
use datafusion::execution::cache::default_cache::DefaultCache;

/// Per-query cache of the `ObjectMeta`s produced by listing a table path.
///
/// DataFusion replaced its purpose-built `DefaultListFilesCache` with the
/// generic [`DefaultCache`]; this alias keeps the old name at OpenSearch call
/// sites so the rename lives in one place.
pub type DefaultListFilesCache = DefaultCache<TableScopedPath, CachedFileList>;

/// Byte budget for a per-query list-files cache.
///
/// These caches hold exactly one entry — the listing snapshot for the shard's
/// table path, pushed in by the caller — and live only as long as the query, so
/// the budget just has to be large enough not to evict that entry. It is not a
/// tuning knob.
const LIST_FILES_CACHE_LIMIT: usize = 64 * 1024 * 1024;

/// Create an empty per-query list-files cache.
///
/// Replaces the `DefaultListFilesCache::default()` calls that the generic
/// [`DefaultCache`] no longer offers, since it requires an explicit budget.
pub fn new_list_files_cache() -> DefaultListFilesCache {
    DefaultCache::new(LIST_FILES_CACHE_LIMIT).with_name("OpenSearchListFilesCache")
}
