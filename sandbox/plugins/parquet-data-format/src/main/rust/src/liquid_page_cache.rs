/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Codec-owned, cross-query decoded-page cache backed by liquid-cache's core API.
//!
//! The Parquet DocValues codec's `PageCache` is per-query scratch: every query re-decodes the
//! same Parquet pages. This module gives the codec a **node-level** (process-lifetime) cache of
//! decoded primitive pages so a later query reuses a page an earlier query already decoded —
//! the cross-query tier the codec otherwise lacks.
//!
//! Design (v1):
//! - A single process-global `Arc<LiquidCache>` (liquid core), built lazily. This is a
//!   **codec-owned** instance, independent of the DataFusion/PPL liquid cache — same technology,
//!   separate instance and keyspace. Sharing the DataFusion instance is a later optimization.
//! - Entries are keyed by `(file_id, column_id, page_idx)` packed into liquid's `EntryID` (a
//!   `usize`). `file_id` comes from a codec-local path→id registry, so the key carries file
//!   identity (and Parquet's immutable-file/generation model means changed data = new path =
//!   new key = automatic miss — no invalidation logic needed).
//! - Values are cached as an Arrow `Int64Array` (with a null buffer derived from the page's
//!   presence bits). On a hit we convert back to the raw `Vec<i64>` + `Vec<bool>` the codec's
//!   `write_primitive_page` already consumes, so the Java/PageCache/per-doc path is byte-identical
//!   whether the page was decoded or served from cache.
//! - liquid's `insert`/`get` are async; we drive them on a dedicated single-threaded tokio runtime
//!   via `block_on`, mirroring `merge::io_task`'s `OnceLock<Runtime>` pattern (the codec's FFM
//!   entry points are synchronous `extern "C"`).
//!
//! Primitives only (INT32/INT64/date → i64 words). BYTE_ARRAY/keyword pages are not cached here.
//! Gated by `set_enabled(true)` from Java; when disabled every entry point is a cheap no-op and the
//! codec's decode path is unchanged.

use std::collections::HashMap;
use std::future::IntoFuture;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{Array, ArrayRef, Int64Array};
use liquid_cache::cache::{EntryID, LiquidCache, LiquidCacheBuilder};
use tokio::runtime::Runtime;

/// The process-global codec-owned decoded-page cache, built on first use. `None` when the cache is
/// disabled or its one-time build failed (e.g. the store directory could not be mounted) — in that
/// case the codec silently falls back to decoding every page, rather than failing the query.
static CACHE: OnceLock<Option<Arc<LiquidCache>>> = OnceLock::new();

/// Dedicated runtime for driving liquid's async `insert`/`get` from the synchronous FFM path.
static RT: OnceLock<Runtime> = OnceLock::new();

/// Master on/off switch, set by Java at init. Off by default → the codec decode path is untouched.
/// May be flipped back off internally if the cache fails to build (see `cache`).
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Configured max memory budget for the cache (bytes). Applied when the cache is first built.
static MAX_MEMORY_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Directory under which the liquid `t4` store is mounted, supplied by Java at init (a writable
/// path derived from the node's data directory — never tmpfs). Empty until `set_enabled` runs.
static CACHE_DIR: OnceLock<Mutex<String>> = OnceLock::new();

/// Codec-local file path → small integer id registry, so entries carry file identity without
/// depending on DataFusion's file numbering.
static FILE_IDS: OnceLock<Mutex<HashMap<String, u32>>> = OnceLock::new();

/// Enable/disable the cache and set the memory budget + store directory. Called by Java at plugin
/// init when the `parquet_liquid_cache` feature flag is on. `cache_dir` must be a writable directory
/// on real disk (the caller passes a path under the node's data dir); the `t4` store is mounted
/// inside it. A `max_memory_bytes` of 0 leaves the liquid default.
pub fn set_enabled(enabled: bool, max_memory_bytes: usize, cache_dir: &str) {
    MAX_MEMORY_BYTES.store(max_memory_bytes, Ordering::Relaxed);
    let slot = CACHE_DIR.get_or_init(|| Mutex::new(String::new()));
    if let Ok(mut guard) = slot.lock() {
        *guard = cache_dir.to_string();
    }
    ENABLED.store(enabled, Ordering::Relaxed);
}

/// True when the cache should be consulted. Cheap relaxed load on the hot path.
#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

fn runtime() -> &'static Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("liquid_page_cache: failed to build tokio runtime")
    })
}

/// Build the `Arc<LiquidCache>` once, mounting a `t4` store at `<cache_dir>/parquet_liquid_cache.t4`.
/// Returns `None` on any failure (missing/unwritable dir, mount error) after logging — the caller
/// then disables the cache so the decode path continues unaffected. Never panics: a cache problem
/// must not poison the column-reader mutex or fail doc-values reads.
fn build_cache() -> Option<Arc<LiquidCache>> {
    let dir = CACHE_DIR
        .get()
        .and_then(|m| m.lock().ok().map(|g| g.clone()))
        .unwrap_or_default();
    if dir.is_empty() {
        crate::log_error!("liquid_page_cache: no cache_dir configured; disabling codec liquid cache");
        return None;
    }
    let base = PathBuf::from(&dir).join(format!("parquet_liquid_cache_{}", std::process::id()));
    if let Err(e) = std::fs::create_dir_all(&base) {
        crate::log_error!(
            "liquid_page_cache: failed to create cache dir {:?}: {}; disabling codec liquid cache",
            base, e
        );
        return None;
    }
    let store_path = base.join("store.t4");
    let store = match runtime().block_on(t4::mount(&store_path)) {
        Ok(s) => s,
        Err(e) => {
            crate::log_error!(
                "liquid_page_cache: failed to mount t4 store at {:?}: {}; disabling codec liquid cache",
                store_path, e
            );
            return None;
        }
    };
    let mut builder = LiquidCacheBuilder::new().with_store(store);
    let budget = MAX_MEMORY_BYTES.load(Ordering::Relaxed);
    if budget > 0 {
        builder = builder.with_max_memory_bytes(budget);
    }
    crate::log_info!("liquid_page_cache: codec liquid cache initialized at {:?}", base);
    Some(runtime().block_on(builder.build()))
}

/// Access the process-global cache, building it once. On build failure this returns `None` and
/// flips `ENABLED` off so subsequent `get_page`/`put_page` calls short-circuit without retrying.
fn cache() -> Option<&'static Arc<LiquidCache>> {
    let slot = CACHE.get_or_init(build_cache);
    if slot.is_none() {
        ENABLED.store(false, Ordering::Relaxed);
    }
    slot.as_ref()
}

/// Resolve (or assign) a stable small id for a Parquet file path. Codec-local; independent of any
/// DataFusion file numbering.
pub fn file_id(path: &str) -> u32 {
    let map = FILE_IDS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().expect("liquid_page_cache: file id registry poisoned");
    let next = guard.len() as u32;
    *guard.entry(path.to_string()).or_insert(next)
}

/// Pack `(file_id, column_id, page_idx)` into a liquid `EntryID`. u16 column id + u32 page fit
/// alongside the file id in a usize on 64-bit targets.
#[inline]
pub fn entry_id(file_id: u32, column_id: u32, page_idx: u32) -> EntryID {
    let v = ((file_id as usize) << 48) | ((column_id as usize) << 32) | (page_idx as usize);
    EntryID::from(v)
}

/// Look up a cached decoded page. Returns `(longs, presence)` in the exact form the decode arms
/// produce (`longs[i]` valid iff `presence[i]`), or `None` on a miss.
pub fn get_page(eid: EntryID) -> Option<(Vec<i64>, Vec<bool>)> {
    let cache = cache()?;
    let array: ArrayRef = runtime().block_on(cache.get(&eid).read())?;
    let int_array = array.as_any().downcast_ref::<Int64Array>()?;
    let len = int_array.len();
    let mut longs = Vec::with_capacity(len);
    let mut presence = Vec::with_capacity(len);
    for i in 0..len {
        if int_array.is_null(i) {
            longs.push(0);
            presence.push(false);
        } else {
            longs.push(int_array.value(i));
            presence.push(true);
        }
    }
    Some((longs, presence))
}

/// Cache a decoded primitive page. `longs[i]` is meaningful only where `presence[i]` is true;
/// null rows are stored as Arrow nulls so a later `get_page` reconstructs presence exactly.
pub fn put_page(eid: EntryID, longs: &[i64], presence: &[bool]) {
    debug_assert_eq!(longs.len(), presence.len());
    let cache = match cache() {
        Some(c) => c,
        None => return,
    };
    let array: Int64Array = longs
        .iter()
        .zip(presence.iter())
        .map(|(&v, &present)| if present { Some(v) } else { None })
        .collect();
    let array_ref: ArrayRef = Arc::new(array);
    // Best-effort: a CacheFull error just means this page is not cached this time.
    // `insert`/`get` return builder types that implement `IntoFuture`, so convert before block_on.
    let _ = runtime().block_on(cache.insert(eid, array_ref).into_future());
}
