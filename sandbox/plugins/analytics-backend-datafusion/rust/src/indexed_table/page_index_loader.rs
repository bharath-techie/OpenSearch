/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Scoped parquet page-index caches — TWO caches, by consumer.
//!
//! # Why this exists
//!
//! Parquet metadata loading pulls the **entire page index** — `ColumnIndex`
//! (per-page min/max; the per-page *string* min/max is the heap hog) plus
//! `OffsetIndex` (per-page byte offsets), for every column of every row group.
//! On wide schemas (the production `textbench` index has 402 columns) this is
//! ~82% of native heap and is re-decoded per query, even when the query filters
//! a single column. The level-1 metadata cache is kept footer-only (see
//! [`crate::cache`]); this module rebuilds a *scoped* page index per query and
//! caches it, shared by both scan paths (the DataFusion `ListingTable` path and
//! the custom indexed-table executor).
//!
//! # Two caches, because the two indexes have different drivers
//!
//! The `ColumnIndex` and `OffsetIndex` are consumed by different parts of
//! DataFusion 54 / parquet 58, with **different natural cache keys**. Forcing
//! them into one key makes the projection-driven OffsetIndex poison the
//! predicate-driven ColumnIndex's broad cross-path sharing (the failure mode of
//! the prior iteration). So they are split:
//!
//! - **ColumnIndex — predicate-driven.** Read only at *prune* time, and only for
//!   the predicate column being evaluated
//!   (`page_filter::PagesPruningStatistics`, `offset_index[rg][predicate_col]`).
//!   Key: `(file, predicate_cols, surviving_rgs)`. Deterministic in the
//!   *predicate* (independent of what you `SELECT`), so the same filter shares
//!   its entry across scan paths **and** across queries with different
//!   projections. This is the heavy index (string min/max) and the big heap win.
//!   Scoped to predicate columns (`NONE` placeholders elsewhere) and, optionally,
//!   to the row groups that pass footer-stats pruning ([`surviving_row_groups`]).
//!
//! - **OffsetIndex — projection-driven.** Read at *scan* time for **projected**
//!   columns (`InMemoryRowGroup::fetch_ranges`, `projection.leaf_included(idx)`),
//!   and at prune time for the predicate column, and at column 0 for the
//!   page-skip metric. Key: `(file, offset_cols)` where
//!   `offset_cols = predicate ∪ projection ∪ {0}`. This is the cheap, fixed-width
//!   index (no per-page string stats). Built for **all row groups** (an empty
//!   OffsetIndex on a row group DataFusion scans panics / breaks reads, and
//!   DataFusion chooses the scanned set itself, after our load — see
//!   HANDOFF_step2_rg_scoping.md §1e).
//!
//! Each cache stores only its decoded vector (`ParquetColumnIndex` /
//! `ParquetOffsetIndex`) — never a full `ParquetMetaData` (no footer
//! duplication). On lookup the two are **grafted** onto the caller's
//! already-resident footer via [`ParquetMetaData::into_builder`] →
//! `set_column_index`/`set_offset_index`.
//!
//! **Consequence for tests:** a lookup returns a *fresh* `Arc`, so `Arc::ptr_eq`
//! is the wrong signal for "served from cache" — assert via the per-cache hit
//! counters ([`column_index_cache_stats`] / [`offset_index_cache_stats`]).
//!
//! ## Correctness / fallback
//!
//! Any failure (file has no page index, a column lacks an index range, a
//! decode/IO error) makes the load return `None`. The caller keeps its
//! footer-only metadata and the pruner conservatively no-ops (scans the whole
//! row group) — never a wrong result.
//!
//! ## Upstream note
//!
//! arrow-rs is moving toward first-class selective metadata decoding
//! (apache/arrow-rs#8643 open; the `ParquetStatisticsPolicy::skip_except` pattern
//! merged in #8797 / #8714 for encoding stats). None yet expose a page-index
//! column/row-group projection, so we hand-roll it with the deprecated
//! [`read_columns_indexes`]/[`read_offset_indexes`] (the only public subset
//! decoders). Migrate to `ParquetMetaDataOptions` when it grows a page-index knob.

use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use once_cell::sync::Lazy;

use arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::{
    ColumnChunkMetaData, OffsetIndexBuilder, ParquetColumnIndex, ParquetMetaData,
    ParquetOffsetIndex,
};
use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
use datafusion::parquet::file::page_index::index_reader::{
    read_columns_indexes, read_offset_indexes,
};
use datafusion::parquet::file::reader::{ChunkReader, Length};
use object_store::ObjectStore;
use prost::bytes::{Buf, Bytes};

/// Default byte budget for EACH scoped cache, used until the caller sets one from
/// the runtime's configured limit (see [`set_column_index_cache_limit`] /
/// [`set_offset_index_cache_limit`]). The two caches are budgeted independently:
/// the ColumnIndex (per-page string min/max) is the heavy one and the OffsetIndex
/// (fixed-width page offsets) is tiny, so they get separate, separately-tunable
/// limits rather than sharing one number.
const DEFAULT_SCOPED_CACHE_LIMIT: usize = 64 * 1024 * 1024;

// ── Generic byte-bounded LRU ────────────────────────────────────────────────

/// Snapshot of one scoped cache's counters plus occupancy. Surfaced on
/// node-stats and used by tests to assert hits/misses without `Arc::ptr_eq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScopedCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub used_bytes: usize,
    pub limit_bytes: usize,
}

struct LruEntry<V> {
    value: V,
    size: usize,
    last_used: u64,
}

/// Byte-bounded LRU. Evicts the least-recently-used entries once `used > limit`,
/// so it always serves cached data within a memory budget rather than silently
/// degrading to "decode every query" when full. `V` is cloned on hit (cheap: a
/// predicate-scoped `ColumnIndex` or a fixed-width `OffsetIndex`, never a footer).
struct Lru<K: Eq + Hash + Clone, V: Clone> {
    map: HashMap<K, LruEntry<V>>,
    used: usize,
    limit: usize,
    /// Monotonic clock for LRU ordering (avoids `Instant`; fine single-process).
    tick: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl<K: Eq + Hash + Clone, V: Clone> Lru<K, V> {
    fn new(limit: usize) -> Self {
        Self {
            map: HashMap::new(),
            used: 0,
            limit,
            tick: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    fn get(&mut self, key: &K) -> Option<V> {
        let t = self.next_tick();
        match self.map.get_mut(key) {
            Some(entry) => {
                entry.last_used = t;
                self.hits += 1;
                Some(entry.value.clone())
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    fn insert(&mut self, key: K, value: V, size: usize) {
        // An entry larger than the whole budget can never be retained; skip it
        // rather than evicting everything else for something we'd drop anyway.
        if size > self.limit {
            return;
        }
        let t = self.next_tick();
        if let Some(old) = self.map.insert(key, LruEntry { value, size, last_used: t }) {
            self.used -= old.size;
        }
        self.used += size;
        self.evict();
    }

    fn evict(&mut self) {
        while self.used > self.limit {
            let Some(victim) = self
                .map
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(removed) = self.map.remove(&victim) {
                self.used -= removed.size;
                self.evictions += 1;
            }
        }
    }

    fn stats(&self) -> ScopedCacheStats {
        ScopedCacheStats {
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            entries: self.map.len(),
            used_bytes: self.used,
            limit_bytes: self.limit,
        }
    }

    fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
        self.evict();
    }

    fn clear_keep_limit(&mut self) {
        self.map.clear();
        self.used = 0;
        self.tick = 0;
        self.hits = 0;
        self.misses = 0;
        self.evictions = 0;
    }
}

// ── Cache keys + the two global caches ──────────────────────────────────────

/// ColumnIndex cache key — one decoded `ColumnIndexMetaData` **cell** per
/// `(file, column, row-group)`. The page index for a given column+RG is an
/// intrinsic property of the file: it is identical no matter which *other*
/// columns a query filters on, or which literal a predicate uses. Keying at the
/// cell granularity means a column's per-page string min/max is decoded and
/// stored **once per file**, then reused by every query whose predicate touches
/// that column — regardless of the predicate-column *combination* or the
/// surviving-row-group *set*. (The prior set-keyed design re-decoded and
/// re-stored a column for every distinct predicate/RG combination — storage grew
/// with query diversity, not schema width.)
///
/// Both scan paths resolve the same `(file, col, rg)` for the same logical
/// request, so cells are shared across paths → cross-path sharing.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct CiCellKey {
    path: Arc<str>,
    col: usize,
    rg: usize,
}

/// OffsetIndex cache key — one decoded value per `(file, column)`, where the
/// value is that column's `OffsetIndexMetaData` for **every** row group (a
/// `Vec` indexed by RG). Unlike the ColumnIndex, the OffsetIndex is read at scan
/// time for any RG DataFusion chooses to scan — and DataFusion picks that set
/// itself, after our load — so a column's OffsetIndex must always cover all RGs
/// (an empty entry on a scanned RG panics / breaks reads). RG can therefore never
/// be a key axis here; the cell is the whole-column, all-RG offset index. Keyed
/// only on `(file, col)`, so any query that reads a column reuses its offset
/// index irrespective of projection or predicate.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct OiCellKey {
    path: Arc<str>,
    col: usize,
}

/// One column's OffsetIndex across all row groups (indexed by RG). The value type
/// of [`OFFSET_INDEX_CACHE`].
type OiColumn = Vec<datafusion::parquet::file::page_index::offset_index::OffsetIndexMetaData>;

static COLUMN_INDEX_CACHE: Lazy<Mutex<Lru<CiCellKey, ColumnIndexMetaData>>> =
    Lazy::new(|| Mutex::new(Lru::new(DEFAULT_SCOPED_CACHE_LIMIT)));

static OFFSET_INDEX_CACHE: Lazy<Mutex<Lru<OiCellKey, OiColumn>>> =
    Lazy::new(|| Mutex::new(Lru::new(DEFAULT_SCOPED_CACHE_LIMIT)));

// ── Limits / stats / clear ──────────────────────────────────────────────────

/// Set the ColumnIndex cache's byte budget. Called from startup wiring with the
/// configured limit. Idempotent; shrinking evicts immediately. Zero ignored.
pub fn set_column_index_cache_limit(limit: usize) {
    if limit == 0 {
        return;
    }
    if let Ok(mut c) = COLUMN_INDEX_CACHE.lock() {
        c.set_limit(limit);
    }
}

/// Set the OffsetIndex cache's byte budget. Called from startup wiring with the
/// configured limit. Idempotent; shrinking evicts immediately. Zero ignored.
pub fn set_offset_index_cache_limit(limit: usize) {
    if limit == 0 {
        return;
    }
    if let Ok(mut c) = OFFSET_INDEX_CACHE.lock() {
        c.set_limit(limit);
    }
}

/// Counters + occupancy of the ColumnIndex (predicate-driven) cache.
pub fn column_index_cache_stats() -> ScopedCacheStats {
    COLUMN_INDEX_CACHE.lock().map(|c| c.stats()).unwrap_or_default()
}

/// Counters + occupancy of the OffsetIndex (projection-driven) cache.
pub fn offset_index_cache_stats() -> ScopedCacheStats {
    OFFSET_INDEX_CACHE.lock().map(|c| c.stats()).unwrap_or_default()
}

/// Drop all entries and reset counters in BOTH caches, keeping the budgets. For
/// operational testing — reset and re-measure without a cluster restart.
pub fn clear_scoped_cache() {
    if let Ok(mut c) = COLUMN_INDEX_CACHE.lock() {
        c.clear_keep_limit();
    }
    if let Ok(mut c) = OFFSET_INDEX_CACHE.lock() {
        c.clear_keep_limit();
    }
}

// ── Public API ──────────────────────────────────────────────────────────────

/// Map the query's arrow predicate-column names to this file's parquet column
/// indices, using the same resolution the pruner uses
/// (`StatisticsConverter::parquet_column_index`). Columns absent from the parquet
/// file (schema evolution) are skipped. Returns a sorted, deduped set so both
/// scan paths produce an identical key for the same logical predicate.
pub fn resolve_predicate_parquet_columns(
    arrow_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    predicate_column_names: &[String],
) -> Vec<usize> {
    let parquet_schema = metadata.file_metadata().schema_descr();
    let mut set = std::collections::BTreeSet::new();
    for name in predicate_column_names {
        if let Ok(conv) = StatisticsConverter::try_new(name, arrow_schema, parquet_schema) {
            if let Some(idx) = conv.parquet_column_index() {
                set.insert(idx);
            }
        }
    }
    set.into_iter().collect()
}

/// Load + graft a scoped page index: ColumnIndex for `parquet_cols` (all RGs),
/// OffsetIndex for all columns/all RGs. The Step-1 baseline.
pub async fn load_scoped_page_index(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    parquet_cols: &[usize],
) -> Option<Arc<ParquetMetaData>> {
    load_combined(store, location, footer_meta, parquet_cols, None, None).await
}

/// Like [`load_scoped_page_index`], but the ColumnIndex is built only for the row
/// groups in `surviving_rgs` (footer-stats survivors — [`surviving_row_groups`]);
/// other RGs get a `NONE` ColumnIndex placeholder. OffsetIndex stays all-columns.
pub async fn load_scoped_page_index_rgs(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    parquet_cols: &[usize],
    surviving_rgs: &[usize],
) -> Option<Arc<ParquetMetaData>> {
    load_combined(store, location, footer_meta, parquet_cols, Some(surviving_rgs), None).await
}

/// Like [`load_scoped_page_index`], but the OffsetIndex is built only for
/// `offset_cols` (the loader unions in the predicate columns + column 0
/// defensively); other columns get an empty placeholder. ColumnIndex stays
/// all-RG. See [`OiKey`] for which columns must be real and why.
pub async fn load_scoped_page_index_cols(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    parquet_cols: &[usize],
    offset_cols: &[usize],
) -> Option<Arc<ParquetMetaData>> {
    load_combined(store, location, footer_meta, parquet_cols, None, Some(offset_cols)).await
}

/// Fully scoped: ColumnIndex RG-scoped to `surviving_rgs`, OffsetIndex
/// column-scoped to `offset_cols` (∪ predicate ∪ {0}). The Step-2 target both
/// scan paths call once they know their surviving-RG set and projection.
pub async fn load_scoped_page_index_scoped(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    parquet_cols: &[usize],
    surviving_rgs: &[usize],
    offset_cols: &[usize],
) -> Option<Arc<ParquetMetaData>> {
    load_combined(
        store,
        location,
        footer_meta,
        parquet_cols,
        Some(surviving_rgs),
        Some(offset_cols),
    )
    .await
}

async fn load_combined(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    parquet_cols: &[usize],
    surviving_rgs: Option<&[usize]>,
    offset_cols: Option<&[usize]>,
) -> Option<Arc<ParquetMetaData>> {
    if parquet_cols.is_empty() {
        return None;
    }
    let column_index =
        get_or_build_column_index(store, location, footer_meta, parquet_cols, surviving_rgs).await?;
    let offset_index =
        get_or_build_offset_index(store, location, footer_meta, parquet_cols, offset_cols).await?;
    Some(graft(footer_meta, column_index, offset_index))
}

/// Build a fresh `ParquetMetaData` = `footer` with the page-index pair grafted
/// on. Deep-clones the footer (the builder consumes an owned `ParquetMetaData`);
/// that transient clone is the only footer copy and is never cached.
fn graft(
    footer_meta: &Arc<ParquetMetaData>,
    column_index: ParquetColumnIndex,
    offset_index: ParquetOffsetIndex,
) -> Arc<ParquetMetaData> {
    let base = ParquetMetaData::clone(footer_meta);
    let rebuilt = base
        .into_builder()
        .set_column_index(Some(column_index))
        .set_offset_index(Some(offset_index))
        .build();
    Arc::new(rebuilt)
}

// ── ColumnIndex cache lookup + build (per `(file, col, rg)` cell) ────────────

/// Assemble the full-width `[rg][col]` `ColumnIndex` matrix (real cells only at
/// `parquet_cols` × built RGs; `NONE` everywhere else) by looking up each
/// `(file, col, rg)` cell in the cache and decoding only the cells that miss.
///
/// `surviving_rgs == None` builds every RG; `Some(set)` restricts the built RGs
/// to footer-stats survivors ([`surviving_row_groups`]). Either way a cell is
/// keyed solely on `(file, col, rg)`, so it is decoded once per file and reused
/// across every predicate combination and surviving-RG set that touches it.
async fn get_or_build_column_index(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    parquet_cols: &[usize],
    surviving_rgs: Option<&[usize]>,
) -> Option<ParquetColumnIndex> {
    let num_rgs = footer_meta.num_row_groups();
    if num_rgs == 0 {
        return None;
    }
    let num_cols = footer_meta.file_metadata().schema_descr().num_columns();
    if parquet_cols.iter().any(|&i| i >= num_cols) {
        return None;
    }

    // Which RGs to build the (heavy) predicate-column ColumnIndex for.
    let build_rgs: Vec<usize> = match surviving_rgs {
        None => (0..num_rgs).collect(),
        Some(set) => {
            let mut v: Vec<usize> = set.iter().copied().filter(|&r| r < num_rgs).collect();
            v.sort_unstable();
            v.dedup();
            v
        }
    };
    if build_rgs.is_empty() {
        // Nothing to build (e.g. an empty survivor set) → footer-only fallback.
        return None;
    }

    let path: Arc<str> = Arc::from(location.as_ref());
    let mut matrix: ParquetColumnIndex = (0..num_rgs)
        .map(|_| (0..num_cols).map(|_| ColumnIndexMetaData::NONE).collect())
        .collect();

    // Phase 1: serve every needed cell that is already cached; collect misses.
    let mut missing: Vec<(usize, usize)> = Vec::new(); // (col, rg)
    if let Ok(mut cache) = COLUMN_INDEX_CACHE.lock() {
        for &rg in &build_rgs {
            for &col in parquet_cols {
                let key = CiCellKey { path: path.clone(), col, rg };
                match cache.get(&key) {
                    Some(cell) => matrix[rg][col] = cell,
                    None => missing.push((col, rg)),
                }
            }
        }
    } else {
        return None;
    }

    // Phase 2: decode the missing cells (vectored fetch grouped by RG), place
    // them in the matrix, and populate the cache.
    if !missing.is_empty() {
        let built = build_column_index_cells(store, location, footer_meta, &missing).await?;
        if let Ok(mut cache) = COLUMN_INDEX_CACHE.lock() {
            for (col, rg, cell, size) in built {
                matrix[rg][col] = cell.clone();
                cache.insert(CiCellKey { path: path.clone(), col, rg }, cell, size);
            }
        }
    }

    Some(matrix)
}

/// Range-read + decode the requested `(col, rg)` ColumnIndex cells, grouping by
/// row group so each RG's columns share one vectored fetch + decode. Returns one
/// `(col, rg, ColumnIndexMetaData, size)` per requested cell. `None` if any
/// requested column lacks a column-index range (→ footer-only fallback).
async fn build_column_index_cells(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    missing: &[(usize, usize)],
) -> Option<Vec<(usize, usize, ColumnIndexMetaData, usize)>> {
    use std::collections::BTreeMap;
    let mut by_rg: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for &(col, rg) in missing {
        by_rg.entry(rg).or_default().push(col);
    }

    struct RgPlan {
        rg: usize,
        cols: Vec<usize>,
        chunks: Vec<ColumnChunkMetaData>,
        range: Range<u64>,
    }
    let mut plans: Vec<RgPlan> = Vec::with_capacity(by_rg.len());
    let mut fetch_ranges: Vec<Range<u64>> = Vec::with_capacity(by_rg.len());
    for (rg, mut cols) in by_rg {
        cols.sort_unstable();
        cols.dedup();
        let rgm = footer_meta.row_group(rg);
        let chunks: Vec<ColumnChunkMetaData> = cols.iter().map(|&i| rgm.column(i).clone()).collect();
        let range = column_index_union(&chunks)?;
        fetch_ranges.push(range.clone());
        plans.push(RgPlan { rg, cols, chunks, range });
    }
    if plans.is_empty() {
        return None;
    }

    let buffers = store.get_ranges(location, &fetch_ranges).await.ok()?;
    if buffers.len() != fetch_ranges.len() {
        return None;
    }

    let mut out: Vec<(usize, usize, ColumnIndexMetaData, usize)> = Vec::with_capacity(missing.len());
    for (plan, buf) in plans.iter().zip(buffers.iter()) {
        let reader = BufferChunkReader { base: plan.range.start, bytes: buf.clone() };
        // Deprecated but the only PUBLIC column-subset decoder (arrow-rs#8643).
        #[allow(deprecated)]
        let decoded = read_columns_indexes(&reader, &plan.chunks).ok()??;
        if decoded.len() != plan.cols.len() {
            return None;
        }
        let rgm = footer_meta.row_group(plan.rg);
        for (k, &col) in plan.cols.iter().enumerate() {
            let size = rgm.column(col).column_index_length().unwrap_or(0).max(0) as usize;
            out.push((col, plan.rg, decoded[k].clone(), size));
        }
    }
    Some(out)
}

// ── OffsetIndex cache lookup + build (per `(file, col)` cell, all RGs) ───────

/// Assemble the full-width `[rg][col]` `OffsetIndex` matrix (real entries only at
/// the resolved offset columns; empty placeholders elsewhere) from per-`(file,
/// col)` cells, decoding only the columns that miss.
///
/// The resolved offset-column set is `predicate ∪ projection ∪ {0}` (`offset_cols
/// == None` → all columns); see [`OiCellKey`] for why each must be real. Each
/// cached cell is a column's OffsetIndex across **all** row groups, keyed only on
/// `(file, col)`, so it is decoded once per file and reused across every query
/// that reads that column irrespective of projection or predicate.
async fn get_or_build_offset_index(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    parquet_cols: &[usize],
    offset_cols: Option<&[usize]>,
) -> Option<ParquetOffsetIndex> {
    let num_rgs = footer_meta.num_row_groups();
    if num_rgs == 0 {
        return None;
    }
    let num_cols = footer_meta.file_metadata().schema_descr().num_columns();

    // Resolve which columns need a real OffsetIndex: predicate ∪ projection ∪ {0},
    // clamped. `None` → all columns.
    let off_cols: Vec<usize> = match offset_cols {
        None => (0..num_cols).collect(),
        Some(cols) => {
            let mut set: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
            set.insert(0); // metric reads column 0
            for &c in parquet_cols {
                set.insert(c); // prune reads predicate cols
            }
            for &c in cols {
                set.insert(c); // read needs projected cols
            }
            set.into_iter().filter(|&c| c < num_cols).collect()
        }
    };
    if off_cols.is_empty() {
        return None;
    }

    let path: Arc<str> = Arc::from(location.as_ref());
    let mut matrix: ParquetOffsetIndex = (0..num_rgs)
        .map(|_| (0..num_cols).map(|_| OffsetIndexBuilder::new().build()).collect())
        .collect();

    // Phase 1: serve cached columns; collect misses.
    let mut missing: Vec<usize> = Vec::new();
    if let Ok(mut cache) = OFFSET_INDEX_CACHE.lock() {
        for &col in &off_cols {
            let key = OiCellKey { path: path.clone(), col };
            match cache.get(&key) {
                Some(column) => scatter_offset_column(&mut matrix, col, &column),
                None => missing.push(col),
            }
        }
    } else {
        return None;
    }

    // Phase 2: decode the missing columns (each spanning all RGs), scatter into
    // the matrix, and populate the cache.
    if !missing.is_empty() {
        let built = build_offset_index_columns(store, location, footer_meta, &missing, num_rgs).await?;
        if let Ok(mut cache) = OFFSET_INDEX_CACHE.lock() {
            for (col, column, size) in built {
                scatter_offset_column(&mut matrix, col, &column);
                cache.insert(OiCellKey { path: path.clone(), col }, column, size);
            }
        }
    }

    Some(matrix)
}

/// Place a column's all-RG OffsetIndex (indexed by RG) into the matrix at `col`.
fn scatter_offset_column(matrix: &mut ParquetOffsetIndex, col: usize, column: &OiColumn) {
    for (rg, entry) in column.iter().enumerate() {
        if rg < matrix.len() {
            matrix[rg][col] = entry.clone();
        }
    }
}

/// Range-read + decode the OffsetIndex for each requested column across **every**
/// row group (read-time safety — see [`OiCellKey`]). Returns one `(col, all-RG
/// OffsetIndex, size)` per requested column. `None` if any column lacks an
/// offset-index range (→ footer-only fallback).
async fn build_offset_index_columns(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    cols: &[usize],
    num_rgs: usize,
) -> Option<Vec<(usize, OiColumn, usize)>> {
    // Group by RG: one vectored fetch + decode per RG over the requested columns.
    struct RgPlan {
        rg_idx: usize,
        chunks: Vec<ColumnChunkMetaData>,
        range: Range<u64>,
    }
    let mut plans: Vec<RgPlan> = Vec::with_capacity(num_rgs);
    let mut fetch_ranges: Vec<Range<u64>> = Vec::with_capacity(num_rgs);
    for rg_idx in 0..num_rgs {
        let rg = footer_meta.row_group(rg_idx);
        let chunks: Vec<ColumnChunkMetaData> = cols.iter().map(|&i| rg.column(i).clone()).collect();
        let range = offset_index_union(&chunks)?;
        fetch_ranges.push(range.clone());
        plans.push(RgPlan { rg_idx, chunks, range });
    }
    if plans.is_empty() {
        return None;
    }

    let buffers = store.get_ranges(location, &fetch_ranges).await.ok()?;
    if buffers.len() != fetch_ranges.len() {
        return None;
    }

    // Per-column accumulator, one slot per RG (filled in RG order below).
    let mut columns: Vec<OiColumn> = cols
        .iter()
        .map(|_| Vec::with_capacity(num_rgs))
        .collect();
    for (plan, buf) in plans.iter().zip(buffers.iter()) {
        let reader = BufferChunkReader { base: plan.range.start, bytes: buf.clone() };
        #[allow(deprecated)]
        let decoded = read_offset_indexes(&reader, &plan.chunks).ok()??;
        if decoded.len() != cols.len() {
            return None;
        }
        for (k, entry) in decoded.into_iter().enumerate() {
            columns[k].push(entry);
        }
    }

    let mut out: Vec<(usize, OiColumn, usize)> = Vec::with_capacity(cols.len());
    for (k, &col) in cols.iter().enumerate() {
        let mut size = 0usize;
        for rg in footer_meta.row_groups() {
            size += rg.column(col).offset_index_length().unwrap_or(0).max(0) as usize;
        }
        out.push((col, std::mem::take(&mut columns[k]), size));
    }
    Some(out)
}

// ── Surviving-RG computation (footer-stats prune; superset of DF's set) ──────

/// Compute the row groups that pass footer RG-statistics pruning for `predicate`.
///
/// A **superset** of the row groups DataFusion will scan (DataFusion applies the
/// same footer-stats pruning plus bloom/range/limit, which only remove more), so
/// scoping the predicate-column ColumnIndex to this set is safe. Returns all row
/// groups if the predicate can't be lowered or stats are missing. Deterministic
/// in `(footer_meta, schema, predicate)` → both scan paths agree.
pub fn surviving_row_groups(
    footer_meta: &ParquetMetaData,
    arrow_schema: &SchemaRef,
    predicate: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
) -> Vec<usize> {
    use arrow::array::{ArrayRef, BooleanArray, UInt64Array};
    use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
    use datafusion::scalar::ScalarValue;

    let num_rgs = footer_meta.num_row_groups();
    let all: Vec<usize> = (0..num_rgs).collect();
    if num_rgs == 0 {
        return all;
    }

    let Ok(pp) = PruningPredicate::try_new(Arc::clone(predicate), Arc::clone(arrow_schema)) else {
        return all;
    };

    struct RgStats<'a> {
        meta: &'a ParquetMetaData,
        schema: &'a SchemaRef,
        num_rgs: usize,
    }
    impl<'a> RgStats<'a> {
        fn conv(&self, col: &str) -> Option<StatisticsConverter<'_>> {
            StatisticsConverter::try_new(col, self.schema, self.meta.file_metadata().schema_descr())
                .ok()
        }
    }
    impl<'a> PruningStatistics for RgStats<'a> {
        fn min_values(&self, column: &datafusion::common::Column) -> Option<ArrayRef> {
            self.conv(&column.name)?
                .row_group_mins(self.meta.row_groups().iter())
                .ok()
        }
        fn max_values(&self, column: &datafusion::common::Column) -> Option<ArrayRef> {
            self.conv(&column.name)?
                .row_group_maxes(self.meta.row_groups().iter())
                .ok()
        }
        fn num_containers(&self) -> usize {
            self.num_rgs
        }
        fn null_counts(&self, column: &datafusion::common::Column) -> Option<ArrayRef> {
            self.conv(&column.name)?
                .row_group_null_counts(self.meta.row_groups().iter())
                .ok()
                .map(|a| Arc::new(a) as ArrayRef)
        }
        fn row_counts(&self) -> Option<ArrayRef> {
            let counts: Vec<u64> =
                self.meta.row_groups().iter().map(|rg| rg.num_rows() as u64).collect();
            Some(Arc::new(UInt64Array::from(counts)) as ArrayRef)
        }
        fn contained(
            &self,
            _column: &datafusion::common::Column,
            _values: &std::collections::HashSet<ScalarValue>,
        ) -> Option<BooleanArray> {
            None
        }
    }

    let stats = RgStats { meta: footer_meta, schema: arrow_schema, num_rgs };
    match pp.prune(&stats) {
        Ok(mask) => mask
            .iter()
            .enumerate()
            .filter_map(|(i, keep)| if *keep { Some(i) } else { None })
            .collect(),
        Err(_) => all,
    }
}

// ── Byte-range helpers + in-memory ChunkReader ──────────────────────────────

/// Union of `column_index` byte ranges across the given column chunks. `None` if
/// any chunk lacks a column index (we require all predicate columns to have one,
/// else fall back to footer-only).
fn column_index_union(chunks: &[ColumnChunkMetaData]) -> Option<Range<u64>> {
    range_union(chunks, |c| {
        let off = u64::try_from(c.column_index_offset()?).ok()?;
        let len = u64::try_from(c.column_index_length()?).ok()?;
        Some(off..off + len)
    })
}

/// Union of `offset_index` byte ranges across the given column chunks.
fn offset_index_union(chunks: &[ColumnChunkMetaData]) -> Option<Range<u64>> {
    range_union(chunks, |c| {
        let off = u64::try_from(c.offset_index_offset()?).ok()?;
        let len = u64::try_from(c.offset_index_length()?).ok()?;
        Some(off..off + len)
    })
}

fn range_union(
    chunks: &[ColumnChunkMetaData],
    f: impl Fn(&ColumnChunkMetaData) -> Option<Range<u64>>,
) -> Option<Range<u64>> {
    let mut acc: Option<Range<u64>> = None;
    for c in chunks {
        let r = f(c)?; // any missing range → bail (caller falls back)
        acc = Some(match acc {
            None => r,
            Some(a) => a.start.min(r.start)..a.end.max(r.end),
        });
    }
    acc
}

/// A [`ChunkReader`] over an in-memory byte buffer representing the file region
/// `[base, base + bytes.len())`. The arrow-rs page-index readers call
/// `get_bytes(absolute_offset, len)`; we translate into the buffer.
struct BufferChunkReader {
    base: u64,
    bytes: Bytes,
}

impl Length for BufferChunkReader {
    fn len(&self) -> u64 {
        self.base + self.bytes.len() as u64
    }
}

impl ChunkReader for BufferChunkReader {
    type T = prost::bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        let rel = self.rel(start, 0)?;
        Ok(self.bytes.slice(rel..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let rel = self.rel(start, length)?;
        Ok(self.bytes.slice(rel..rel + length))
    }
}

impl BufferChunkReader {
    fn rel(&self, start: u64, length: usize) -> ParquetResult<usize> {
        let rel = start.checked_sub(self.base).ok_or_else(|| {
            ParquetError::General(format!(
                "page-index read offset {} precedes buffer base {}",
                start, self.base
            ))
        })?;
        let rel = usize::try_from(rel)
            .map_err(|e| ParquetError::General(format!("offset overflow: {}", e)))?;
        if rel + length > self.bytes.len() {
            return Err(ParquetError::General(format!(
                "page-index read [{}..{}) exceeds buffer of len {}",
                rel,
                rel + length,
                self.bytes.len()
            )));
        }
        Ok(rel)
    }
}

// ── Test-only helpers ────────────────────────────────────────────────────────

/// Crate-wide guard so every test that touches the process-global caches mutually
/// excludes (distinct fixtures alone aren't enough — the `InMemory` path is always
/// "data.parquet"). Shared (not per-module) so all cache users serialize.
#[cfg(test)]
pub(crate) static SCOPED_CACHE_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Clear both caches AND restore the default limit on each.
#[cfg(test)]
pub(crate) fn clear_scoped_cache_for_test() {
    if let Ok(mut c) = COLUMN_INDEX_CACHE.lock() {
        c.clear_keep_limit();
        c.limit = DEFAULT_SCOPED_CACHE_LIMIT;
    }
    if let Ok(mut c) = OFFSET_INDEX_CACHE.lock() {
        c.clear_keep_limit();
        c.limit = DEFAULT_SCOPED_CACHE_LIMIT;
    }
}

#[cfg(test)]
pub(crate) fn set_column_index_cache_limit_for_test(limit: usize) {
    if let Ok(mut c) = COLUMN_INDEX_CACHE.lock() {
        c.set_limit(limit);
    }
}

/// Combined view (sum of both caches) — test-only convenience for assertions that
/// only need "is the scoped machinery doing anything". Production code reads the
/// two caches separately ([`column_index_cache_stats`] / [`offset_index_cache_stats`]).
#[cfg(test)]
pub(crate) fn scoped_cache_stats() -> ScopedCacheStats {
    let a = column_index_cache_stats();
    let b = offset_index_cache_stats();
    ScopedCacheStats {
        hits: a.hits + b.hits,
        misses: a.misses + b.misses,
        evictions: a.evictions + b.evictions,
        entries: a.entries + b.entries,
        used_bytes: a.used_bytes + b.used_bytes,
        limit_bytes: a.limit_bytes.max(b.limit_bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::page_pruner::{build_pruning_predicate, PagePruner};
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::parquet::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
    };
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
    use datafusion::physical_expr::PhysicalExpr;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::SCOPED_CACHE_TEST_GUARD as CACHE_TEST_GUARD;

    // ── fixtures + expr helpers ──────────────────────────────────────────

    /// 2 columns (`price`, `qty`), 32 rows, 1 row group, 4 pages of 8 rows.
    fn two_col_parquet() -> (Bytes, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int32, false),
            Field::new("qty", DataType::Int32, false),
        ]));
        let prices: Vec<i32> = (0..32).collect();
        let qtys: Vec<i32> = (100..132).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(prices)), Arc::new(Int32Array::from(qtys))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        (Bytes::from(buf), schema)
    }

    /// 4 row groups of 10 rows (`id` 0..40, `v` = id*2), page size 5.
    fn four_rg_parquet() -> (Bytes, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let ids: Vec<i32> = (0..40).collect();
        let vs: Vec<i32> = (0..40).map(|x| x * 2).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(ids)), Arc::new(Int32Array::from(vs))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(10)
            .set_data_page_row_count_limit(5)
            .set_write_batch_size(5)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        (Bytes::from(buf), schema)
    }

    /// 4 columns (2 int `n0`,`n1` + 2 wide string `s0`,`s1`), 1 RG, multiple pages.
    fn wide4_parquet() -> (Bytes, SchemaRef) {
        use arrow::array::StringArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("n0", DataType::Int32, false),
            Field::new("n1", DataType::Int32, false),
            Field::new("s0", DataType::Utf8, false),
            Field::new("s1", DataType::Utf8, false),
        ]));
        const ROWS: i32 = 256;
        let n0: Vec<i32> = (0..ROWS).collect();
        let n1: Vec<i32> = (0..ROWS).collect();
        let s0: Vec<String> = (0..ROWS).map(|r| format!("s0_{r:05}_padpadpad")).collect();
        let s1: Vec<String> = (0..ROWS).map(|r| format!("s1_{r:05}_padpadpad")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(n0)),
                Arc::new(Int32Array::from(n1)),
                Arc::new(StringArray::from(s0)),
                Arc::new(StringArray::from(s1)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(ROWS as usize)
            .set_data_page_row_count_limit(32)
            .set_write_batch_size(32)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        (Bytes::from(buf), schema)
    }

    async fn stage(bytes: Bytes) -> (Arc<dyn ObjectStore>, ObjPath) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = ObjPath::from("data.parquet");
        store.put(&loc, PutPayload::from_bytes(bytes)).await.unwrap();
        (store, loc)
    }

    fn footer_only(bytes: &Bytes) -> Arc<ParquetMetaData> {
        ArrowReaderMetadata::load(&bytes.clone(), ArrowReaderOptions::new().with_page_index(false))
            .unwrap()
            .metadata()
            .clone()
    }

    fn full_index(bytes: &Bytes) -> Arc<ParquetMetaData> {
        ArrowReaderMetadata::load(&bytes.clone(), ArrowReaderOptions::new().with_page_index(true))
            .unwrap()
            .metadata()
            .clone()
    }

    fn col(name: &str, idx: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(PhysColumn::new(name, idx))
    }
    fn lit_int(v: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Int32(Some(v))))
    }
    fn pred(name: &str, idx: usize, op: Operator, v: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(col(name, idx), op, lit_int(v)))
    }
    fn kept(sel: &RowSelection) -> usize {
        sel.iter().filter(|s| !s.skip).map(|s| s.row_count).sum()
    }
    fn ci() -> ScopedCacheStats {
        column_index_cache_stats()
    }
    fn oi() -> ScopedCacheStats {
        offset_index_cache_stats()
    }

    fn read_selected_column(
        bytes: &Bytes,
        meta: &Arc<ParquetMetaData>,
        leaf_col: usize,
        selection: RowSelection,
    ) -> std::result::Result<Vec<i32>, String> {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use datafusion::parquet::arrow::ProjectionMask;

        let arm = ArrowReaderMetadata::try_new(Arc::clone(meta), ArrowReaderOptions::new())
            .map_err(|e| format!("try_new metadata: {e}"))?;
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(bytes.clone(), arm);
        let proj = ProjectionMask::leaves(builder.parquet_schema(), [leaf_col]);
        let mut reader = builder
            .with_row_groups(vec![0])
            .with_projection(proj)
            .with_row_selection(selection)
            .build()
            .map_err(|e| format!("build reader: {e}"))?;
        let mut out = Vec::new();
        while let Some(next) = reader.next() {
            let batch = next.map_err(|e| format!("read batch: {e}"))?;
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("projected column was not Int32")?;
            for i in 0..a.len() {
                out.push(a.value(i));
            }
        }
        Ok(out)
    }

    // ── baseline / correctness ────────────────────────────────────────────

    #[tokio::test]
    async fn footer_only_has_no_page_index() {
        let (bytes, _schema) = two_col_parquet();
        let fo = footer_only(&bytes);
        assert!(fo.column_index().is_none());
        assert!(fo.offset_index().is_none());
    }

    #[tokio::test]
    async fn empty_column_set_returns_none() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, _schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        assert!(load_scoped_page_index(&store, &loc, &fo, &[]).await.is_none());
        assert_eq!(ci().entries, 0);
        assert_eq!(oi().entries, 0);
    }

    #[tokio::test]
    async fn scoped_index_is_predicate_scoped_for_column_index() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);

        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        assert_eq!(cols, vec![0]);

        let aug = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        let c = aug.column_index().unwrap();
        let o = aug.offset_index().unwrap();
        assert!(!matches!(c[0][0], ColumnIndexMetaData::NONE), "predicate col has real CI");
        assert!(matches!(c[0][1], ColumnIndexMetaData::NONE), "non-predicate col CI is NONE");
        assert!(
            !o[0][0].page_locations().is_empty() && !o[0][1].page_locations().is_empty(),
            "OffsetIndex real for every column (all-col default)"
        );
    }

    #[tokio::test]
    async fn scoped_pruning_matches_full_index() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let aug = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        let full = full_index(&bytes);
        let pp = build_pruning_predicate(&pred("price", 0, Operator::GtEq, 20), schema.clone()).unwrap();
        let s = PagePruner::new(&schema, Arc::clone(&aug)).prune_rg(&pp, 0, None);
        let f = PagePruner::new(&schema, full).prune_rg(&pp, 0, None);
        assert_eq!(s.as_ref().map(kept), f.as_ref().map(kept));
        assert_eq!(s.as_ref().map(kept), Some(16));
    }

    #[tokio::test]
    async fn scoped_index_reads_non_predicate_projected_column() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let aug = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        let selection = RowSelection::from(vec![RowSelector::skip(16), RowSelector::select(16)]);
        let scoped_vals = read_selected_column(&bytes, &aug, 1, selection.clone()).unwrap();
        let full = full_index(&bytes);
        let full_vals = read_selected_column(&bytes, &full, 1, selection).unwrap();
        let expected: Vec<i32> = (116..132).collect();
        assert_eq!(scoped_vals, expected);
        assert_eq!(scoped_vals, full_vals);
    }

    // ── cache behavior: hits, independence, eviction ──────────────────────

    /// Second identical load is a pure hit in BOTH caches; no new cells/bytes.
    /// Cells: predicate `price` → 1 CI cell `(col0,rg0)`; all-column OffsetIndex
    /// (the default) → 2 OI cells (one per column).
    #[tokio::test]
    async fn second_load_is_cache_hit() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);

        let _ = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        let (c1, o1) = (ci(), oi());
        assert_eq!((c1.hits, c1.misses, c1.entries), (0, 1, 1), "1 CI cell (price,rg0)");
        assert_eq!((o1.hits, o1.misses, o1.entries), (0, 2, 2), "2 OI cells (col0,col1)");
        assert!(c1.used_bytes > 0 && o1.used_bytes > 0);

        let _ = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        let (c2, o2) = (ci(), oi());
        assert_eq!((c2.hits, c2.misses, c2.entries, c2.used_bytes), (1, 1, 1, c1.used_bytes));
        assert_eq!((o2.hits, o2.misses, o2.entries, o2.used_bytes), (2, 2, 2, o1.used_bytes));
    }

    /// Distinct predicate columns → distinct CI cells, but the OffsetIndex column
    /// cells are SHARED. Both loads default to the all-column OffsetIndex, so the
    /// second load re-reads the SAME 2 OI cells from cache (no new cells). This is
    /// the whole point of cell-keying: a column's index is stored once per file.
    #[tokio::test]
    async fn distinct_predicates_share_offset_index() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);

        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();

        assert_eq!(ci().entries, 2, "distinct predicate cells: (price,rg0) + (qty,rg0)");
        assert_eq!(oi().entries, 2, "all-column OffsetIndex: 2 column cells, shared");
        // Second (qty) load re-read the same 2 OI cells from cache.
        assert_eq!(oi().hits, 2);
    }

    /// The cell-keying payoff: a predicate that ADDS a column reuses the cell the
    /// first predicate already decoded, instead of re-decoding it inside a new
    /// set-keyed entry. `price` then `{price, qty}` → `price`'s cell is a HIT; only
    /// `qty`'s cell is freshly decoded. (Under the old set-keyed cache this was a
    /// full miss that re-decoded `price`.)
    #[tokio::test]
    async fn adding_predicate_column_reuses_existing_cell() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_both = resolve_predicate_parquet_columns(
            &schema,
            &fo,
            &["price".to_string(), "qty".to_string()],
        );

        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        assert_eq!((ci().hits, ci().misses, ci().entries), (0, 1, 1), "price cell decoded");

        // Predicate now covers {price, qty}: price's cell hits, qty's cell misses.
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_both).await.unwrap();
        assert_eq!(
            (ci().hits, ci().misses, ci().entries),
            (1, 2, 2),
            "price cell reused (hit); only qty cell freshly decoded"
        );
        clear_scoped_cache_for_test();
    }

    /// Two predicates on the SAME column with DIFFERENT literals resolve to the
    /// same `(file, col)` parquet column, so they share the one CI cell — predicate
    /// *value* never multiplies cache entries. (`status>=400` vs `status>=100`.)
    #[tokio::test]
    async fn different_literals_same_column_share_cell() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        // Both predicates are on `price` (col 0) — only the literal differs, which
        // never enters the cache key.
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);

        let _ = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        let _ = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        assert_eq!(ci().entries, 1, "same column → one cell regardless of literal");
        assert_eq!(ci().hits, 1);
        clear_scoped_cache_for_test();
    }

    /// CI hit/miss accounting across two predicate-column sets.
    #[tokio::test]
    async fn stats_count_hits_and_misses() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);

        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        assert_eq!((ci().hits, ci().misses), (0, 1));
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        assert_eq!((ci().hits, ci().misses), (1, 1));
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        assert_eq!((ci().hits, ci().misses), (1, 2));
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        let s = ci();
        assert_eq!((s.hits, s.misses, s.entries, s.evictions), (3, 2, 2, 0));
    }

    /// Byte-bounded LRU on the (now cell-keyed) ColumnIndex cache: with the budget
    /// sized to hold ~1.5 cells, loading two distinct column cells evicts the LRU
    /// one; the cache never exceeds its limit and never degrades to "cache
    /// nothing"; the most-recently-used cell survives.
    #[tokio::test]
    async fn lru_evicts_over_byte_budget() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);

        // Measure one CI cell (predicate `price` = col0 at the single RG), then set
        // a budget of ~1.5 cells so a second distinct cell forces an eviction.
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let one_cell = ci().used_bytes;
        assert!(one_cell > 0);
        let budget = one_cell + one_cell / 2;
        clear_scoped_cache_for_test();
        set_column_index_cache_limit_for_test(budget);

        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap(); // cell (col0)
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap(); // cell (col1) → evicts col0

        assert!(ci().used_bytes <= budget, "CI bytes {} must stay within {}", ci().used_bytes, budget);
        assert_eq!(ci().entries, 1, "only the most-recent cell fits");
        assert!(ci().evictions >= 1, "the LRU cell must have evicted");

        // The most-recently-used cell (qty/col1) must still be a hit.
        let hits_before = ci().hits;
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        assert_eq!(ci().hits, hits_before + 1, "MRU cell must remain cached");

        clear_scoped_cache_for_test();
    }

    // ── Step 2: RG-scoping the ColumnIndex ────────────────────────────────

    #[tokio::test]
    async fn surviving_row_groups_matches_footer_stats_prune() {
        let (bytes, schema) = four_rg_parquet();
        let fo = footer_only(&bytes);
        assert_eq!(fo.num_row_groups(), 4);
        let p = pred("id", 0, Operator::GtEq, 25);
        assert_eq!(surviving_row_groups(&fo, &schema, &p), vec![2, 3]);
        let p2 = pred("id", 0, Operator::Lt, 12);
        assert_eq!(surviving_row_groups(&fo, &schema, &p2), vec![0, 1]);
    }

    #[tokio::test]
    async fn rg_scoped_load_builds_column_index_only_for_survivors() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = four_rg_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["id".to_string()]);
        let surviving = vec![2usize, 3usize];

        let aug = load_scoped_page_index_rgs(&store, &loc, &fo, &cols, &surviving).await.unwrap();
        let c = aug.column_index().unwrap();
        let o = aug.offset_index().unwrap();
        assert_eq!(c.len(), 4);
        for &rg in &surviving {
            assert!(!matches!(c[rg][0], ColumnIndexMetaData::NONE), "survivor RG {rg} real CI");
        }
        for &rg in &[0usize, 1usize] {
            assert!(matches!(c[rg][0], ColumnIndexMetaData::NONE), "pruned RG {rg} NONE CI");
        }
        for rg in 0..4 {
            for cc in 0..2 {
                assert!(!o[rg][cc].page_locations().is_empty(), "OI real for all rg/col");
            }
        }
        let full = full_index(&bytes);
        let pp = build_pruning_predicate(&pred("id", 0, Operator::GtEq, 25), schema.clone()).unwrap();
        let s = PagePruner::new(&schema, Arc::clone(&aug)).prune_rg(&pp, 2, None);
        let f = PagePruner::new(&schema, full).prune_rg(&pp, 2, None);
        assert_eq!(s.as_ref().map(kept), f.as_ref().map(kept));
        clear_scoped_cache_for_test();
    }

    #[tokio::test]
    async fn rg_scoping_reduces_column_index_bytes() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = four_rg_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["id".to_string()]);

        let _ = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        let all_rg = ci().used_bytes;
        clear_scoped_cache_for_test();
        let _ = load_scoped_page_index_rgs(&store, &loc, &fo, &cols, &[2, 3]).await.unwrap();
        assert!(ci().used_bytes < all_rg, "RG-scoped CI bytes {} < all-RG {}", ci().used_bytes, all_rg);
        clear_scoped_cache_for_test();
    }

    /// CI cells are keyed per `(col, rg)`. Loading survivors {2,3} caches cells
    /// (id,rg2) + (id,rg3); reloading the same survivor set hits both; a different
    /// survivor set {0,1} adds two fresh cells. So a column's per-RG index is
    /// reused across overlapping survivor sets instead of re-decoded per set.
    #[tokio::test]
    async fn rg_scoped_key_includes_surviving_rgs() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = four_rg_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["id".to_string()]);

        let _ = load_scoped_page_index_rgs(&store, &loc, &fo, &cols, &[2, 3]).await.unwrap();
        assert_eq!((ci().misses, ci().entries), (2, 2), "cells (id,rg2)+(id,rg3)");
        let _ = load_scoped_page_index_rgs(&store, &loc, &fo, &cols, &[2, 3]).await.unwrap();
        assert_eq!((ci().hits, ci().entries), (2, 2), "same survivors → both cells hit");
        let _ = load_scoped_page_index_rgs(&store, &loc, &fo, &cols, &[0, 1]).await.unwrap();
        assert_eq!((ci().misses, ci().entries), (4, 4), "new survivors → 2 fresh cells");
        // OI stayed all-columns across all three → 2 column cells, shared.
        assert_eq!(oi().entries, 2);
        clear_scoped_cache_for_test();
    }

    /// Partial-overlap survivor sets only decode the NEW row groups. Load
    /// survivors {2,3} (cells rg2,rg3), then {1,2,3}: rg2+rg3 hit, only rg1 is
    /// freshly decoded. Proves RG-scoping reuses per-RG cells across overlapping
    /// survivor sets rather than re-decoding the whole set.
    #[tokio::test]
    async fn overlapping_survivor_sets_decode_only_new_rgs() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = four_rg_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["id".to_string()]);

        let _ = load_scoped_page_index_rgs(&store, &loc, &fo, &cols, &[2, 3]).await.unwrap();
        assert_eq!((ci().hits, ci().misses, ci().entries), (0, 2, 2), "cells (id,rg2)+(id,rg3)");

        // {1,2,3}: rg2 & rg3 are cached (2 hits); only rg1 is new (1 miss).
        let _ = load_scoped_page_index_rgs(&store, &loc, &fo, &cols, &[1, 2, 3]).await.unwrap();
        assert_eq!(
            (ci().hits, ci().misses, ci().entries),
            (2, 3, 3),
            "rg2+rg3 reused (2 hits); only rg1 freshly decoded"
        );
        clear_scoped_cache_for_test();
    }

    /// The combined payoff across BOTH axes: a second query that adds a new
    /// predicate column AND scans a wider RG set decodes only the genuinely new
    /// `(col, rg)` cells. Uses `wide4` (1 RG) for the column axis and asserts CI
    /// cell-level hit/miss deltas.
    #[tokio::test]
    async fn new_column_combination_caches_only_new_column_cells() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = wide4_parquet(); // n0,n1,s0,s1 — 1 RG
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_n0 = resolve_predicate_parquet_columns(&schema, &fo, &["n0".to_string()]);
        let c_n0_n1 = resolve_predicate_parquet_columns(
            &schema,
            &fo,
            &["n0".to_string(), "n1".to_string()],
        );
        let c_n1_s0 = resolve_predicate_parquet_columns(
            &schema,
            &fo,
            &["n1".to_string(), "s0".to_string()],
        );

        // {n0}: 1 new cell.
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_n0).await.unwrap();
        assert_eq!((ci().hits, ci().misses, ci().entries), (0, 1, 1));
        // {n0,n1}: n0 hits, n1 new.
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_n0_n1).await.unwrap();
        assert_eq!((ci().hits, ci().misses, ci().entries), (1, 2, 2), "n0 reused; n1 new");
        // {n1,s0}: n1 hits, s0 new.
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_n1_s0).await.unwrap();
        assert_eq!((ci().hits, ci().misses, ci().entries), (2, 3, 3), "n1 reused; s0 new");
        clear_scoped_cache_for_test();
    }

    /// OffsetIndex equivalent: different projections cache only the new column
    /// cells. Project {s0} (offset cols n1∪s0∪{0}), then {s1} (offset cols
    /// n1∪s1∪{0}) — the shared cols (0, n1) hit; only the genuinely new projected
    /// column is decoded.
    #[tokio::test]
    async fn different_projections_cache_only_new_offset_columns() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = wide4_parquet(); // n0=0,n1=1,s0=2,s1=3 — 1 RG
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["n1".to_string()]);

        // Project s0 (col 2): offset cols = {0, 1(n1), 2(s0)} → 3 new cells.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[2]).await.unwrap();
        assert_eq!((oi().hits, oi().misses, oi().entries), (0, 3, 3), "cols 0,1,2");

        // Project s1 (col 3): offset cols = {0, 1, 3}. Cols 0 & 1 hit; col 3 new.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[3]).await.unwrap();
        assert_eq!(
            (oi().hits, oi().misses, oi().entries),
            (2, 4, 4),
            "cols 0,1 reused (2 hits); only col 3 freshly decoded"
        );
        clear_scoped_cache_for_test();
    }

    // ── Step 2: column-scoping the OffsetIndex ────────────────────────────

    #[tokio::test]
    async fn col_scoped_offset_index_only_for_requested_columns() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = wide4_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["n1".to_string()]);
        assert_eq!(pred_cols, vec![1]);

        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[2]).await.unwrap();
        let o = aug.offset_index().unwrap();
        for &c in &[0usize, 1, 2] {
            assert!(!o[0][c].page_locations().is_empty(), "col {c} (pred/proj/metric) real OI");
        }
        assert!(o[0][3].page_locations().is_empty(), "col 3 OI is empty placeholder");
    }

    #[tokio::test]
    async fn col_scoped_reads_projected_non_predicate_column() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let aug = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[1]).await.unwrap();
        let selection = RowSelection::from(vec![RowSelector::skip(16), RowSelector::select(16)]);
        let scoped_vals = read_selected_column(&bytes, &aug, 1, selection.clone()).unwrap();
        let full = full_index(&bytes);
        let full_vals = read_selected_column(&bytes, &full, 1, selection).unwrap();
        let expected: Vec<i32> = (116..132).collect();
        assert_eq!(scoped_vals, expected);
        assert_eq!(scoped_vals, full_vals);
    }

    #[tokio::test]
    async fn col_scoping_reduces_offset_index_bytes() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = wide4_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["n1".to_string()]);

        let _ = load_scoped_page_index(&store, &loc, &fo, &pred_cols).await.unwrap();
        let all_cols = oi().used_bytes;
        clear_scoped_cache_for_test();
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[2]).await.unwrap();
        assert!(oi().used_bytes < all_cols, "col-scoped OI {} < all-col {}", oi().used_bytes, all_cols);
        clear_scoped_cache_for_test();
    }

    /// Cell-keying makes OffsetIndex reuse automatic: an all-columns load caches
    /// per-column cells, and a later column-scoped load whose set is covered by
    /// those cells hits them — no new entries, no special "collapse to all-columns
    /// sentinel" needed (the prior set-keyed design's mechanism).
    #[tokio::test]
    async fn col_scoping_full_coverage_collapses_to_all_columns_entry() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let pred_cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);

        let _ = load_scoped_page_index(&store, &loc, &fo, &pred_cols).await.unwrap();
        assert_eq!(oi().entries, 2, "all-columns load caches 2 column cells");
        // Project {1}; union {0,1} = both columns, both already cached → 2 hits.
        let _ = load_scoped_page_index_cols(&store, &loc, &fo, &pred_cols, &[1]).await.unwrap();
        assert_eq!(oi().entries, 2, "covered columns reuse their cells, no new entries");
        assert_eq!(oi().hits, 2);
        clear_scoped_cache_for_test();
    }

    /// The fully-scoped entry point: CI RG-scoped + OI column-scoped together.
    #[tokio::test]
    async fn fully_scoped_load_combines_both_axes() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = four_rg_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["id".to_string()]);

        // CI scoped to RGs {2,3}; OI scoped to {0,1} = all 2 cols (collapses to all).
        let aug = load_scoped_page_index_scoped(&store, &loc, &fo, &cols, &[2, 3], &[1])
            .await
            .unwrap();
        let c = aug.column_index().unwrap();
        assert!(matches!(c[0][0], ColumnIndexMetaData::NONE), "RG0 pruned → NONE CI");
        assert!(!matches!(c[2][0], ColumnIndexMetaData::NONE), "RG2 survivor → real CI");
        // CI cells (id,rg2)+(id,rg3) = 2; OI cells (col0)+(col1) = 2.
        assert_eq!(ci().entries, 2);
        assert_eq!(oi().entries, 2);
        clear_scoped_cache_for_test();
    }
}
