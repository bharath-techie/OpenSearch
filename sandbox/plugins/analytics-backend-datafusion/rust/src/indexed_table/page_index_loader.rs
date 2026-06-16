/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unified, scoped parquet page-index cache (Step 1a).
//!
//! # Why this exists
//!
//! Parquet metadata loading pulls the **entire page index** — `ColumnIndex`
//! (per-page min/max; the per-page *string* min/max is the heap hog) plus
//! `OffsetIndex` (per-page byte offsets), for every column of every row group.
//! On wide schemas (the production `textbench` index has 402 columns) this is
//! ~82% of native heap and is re-decoded per query, even when the query filters
//! a single column.
//!
//! This module loads the page index **scoped to the query's predicate columns**
//! and caches the result in **one** process-global cache that is shared by both
//! scan paths (the DataFusion `ListingTable` path and the custom indexed-table
//! executor). For the same `(file, predicate columns)` an entry built by one
//! path is reusable by the other.
//!
//! ## What is scoped, and why each scope is safe
//!
//! - **`ColumnIndex` → predicate columns only.** It is read only at *prune*
//!   time, and the pruner (`StatisticsConverter`, and our [`super::page_pruner`])
//!   only ever dereferences predicate-column positions. Non-predicate positions
//!   carry [`ColumnIndexMetaData::NONE`] placeholders, which keep absolute
//!   `index[rg][col]` indexing valid and cost nothing. This scoping is where the
//!   heap win comes from.
//! - **`OffsetIndex` → all columns (this step also: all row groups).** Unlike
//!   the `ColumnIndex`, the `OffsetIndex` is read at *scan* time: when a
//!   `RowSelection` is active, arrow-rs's `InMemoryRowGroup::fetch_ranges`
//!   dereferences `offset_index[col].page_locations()` for every **projected**
//!   column (by absolute index) to compute which page byte ranges to fetch. A
//!   placeholder there for a projected column fetches zero ranges and the read
//!   fails ("failed to skip rows, expected N, got 0"). Because this loader runs
//!   before the projection is known, it keeps a real `OffsetIndex` for every
//!   column. That is cheap relative to the `ColumnIndex`: fixed-width page
//!   offsets/sizes, no per-page string stats.
//!
//! ## Why the cache stores the (ColumnIndex, OffsetIndex) PAIR, not metadata
//!
//! `ParquetMetaData` owns its `row_groups: Vec<RowGroupMetaData>` (~5–6 MB on
//! the 402-column schema). Caching a full augmented `ParquetMetaData` per entry
//! would **duplicate that footer** in every entry (the prior iteration's
//! over-allocation bug: a row-group-pruned entry that read 0 row groups still
//! cost a 60 MB footer clone). Instead this cache stores only the decoded
//! `(ParquetColumnIndex, ParquetOffsetIndex)` pair plus a size estimate, and
//! **grafts** the pair onto the caller's already-resident footer at lookup via
//! [`ParquetMetaData::into_builder`] → `set_column_index`/`set_offset_index`.
//! The footer is never duplicated into the cache; the only footer copy is the
//! one transient graft handed to the scan (one deep clone per file per query,
//! the same order as a cold metadata fetch).
//!
//! **Consequence for tests:** a cache hit returns a *fresh* `Arc`, so
//! `Arc::ptr_eq` is the wrong signal for "served from cache" — assert via the
//! [`ScopedCacheStats::hits`] counter instead.
//!
//! ## Key
//!
//! The cache key is `(file path, predicate parquet-column indices)` **only**.
//! Both scan paths resolve predicate columns the same way
//! ([`resolve_predicate_parquet_columns`]) and build the identical artifact
//! (all-RG, all-column `OffsetIndex`; predicate-scoped `ColumnIndex`), so an
//! entry is shared across paths. Row-group scoping is a *future* axis that would
//! extend the key — deliberately omitted here so Step 1 keeps one unified key.
//!
//! ## Correctness / fallback
//!
//! Any failure (file has no page index, a predicate column lacks an index range,
//! a decode/IO error) makes the load return `None`. The caller keeps its
//! footer-only metadata and the pruner conservatively no-ops (scans the whole
//! row group) — never a wrong result.
//!
//! ## Upstream note
//!
//! arrow-rs is moving toward first-class selective metadata decoding: a unified
//! options mechanism that would include "decode column indexes only for
//! predicate columns" and "row-group selection" (apache/arrow-rs#8643, open),
//! and the `ParquetMetaDataOptions` / `ParquetStatisticsPolicy::skip_except`
//! pattern just merged for page *encoding* statistics (apache/arrow-rs#8797,
//! built on the selective-decode metadata "index" of #8714). None of these yet
//! expose a page-index column/row-group projection. Until one does, the only
//! **public** API that decodes a *subset* of columns is the deprecated
//! [`read_columns_indexes`]/[`read_offset_indexes`] (the per-column primitives
//! are `pub(crate)`; `ParquetMetaDataReader`/`PageIndexPolicy` is
//! all-columns-or-nothing). This module is the interim hand-rolled equivalent;
//! when a `ParquetMetaDataOptions` page-index projection lands, migrate
//! [`build_scoped_page_index`] to it and drop the `#[allow(deprecated)]`.

use std::collections::HashMap;
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

/// Default byte budget for the scoped page-index cache, used until the caller
/// sets one from the runtime's configured limit (see [`set_scoped_cache_limit`]).
/// 64 MiB is generous for a predicate-column-only page index yet a tiny fraction
/// of the footer-only baseline the page-index strip buys back.
const DEFAULT_SCOPED_CACHE_LIMIT: usize = 64 * 1024 * 1024;

// ── Cache types ──────────────────────────────────────────────────────────

/// Cache key: object-store path + the sorted/deduped set of parquet column
/// indices the page index was built for. Row-group scoping is intentionally
/// NOT part of the key in Step 1 — both paths build an all-row-group artifact so
/// the key (and the artifact) is identical across paths.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ScopedKey {
    path: String,
    parquet_cols: Vec<usize>,
}

/// One cached entry: the decoded page-index pair (full-width over all row groups
/// and all columns; `ColumnIndex` real only at predicate positions), a size
/// estimate, and a last-used tick for LRU ordering. Deliberately does NOT hold a
/// `ParquetMetaData` — see the module docs (no footer duplication).
struct ScopedEntry {
    column_index: ParquetColumnIndex,
    offset_index: ParquetOffsetIndex,
    size: usize,
    last_used: u64,
}

/// Snapshot of scoped page-index cache counters. Surfaced on node-stats and used
/// by tests to assert hits/misses without relying on `Arc::ptr_eq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScopedCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub used_bytes: usize,
    pub limit_bytes: usize,
}

/// Byte-bounded LRU over scoped page-index pairs, keyed by
/// `(file, predicate-column-set)`.
///
/// Evicts the least-recently-used entries once `used > limit`, so it always
/// serves cached data within a memory budget rather than silently degrading to
/// "decode every query" when full. Entry size is a deterministic structural
/// estimate (see [`scoped_page_index_size`]).
struct ScopedLru {
    map: HashMap<ScopedKey, ScopedEntry>,
    used: usize,
    limit: usize,
    /// Monotonic clock for LRU ordering (avoids `Instant`; fine single-process).
    tick: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl ScopedLru {
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

    /// On hit, bump recency and return a clone of the pair (cheap relative to a
    /// footer: predicate-scoped `ColumnIndex` + fixed-width `OffsetIndex`).
    fn get(&mut self, key: &ScopedKey) -> Option<(ParquetColumnIndex, ParquetOffsetIndex)> {
        let t = self.next_tick();
        match self.map.get_mut(key) {
            Some(entry) => {
                entry.last_used = t;
                self.hits += 1;
                Some((entry.column_index.clone(), entry.offset_index.clone()))
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    fn insert(
        &mut self,
        key: ScopedKey,
        column_index: ParquetColumnIndex,
        offset_index: ParquetOffsetIndex,
        size: usize,
    ) {
        // An entry larger than the whole budget can never be retained; skip it
        // rather than evicting everything else for something we'd drop anyway.
        if size > self.limit {
            return;
        }
        let t = self.next_tick();
        if let Some(old) = self.map.insert(
            key,
            ScopedEntry {
                column_index,
                offset_index,
                size,
                last_used: t,
            },
        ) {
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
}

/// Process-wide scoped page-index cache.
static SCOPED_CACHE: Lazy<Mutex<ScopedLru>> =
    Lazy::new(|| Mutex::new(ScopedLru::new(DEFAULT_SCOPED_CACHE_LIMIT)));

/// Set the scoped cache's byte budget. Called from startup wiring with the
/// configured limit. Idempotent; shrinking evicts immediately. Zero is ignored
/// (keeps the existing budget).
pub fn set_scoped_cache_limit(limit: usize) {
    if limit == 0 {
        return;
    }
    if let Ok(mut c) = SCOPED_CACHE.lock() {
        c.set_limit(limit);
    }
}

/// Snapshot of the scoped page-index cache counters plus occupancy. For
/// node-stats / observability and tests.
pub fn scoped_cache_stats() -> ScopedCacheStats {
    SCOPED_CACHE.lock().map(|c| c.stats()).unwrap_or_default()
}

// ── Public API ─────────────────────────────────────────────────────────────

/// Map the query's arrow predicate-column names to this file's parquet column
/// indices, using the same resolution the pruner uses
/// (`StatisticsConverter::parquet_column_index`). Columns absent from the
/// parquet file (schema evolution) are skipped. Returns a sorted, deduped set so
/// both scan paths produce an identical key for the same logical predicate.
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

/// Load (or build + cache) the scoped page index for `parquet_cols` and graft it
/// onto `footer_meta`, returning fresh metadata that carries:
///   - a real `ColumnIndex` at the predicate-column positions (NONE elsewhere),
///   - a real `OffsetIndex` for every column, across all row groups.
///
/// Returns `None` (caller keeps footer-only metadata) on any condition that
/// would make page pruning unsafe or impossible: empty column set, a file
/// without a page index, an out-of-range predicate column, or a decode/IO error.
///
/// Consults and populates the shared `(file, predicate-cols)` cache.
pub async fn load_scoped_page_index(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    parquet_cols: &[usize],
) -> Option<Arc<ParquetMetaData>> {
    if parquet_cols.is_empty() {
        return None;
    }
    let key = ScopedKey {
        path: location.as_ref().to_string(),
        parquet_cols: parquet_cols.to_vec(),
    };

    // Cache hit: graft the cached pair onto the caller's footer. No I/O, no
    // decode. (Returns a fresh Arc — assert hits via the counter, not ptr_eq.)
    if let Ok(mut cache) = SCOPED_CACHE.lock() {
        if let Some((ci, oi)) = cache.get(&key) {
            return Some(graft(footer_meta, ci, oi));
        }
    }

    // Miss: range-read + decode the scoped page index.
    let (column_index, offset_index, size) =
        build_scoped_page_index(store, location, footer_meta, parquet_cols).await?;

    // Graft a copy for the caller; store the originals in the cache.
    let grafted = graft(footer_meta, column_index.clone(), offset_index.clone());
    if let Ok(mut cache) = SCOPED_CACHE.lock() {
        cache.insert(key, column_index, offset_index, size);
    }
    Some(grafted)
}

/// Build a fresh `ParquetMetaData` = `footer` with the scoped page-index pair
/// grafted on. This deep-clones the footer (the builder consumes an owned
/// `ParquetMetaData`); that transient clone is the only footer copy and is never
/// stored in the cache.
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

// ── Build ────────────────────────────────────────────────────────────────

/// Range-read and decode the page index scoped to `parquet_cols`, returning the
/// full-width `(ColumnIndex, OffsetIndex)` pair plus a size estimate. All row
/// groups, all columns for the `OffsetIndex`; predicate columns only (NONE
/// elsewhere) for the `ColumnIndex`. `None` on any unsafe/impossible condition.
async fn build_scoped_page_index(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    footer_meta: &Arc<ParquetMetaData>,
    parquet_cols: &[usize],
) -> Option<(ParquetColumnIndex, ParquetOffsetIndex, usize)> {
    let num_rgs = footer_meta.num_row_groups();
    if num_rgs == 0 {
        return None;
    }
    let num_cols = footer_meta.file_metadata().schema_descr().num_columns();
    // Predicate indices index into the file schema (shared across RGs).
    if parquet_cols.iter().any(|&i| i >= num_cols) {
        return None;
    }

    // Phase 1: per RG, gather the predicate columns' ColumnIndex chunks and ALL
    // columns' OffsetIndex chunks, and compute their union byte ranges for a
    // single vectored fetch. Bail to footer-only if any required index range is
    // missing (the file has no page index for it).
    struct RgPlan {
        rg_idx: usize,
        pred_chunks: Vec<ColumnChunkMetaData>,
        all_chunks: Vec<ColumnChunkMetaData>,
        col_range: Range<u64>,
        off_range: Range<u64>,
    }
    let mut plans: Vec<RgPlan> = Vec::with_capacity(num_rgs);
    let mut fetch_ranges: Vec<Range<u64>> = Vec::with_capacity(num_rgs * 2);
    for rg_idx in 0..num_rgs {
        let rg = footer_meta.row_group(rg_idx);
        let pred_chunks: Vec<ColumnChunkMetaData> =
            parquet_cols.iter().map(|&i| rg.column(i).clone()).collect();
        let all_chunks: Vec<ColumnChunkMetaData> =
            (0..num_cols).map(|i| rg.column(i).clone()).collect();
        let col_range = column_index_union(&pred_chunks)?;
        let off_range = offset_index_union(&all_chunks)?;
        fetch_ranges.push(col_range.clone());
        fetch_ranges.push(off_range.clone());
        plans.push(RgPlan {
            rg_idx,
            pred_chunks,
            all_chunks,
            col_range,
            off_range,
        });
    }
    if plans.is_empty() {
        return None;
    }

    // Phase 2: one vectored fetch of all RGs' index byte ranges.
    let buffers = store.get_ranges(location, &fetch_ranges).await.ok()?;
    if buffers.len() != fetch_ranges.len() {
        return None;
    }

    // Phase 3: decode and scatter into full-width per-RG vectors. Pre-fill with
    // placeholders so absolute `index[rg][col]` indexing is always valid.
    let mut column_index: ParquetColumnIndex = (0..num_rgs)
        .map(|_| (0..num_cols).map(|_| ColumnIndexMetaData::NONE).collect())
        .collect();
    let mut offset_index: ParquetOffsetIndex = (0..num_rgs)
        .map(|_| {
            (0..num_cols)
                .map(|_| OffsetIndexBuilder::new().build())
                .collect()
        })
        .collect();

    for (i, plan) in plans.iter().enumerate() {
        let col_buf = buffers[i * 2].clone();
        let off_buf = buffers[i * 2 + 1].clone();

        let col_reader = BufferChunkReader {
            base: plan.col_range.start,
            bytes: col_buf,
        };
        let off_reader = BufferChunkReader {
            base: plan.off_range.start,
            bytes: off_buf,
        };

        // `read_columns_indexes` / `read_offset_indexes` are deprecated in
        // arrow-rs but are the only PUBLIC API that decodes a *column subset*.
        // See module docs + apache/arrow-rs#8643.
        #[allow(deprecated)]
        let decoded_cols = read_columns_indexes(&col_reader, &plan.pred_chunks).ok()??;
        #[allow(deprecated)]
        let decoded_offs = read_offset_indexes(&off_reader, &plan.all_chunks).ok()??;
        if decoded_cols.len() != parquet_cols.len() || decoded_offs.len() != num_cols {
            return None;
        }

        let col_row = &mut column_index[plan.rg_idx];
        for (k, &parquet_col) in parquet_cols.iter().enumerate() {
            col_row[parquet_col] = decoded_cols[k].clone();
        }
        offset_index[plan.rg_idx] = decoded_offs;
    }

    let size = scoped_page_index_size(footer_meta, parquet_cols, num_cols);
    Some((column_index, offset_index, size))
}

/// Deterministic size estimate for one cached pair, in bytes.
///
/// Uses the **on-disk serialized lengths** from the footer
/// (`column_index_length` for predicate columns + `offset_index_length` for all
/// columns, summed over all row groups). This is a robust, monotonic proxy for
/// the decoded heap: it is never zero for a real page index, grows with the
/// number of predicate columns / row groups / pages, and — unlike
/// `ParquetMetaData::memory_size()` (which includes the footer we deliberately
/// don't store and rounds to ~0 on small files) — does not depend on private
/// `HeapSize` internals or on arrow-rs decoded-struct layout. The decoded form
/// is a small constant multiple of this; the configured limit is interpreted in
/// these units.
fn scoped_page_index_size(
    footer_meta: &ParquetMetaData,
    parquet_cols: &[usize],
    num_cols: usize,
) -> usize {
    let mut total = 0usize;
    for rg in footer_meta.row_groups() {
        for &pc in parquet_cols {
            total += rg.column(pc).column_index_length().unwrap_or(0).max(0) as usize;
        }
        for c in 0..num_cols {
            total += rg.column(c).offset_index_length().unwrap_or(0).max(0) as usize;
        }
    }
    total
}

/// Union of `column_index` byte ranges across the given column chunks. `None` if
/// any chunk lacks a column index (we require all predicate columns to have one,
/// else we fall back to footer-only).
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
/// `get_bytes(absolute_offset, len)`; we translate the absolute file offset into
/// the buffer.
struct BufferChunkReader {
    /// Absolute file offset of `bytes[0]`.
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
    /// Translate an absolute file offset (and a length to validate) into a
    /// buffer-relative start, erroring if it falls outside the prefetched span.
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

// ── Test-only helpers (the cache is process-global; see SCOPED_CACHE_TEST_GUARD) ──

/// Crate-wide guard so every test that touches the global scoped cache mutually
/// excludes — distinct fixtures alone aren't enough because the `InMemory` path
/// is always "data.parquet". Shared (not per-module) so future cache users
/// (`shard_scoped_reader`, the optimizer) serialize against the same lock.
#[cfg(test)]
pub(crate) static SCOPED_CACHE_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Reset entries + counters AND restore the default limit.
#[cfg(test)]
pub(crate) fn clear_scoped_cache_for_test() {
    if let Ok(mut c) = SCOPED_CACHE.lock() {
        c.map.clear();
        c.used = 0;
        c.tick = 0;
        c.limit = DEFAULT_SCOPED_CACHE_LIMIT;
        c.hits = 0;
        c.misses = 0;
        c.evictions = 0;
    }
}

#[cfg(test)]
pub(crate) fn scoped_cache_len_for_test() -> usize {
    SCOPED_CACHE.lock().map(|c| c.map.len()).unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn scoped_cache_bytes_for_test() -> usize {
    SCOPED_CACHE.lock().map(|c| c.used).unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn set_scoped_cache_limit_for_test(limit: usize) {
    if let Ok(mut c) = SCOPED_CACHE.lock() {
        c.set_limit(limit);
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

    // Aliased so every cache-touching test serializes on one lock.
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
            vec![
                Arc::new(Int32Array::from(prices)),
                Arc::new(Int32Array::from(qtys)),
            ],
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

    async fn stage(bytes: Bytes) -> (Arc<dyn ObjectStore>, ObjPath) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = ObjPath::from("data.parquet");
        store.put(&loc, PutPayload::from_bytes(bytes)).await.unwrap();
        (store, loc)
    }

    fn footer_only(bytes: &Bytes) -> Arc<ParquetMetaData> {
        ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(false),
        )
        .unwrap()
        .metadata()
        .clone()
    }

    fn full_index(bytes: &Bytes) -> Arc<ParquetMetaData> {
        ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(true),
        )
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

    /// Read the rows kept by `selection` for one projected leaf column — drives
    /// the offset-index read path so we can prove non-predicate projected reads
    /// succeed with scoped metadata.
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

    // ── tests ────────────────────────────────────────────────────────────

    /// Footer-only load carries no page index — the baseline the cache restores.
    #[tokio::test]
    async fn footer_only_has_no_page_index() {
        let (bytes, _schema) = two_col_parquet();
        let fo = footer_only(&bytes);
        assert!(fo.column_index().is_none());
        assert!(fo.offset_index().is_none());
    }

    /// Empty predicate-column set never builds an entry.
    #[tokio::test]
    async fn empty_column_set_returns_none() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, _schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        assert!(load_scoped_page_index(&store, &loc, &fo, &[]).await.is_none());
        assert_eq!(scoped_cache_len_for_test(), 0);
    }

    /// The grafted metadata has a real ColumnIndex only at predicate positions
    /// (NONE elsewhere) and a real OffsetIndex for every column.
    #[tokio::test]
    async fn scoped_index_is_predicate_scoped_for_column_index() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);

        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        assert_eq!(cols, vec![0]);

        let aug = load_scoped_page_index(&store, &loc, &fo, &cols)
            .await
            .expect("augmentation must succeed");
        let ci = aug.column_index().expect("has column index");
        let oi = aug.offset_index().expect("has offset index");

        assert!(
            !matches!(ci[0][0], ColumnIndexMetaData::NONE),
            "predicate col (price) must have a real ColumnIndex"
        );
        assert!(
            matches!(ci[0][1], ColumnIndexMetaData::NONE),
            "non-predicate col (qty) ColumnIndex must be a NONE placeholder"
        );
        assert!(
            !oi[0][0].page_locations().is_empty() && !oi[0][1].page_locations().is_empty(),
            "OffsetIndex must be real for every column"
        );
    }

    /// Scoped pruning must match full-index pruning on the predicate column.
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

        // price pages: 0..8,8..16,16..24,24..32; `price >= 20` keeps the last two.
        let pp = build_pruning_predicate(&pred("price", 0, Operator::GtEq, 20), schema.clone())
            .unwrap();
        let s = PagePruner::new(&schema, Arc::clone(&aug)).prune_rg(&pp, 0, None);
        let f = PagePruner::new(&schema, full).prune_rg(&pp, 0, None);
        assert_eq!(s.as_ref().map(kept), f.as_ref().map(kept));
        assert_eq!(s.as_ref().map(kept), Some(16));
    }

    /// A non-predicate but PROJECTED column must read correctly through the
    /// scoped metadata — its OffsetIndex is real, so the offset-index read path
    /// fetches the right page ranges (regression for "failed to skip rows").
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
        // Project the NON-predicate column (qty = leaf 1).
        let scoped_vals = read_selected_column(&bytes, &aug, 1, selection.clone())
            .expect("non-predicate projected read must succeed with scoped metadata");
        let full = full_index(&bytes);
        let full_vals = read_selected_column(&bytes, &full, 1, selection).unwrap();

        let expected: Vec<i32> = (116..132).collect();
        assert_eq!(scoped_vals, expected);
        assert_eq!(scoped_vals, full_vals);
    }

    /// First load of a (file, column-set) is a miss; the next identical load is a
    /// HIT served from cache — asserted via the counter (graft returns a fresh
    /// Arc, so ptr_eq would be wrong), with no new entry and no byte growth.
    #[tokio::test]
    async fn second_load_is_cache_hit() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let cols = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);

        let _first = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        let s1 = scoped_cache_stats();
        assert_eq!((s1.hits, s1.misses, s1.entries), (0, 1, 1));
        let bytes_after_first = s1.used_bytes;
        assert!(bytes_after_first > 0, "a real page index must cost > 0 bytes");

        let _second = load_scoped_page_index(&store, &loc, &fo, &cols).await.unwrap();
        let s2 = scoped_cache_stats();
        assert_eq!(
            (s2.hits, s2.misses, s2.entries, s2.used_bytes),
            (1, 1, 1, bytes_after_first),
            "second identical load: pure hit, no new entry, no byte growth"
        );
    }

    /// Distinct predicate-column sets are cached independently.
    #[tokio::test]
    async fn distinct_column_sets_cached_independently() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);

        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);
        assert_eq!(c_price, vec![0]);
        assert_eq!(c_qty, vec![1]);

        let a_price = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let _a_qty = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        assert_eq!(scoped_cache_len_for_test(), 2);

        // qty-scoped index prunes a qty predicate; price-scoped prunes a price one.
        let pp_qty =
            build_pruning_predicate(&pred("qty", 1, Operator::GtEq, 120), schema.clone()).unwrap();
        // The qty entry must be usable; reload (hit) then prune.
        let a_qty2 = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        let sel_qty = PagePruner::new(&schema, a_qty2).prune_rg(&pp_qty, 0, None).unwrap();
        assert_eq!(kept(&sel_qty), 16);

        let pp_price =
            build_pruning_predicate(&pred("price", 0, Operator::GtEq, 20), schema.clone()).unwrap();
        let sel_price = PagePruner::new(&schema, a_price).prune_rg(&pp_price, 0, None).unwrap();
        assert_eq!(kept(&sel_price), 16);
    }

    /// Hit/miss accounting across two distinct column-sets.
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
        assert_eq!(
            (scoped_cache_stats().hits, scoped_cache_stats().misses),
            (0, 1)
        );
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        assert_eq!(
            (scoped_cache_stats().hits, scoped_cache_stats().misses),
            (1, 1)
        );
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        assert_eq!(
            (scoped_cache_stats().hits, scoped_cache_stats().misses),
            (1, 2)
        );
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        let s = scoped_cache_stats();
        assert_eq!((s.hits, s.misses, s.entries, s.evictions), (3, 2, 2, 0));
    }

    /// Byte-bounded LRU evicts the least-recently-used over budget, never
    /// exceeding the limit, and never degrades to "cache nothing".
    #[tokio::test]
    async fn lru_evicts_over_byte_budget() {
        let _g = CACHE_TEST_GUARD.lock().unwrap();
        clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes.clone()).await;
        let fo = footer_only(&bytes);
        let c_price = resolve_predicate_parquet_columns(&schema, &fo, &["price".to_string()]);
        let c_qty = resolve_predicate_parquet_columns(&schema, &fo, &["qty".to_string()]);
        let c_both = resolve_predicate_parquet_columns(
            &schema,
            &fo,
            &["price".to_string(), "qty".to_string()],
        );

        // Size one entry, then set the budget to ~1.5 entries so only one fits.
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let one_size = scoped_cache_bytes_for_test();
        assert!(one_size > 0);
        let budget = one_size + one_size / 2;
        // Clear, set the budget, and fill from empty.
        clear_scoped_cache_for_test();
        set_scoped_cache_limit_for_test(budget);

        let _ = load_scoped_page_index(&store, &loc, &fo, &c_price).await.unwrap();
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_qty).await.unwrap();
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_both).await.unwrap();

        assert!(
            scoped_cache_bytes_for_test() <= budget,
            "cache bytes {} must stay within budget {}",
            scoped_cache_bytes_for_test(),
            budget
        );
        assert!(
            scoped_cache_len_for_test() >= 1,
            "LRU must retain at least the most-recent entry"
        );
        assert!(scoped_cache_stats().evictions >= 1, "something must have evicted");

        // The most-recently-used (c_both) must still be a hit.
        let hits_before = scoped_cache_stats().hits;
        let _ = load_scoped_page_index(&store, &loc, &fo, &c_both).await.unwrap();
        assert_eq!(
            scoped_cache_stats().hits,
            hits_before + 1,
            "most-recently-used entry must remain cached"
        );

        clear_scoped_cache_for_test();
    }
}
