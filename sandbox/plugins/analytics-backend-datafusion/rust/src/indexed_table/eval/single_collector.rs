/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Single-collector evaluator — one backend collector plus DataFusion for
//! residual predicates.
//!
//! When the filter has exactly one `index_filter(...)` call AND'd with
//! (possibly zero, one, or many) parquet-native predicates, this evaluator
//! runs. Per RG:
//!
//! 1. Call the single collector → bitset.
//! 2. Apply page pruning (AND/OR mode depending on how the query combined them).
//! 3. Hand the bitset offsets to `IndexedStream` as a RowSelection.
//! 4. `on_batch_mask` returns `None` — DataFusion's
//!    `with_predicate(residual).with_pushdown_filters(true)` applies the
//!    residual predicates during decode, so indices stay aligned and no
//!    post-filtering is needed.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use native_bridge_common::log_debug;

use super::{PrefetchedRg, PrefetchedRgRows, RowGroupBitsetSource};
use crate::indexed_table::dense_bits::{DenseBitset, DenseBitsetBuilder};
use crate::indexed_table::ffm_callbacks::{create_provider, FfmSegmentCollector, ProviderHandle};
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner, StatsPruneTree};
use crate::indexed_table::row_selection::{packed_bits_to_boolean_array, PositionMap};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use std::time::Instant;

/// Re-exported from parent module for backward compatibility.
pub use super::CollectorCallStrategy;
use crate::indexed_table::stream::RowGroupInfo;

/// TODO(phase-99): hardcoded selectivity threshold for opportunistic peer consultation.
/// Replaced by a cluster setting plumbed through `WireConfigSnapshot` and
/// `DatafusionQueryConfig` in the very last phase, after Phase 7 OR/NOT support and
/// everything else. Until then, performance-delegated leaves consult the peer when DF
/// page-pruning kept more than 5% of an RG.
const HARDCODED_SELECTIVITY_THRESHOLD: f64 = 0.05;

/// Builds delegated-backend collectors for performance-delegated leaves. Production impl
/// wraps `FfmSegmentCollector::create` (Java/Lucene round-trip); fuzz tests inject a
/// mock that replays a pre-computed bitset without an FFM call.
///
/// `context_id` is the per-query identifier passed through to every FFM upcall so Java
/// can route each callback to the correct per-query handle and tracker.
///
/// TODO: extend this factory to also build the *correctness* collector currently passed
/// in pre-built by `indexed_executor.rs`. Today delegated-backend (perf-delegated)
/// collectors are built inside this evaluator while correctness collectors are built
/// upstream — that asymmetry should go once we have more than one delegated backend
/// (DSL, vector, etc.) and the executor wants a single place to plug them in.
pub trait DelegatedBackendCollectorFactory: Send + Sync + std::fmt::Debug {
    fn create(
        &self,
        context_id: i64,
        provider_key: i32,
        writer_generation: i64,
        doc_min: i32,
        doc_max: i32,
    ) -> Result<Arc<dyn RowGroupDocsCollector>, String>;
}

/// Production factory: delegates to `FfmSegmentCollector::create`, which round-trips
/// to Java via FFM to build a Lucene-backed collector.
#[derive(Debug)]
pub struct FfmDelegatedBackendCollectorFactory;

impl DelegatedBackendCollectorFactory for FfmDelegatedBackendCollectorFactory {
    fn create(
        &self,
        context_id: i64,
        provider_key: i32,
        writer_generation: i64,
        doc_min: i32,
        doc_max: i32,
    ) -> Result<Arc<dyn RowGroupDocsCollector>, String> {
        let collector = FfmSegmentCollector::create(
            context_id,
            provider_key,
            writer_generation,
            doc_min,
            doc_max,
        )?;
        Ok(Arc::new(collector) as Arc<dyn RowGroupDocsCollector>)
    }
}

/// Per-RG state the evaluator keeps for refinement. In row-granular
/// mode parquet narrowed fully via `with_predicate` + `RowSelection`
/// and nothing is needed here. In block-granular mode we need the
/// Collector candidate bitset to build a post-decode mask.
struct SingleCollectorState {
    candidates: Arc<DenseBitset>,
}

fn candidate_mask_for_batch(
    candidates: &DenseBitset,
    position_map: &PositionMap,
    batch_offset: usize,
    batch_len: usize,
) -> BooleanArray {
    if matches!(
        position_map,
        PositionMap::Bitmap { .. } | PositionMap::DenseBitmap { .. }
    ) {
        return BooleanArray::new(
            datafusion::arrow::buffer::BooleanBuffer::new_set(batch_len),
            None,
        );
    }

    match position_map {
        // Identity: delivered row i == RG position batch_offset + i.
        // Zero-copy slice over the packed candidate words.
        PositionMap::Identity { .. } => candidates.boolean_slice(batch_offset, batch_len),
        PositionMap::Runs { runs, .. } => {
            let mut out = vec![0u64; batch_len.div_ceil(64)];
            let batch_end = batch_offset.saturating_add(batch_len);
            for &(rg_start, delivered_start, run_len) in runs {
                let delivered_end = delivered_start.saturating_add(run_len);
                let overlap_start = delivered_start.max(batch_offset);
                let overlap_end = delivered_end.min(batch_end);
                if overlap_start >= overlap_end {
                    continue;
                }
                let rg_overlap_start = rg_start + overlap_start - delivered_start;
                // Word-copy the overlapping candidate bits to their local
                // batch positions; no per-bit iteration.
                candidates.copy_bits_into(
                    rg_overlap_start,
                    overlap_end - overlap_start,
                    &mut out,
                    overlap_start - batch_offset,
                );
            }
            packed_bits_to_boolean_array(out, batch_len)
        }
        PositionMap::Bitmap { .. } | PositionMap::DenseBitmap { .. } => {
            unreachable!("handled above")
        }
    }
}

/// Evaluator holding one collector and applying per-RG page pruning.
///
/// Always AND-intersects the collector bitmap with page pruning. The
/// `BitsetMode::Or` branch that previously existed was never emitted by
/// the classifier (reserved for a future `OR(Collector, predicates)`
/// extension) and has been removed; an OR-between-Collector-and-predicates
/// shape routes to the multi-filter tree path today.
pub struct SingleCollectorEvaluator {
    /// Always-call collector for correctness-delegated predicates. `None` when
    /// the query has only performance-delegated leaves (no peer call required
    /// upfront — see `performance_provider_locks`).
    collector: Option<Arc<dyn RowGroupDocsCollector>>,
    page_pruner: Arc<PagePruner>,
    /// Residual pruning predicate: the non-Collector portion of the
    /// top-level AND, translated to a `PruningPredicate`. `None` means
    /// no residual predicate applies (nothing to prune with).
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Raw residual expression (non-Collector children of the top-level
    /// AND, converted to a single `PhysicalExpr`).
    ///
    /// Used in two modes:
    ///
    /// - **Row-granular** (`min_skip_run = 1`): the same expression is
    ///   stashed on `IndexedTableConfig.pushdown_predicate` and handed
    ///   to parquet's `with_predicate` for decode-time filtering.
    ///   Combined with the Collector-bitmap `RowSelection`, parquet
    ///   delivers exact `Collector ∧ residual` rows. `on_batch_mask`
    ///   returns `None` (nothing left to do).
    ///
    /// - **Block-granular** (`min_skip_run > 1`): pushdown is OFF
    ///   (alignment risk with coalesced selection). `on_batch_mask`
    ///   evaluates this expression against the decoded batch and
    ///   AND-combines with the Collector bitmap mask to produce the
    ///   exact result.
    residual_expr: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Counters recorded by `page_pruner.prune_rg`. Built from the
    /// stream's `PartitionMetrics` at evaluator construction.
    page_prune_metrics: Option<PagePruneMetrics>,
    /// Incremented once per `prefetch_rg` call (once per RG) — the
    /// Collector path always performs one FFM round-trip to Java.
    ffm_collector_calls: Option<datafusion::physical_plan::metrics::Count>,
    call_strategy: CollectorCallStrategy,
    /// Lazy `ProviderHandle` cache, one per performance-delegated annotation_id.
    /// Empty when the query has no performance-delegated leaves. Populated by
    /// the factory at query setup; lookups + `OnceLock` init happen ONLY when
    /// `should_consult_lucene` decides DF's own pruning wasn't selective enough
    /// for an RG. Drop releases the Lucene Weight via `releaseProvider`.
    ///
    /// The HashMap is **query-scoped** (shared across all per-(segment×chunk)
    /// evaluators of a single query via `Arc::clone`), so threads racing to fill
    /// a slot do so once per (query × annotation_id) — not per chunk.
    performance_provider_locks: Arc<HashMap<i32, Arc<OnceLock<ProviderHandle>>>>,
    /// Writer generation identifying the segment this evaluator was bound to at
    /// factory time. Captured so `prefetch_rg` can build a per-call
    /// `FfmSegmentCollector` lazily without re-deriving the segment from
    /// `RowGroupInfo` (which doesn't carry it).
    writer_generation: i64,
    /// Builds the per-RG delegated-backend collector when the gate fires. Production
    /// wires `FfmDelegatedBackendCollectorFactory`; fuzz tests inject a mock that
    /// replays a pre-computed bitset without an FFM call.
    delegated_backend_collector_factory: Arc<dyn DelegatedBackendCollectorFactory>,
    /// Per-query context identifier passed through every FFM upcall so Java can route
    /// each callback to the correct per-query `FilterDelegationHandle` and tracker.
    context_id: i64,
    /// Bloom filter pruning config. None = disabled.
    bloom_config: Option<BloomConfig>,
    /// Precomputed per-RG/subtree match status from RG-level column stats.
    stats_prune_tree: Option<Arc<StatsPruneTree>>,
    /// Reverse map: absolute RG index → position in `rg_can_match` vectors.
    rg_index_to_pos: HashMap<usize, usize>,
}

/// Resources needed for per-RG bloom filter pruning.
pub struct BloomConfig {
    pub store: Arc<dyn object_store::ObjectStore>,
    pub object_path: object_store::path::Path,
    pub metadata: Arc<ParquetMetaData>,
    pub arrow_schema: Arc<datafusion::arrow::datatypes::Schema>,
    pub io_handle: tokio::runtime::Handle,
    pub rg_bloom_pruned: Option<datafusion::physical_plan::metrics::Count>,
    pub bloom_filter_eval_time: Option<datafusion::physical_plan::metrics::Time>,
}

impl SingleCollectorEvaluator {
    pub fn new(
        collector: Option<Arc<dyn RowGroupDocsCollector>>,
        page_pruner: Arc<PagePruner>,
        pruning_predicate: Option<Arc<PruningPredicate>>,
        residual_expr: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        page_prune_metrics: Option<PagePruneMetrics>,
        ffm_collector_calls: Option<datafusion::physical_plan::metrics::Count>,
        call_strategy: CollectorCallStrategy,
        performance_provider_locks: Arc<HashMap<i32, Arc<OnceLock<ProviderHandle>>>>,
        writer_generation: i64,
        delegated_backend_collector_factory: Arc<dyn DelegatedBackendCollectorFactory>,
        context_id: i64,
        bloom_config: Option<BloomConfig>,
        stats_prune_tree: Option<Arc<StatsPruneTree>>,
        rg_index_to_pos: HashMap<usize, usize>,
    ) -> Self {
        Self {
            collector,
            page_pruner,
            pruning_predicate,
            residual_expr,
            page_prune_metrics,
            ffm_collector_calls,
            call_strategy,
            performance_provider_locks,
            writer_generation,
            delegated_backend_collector_factory,
            context_id,
            bloom_config,
            stats_prune_tree,
            rg_index_to_pos,
        }
    }
}

/// Per-RG decision: should we consult the peer backend?
///
/// Pure function. Inputs: post-page-prune surviving ranges, RG row count, and the
/// configured selectivity threshold. The function says "consult" when DF kept
/// MORE than `threshold` of the RG (page pruning wasn't selective enough — peer
/// might narrow further); "skip" when DF already squeezed it below threshold
/// (peer call would be wasted work).
///
/// `page_ranges == None` means there's no usable PruningPredicate for the
/// residual at all (e.g. text-column predicate with no parquet stats) — DF
/// can't help, so consult.
fn should_consult_lucene(
    page_ranges: &Option<Vec<(i32, i32)>>,
    rg: &RowGroupInfo,
    threshold: f64,
) -> bool {
    let surviving_rows = match page_ranges {
        None => rg.num_rows as i64,
        Some(ranges) => ranges.iter().map(|(lo, hi)| (hi - lo) as i64).sum::<i64>(),
    };
    if rg.num_rows == 0 {
        return false;
    }
    let surviving_fraction = surviving_rows as f64 / rg.num_rows as f64;
    surviving_fraction > threshold
}

impl RowGroupBitsetSource for SingleCollectorEvaluator {
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let t = Instant::now();

        // RG-level early-exit: precomputed from column stats at construction.
        if let Some(ref spt) = self.stats_prune_tree {
            if let Some(&pos) = self.rg_index_to_pos.get(&rg.index) {
                if let Some(&false) = spt.rg_can_match.get(pos) {
                    native_bridge_common::log_debug!(
                        "SingleCollector: skipping RG {} — pruned by RG-level stats",
                        rg.index
                    );
                    return Ok(None);
                }
            }
        }

        // Page-prune to discover which row ranges survive.
        let page_ranges: Option<Vec<(i32, i32)>> = self.pruning_predicate.as_ref().and_then(|pp| {
            self.page_pruner
                .prune_rg(pp, rg.index, self.page_prune_metrics.as_ref())
                .map(|sel| {
                    let mut ranges = Vec::new();
                    let mut rg_pos: i64 = 0;
                    for s in sel.iter() {
                        if s.skip {
                            rg_pos += s.row_count as i64;
                        } else {
                            let abs_min = min_doc + rg_pos as i32;
                            let abs_max = min_doc + rg_pos as i32 + s.row_count as i32;
                            ranges.push((abs_min, abs_max));
                            rg_pos += s.row_count as i64;
                        }
                    }
                    ranges
                })
        });

        // All pages pruned by stats → skip bloom + collector entirely.
        if let Some(ref ranges) = page_ranges {
            if ranges.is_empty() {
                return Ok(None);
            }
        }

        // Bloom filter pruning: runs after page pruning (free) but before
        // the expensive FFM collector call. Uses the IO runtime handle from
        // the RuntimeManager to drive the async object-store read.
        if let (Some(bloom), Some(pp)) = (&self.bloom_config, &self.pruning_predicate) {
            let _timer = bloom.bloom_filter_eval_time.as_ref().map(|t| t.timer());
            let pruned =
                bloom
                    .io_handle
                    .block_on(crate::indexed_table::bloom_pruner::bloom_prune_rg(
                        &*bloom.store,
                        &bloom.object_path,
                        &bloom.metadata,
                        &bloom.arrow_schema,
                        rg.index,
                        pp.as_ref(),
                    ));
            if pruned {
                if let Some(ref c) = bloom.rg_bloom_pruned {
                    c.add(1);
                }
                return Ok(None);
            }
        }

        // Build candidates either from the always-call correctness collector OR, when
        // the query is performance-only (no Collector leaves), from the page-pruned
        // universe. Performance leaves are AND'd in below if the selectivity gate fires.
        //
        // Candidates are kept as a packed-word DenseBitset: the collector's
        // packed u64 return is OR'd in place and page ranges applied by
        // word-masked fills — no per-bit roaring construction.
        let mut candidates = DenseBitsetBuilder::zeros(rg.num_rows as usize);
        match self.collector.as_ref() {
            Some(collector) => {
                // Dispatch collector call strategy.
                let call_ranges: Vec<(i32, i32)> = match self.call_strategy {
                    CollectorCallStrategy::FullRange => vec![(min_doc, max_doc)],
                    CollectorCallStrategy::TightenOuterBounds => match &page_ranges {
                        Some(r) if r.is_empty() => return Ok(None),
                        Some(r) => vec![(r.first().unwrap().0, r.last().unwrap().1)],
                        None => vec![(min_doc, max_doc)],
                    },
                    CollectorCallStrategy::PageRangeSplit => match &page_ranges {
                        Some(r) if r.is_empty() => return Ok(None),
                        Some(r) => r.clone(),
                        None => vec![(min_doc, max_doc)],
                    },
                };

                // Call collector for each range, OR the packed words in place.
                for (r_min, r_max) in &call_ranges {
                    let bitset = collector
                        .collect_packed_u64_bitset(*r_min, *r_max)
                        .map_err(|e| {
                            format!(
                                "collector.collect_packed_u64_bitset(rg={}, [{}, {})): {}",
                                rg.index, r_min, r_max, e
                            )
                        })?;
                    if let Some(ref c) = self.ffm_collector_calls {
                        c.add(1);
                    }
                    let offset = (*r_min as i64 - rg.first_row) as usize;
                    let num_docs = (*r_max - *r_min) as usize;
                    candidates.or_lsb0_words(offset, &bitset, num_docs);
                }

                // For FullRange and TightenOuterBounds, mask to page ranges
                // to remove rows in dead pages that the collector scanned.
                if self.call_strategy != CollectorCallStrategy::PageRangeSplit {
                    if let Some(ref ranges) = page_ranges {
                        let rel: Vec<(usize, usize)> = ranges
                            .iter()
                            .map(|(r_min, r_max)| {
                                (
                                    (*r_min as i64 - rg.first_row) as usize,
                                    (*r_max as i64 - rg.first_row) as usize,
                                )
                            })
                            .collect();
                        candidates.retain_ranges(&rel);
                    }
                }
            }
            None => {
                // Performance-only query. Seed candidates with the page-pruned universe
                // (or the full RG if no PruningPredicate). The opportunistic peer branch
                // below may narrow further; otherwise DF's pushdown filter handles the
                // residual at decode time.
                match &page_ranges {
                    Some(r) if r.is_empty() => return Ok(None),
                    Some(r) => {
                        for (r_min, r_max) in r {
                            let lo = (*r_min as i64 - rg.first_row) as usize;
                            let hi = (*r_max as i64 - rg.first_row) as usize;
                            candidates.set_range(lo, hi);
                        }
                    }
                    None => {
                        candidates.set_range(0, rg.num_rows as usize);
                    }
                }
            }
        };

        // Opportunistic peer consultation for performance-delegated leaves. Only fires
        // when DF page-pruning kept more than the configured fraction of the RG —
        // skipping the FFM round-trip when DF was already selective. Lazy: lock the
        // map only if the gate fires; create the provider only once per query × leaf.
        // TODO(d3): consult ALL performance leaves whose gate fires and AND their
        // bitsets. Today we consult the first leaf only — sufficient for AND-only
        // single-call demo. Multi-leaf intersection is part of D3 follow-up.
        if !self.performance_provider_locks.is_empty()
            && should_consult_lucene(&page_ranges, rg, HARDCODED_SELECTIVITY_THRESHOLD)
        {
            // Pick the smallest annotation_id deterministically so logs/tests are stable.
            // Avoids the Vec/sort allocation in the common single-leaf case.
            let annotation_id = *self
                .performance_provider_locks
                .keys()
                .min()
                .expect("performance_provider_locks is non-empty (just checked)");
            // Per-RG debug log — `format!` runs unconditionally regardless of log level
            // (the level filter happens on the Java side). Commented out to avoid
            // per-RG allocation. Re-enable locally for debugging.
            // log_debug!(
            //     "[scf-rust] consulting peer for performance leaf rg={} writer_generation={} range=[{},{}) annotation_id={}",
            //     rg.index, self.writer_generation, min_doc, max_doc, annotation_id
            // );
            let lock = self
                .performance_provider_locks
                .get(&annotation_id)
                .expect("annotation_id was just pulled from the map's keys");
            let context_id = self.context_id;
            let mut just_initialized = false;
            let provider = lock.get_or_init(|| {
                just_initialized = true;
                create_provider(context_id, annotation_id)
                    .expect("create_provider FFM upcall failed")
            });
            if just_initialized {
                log_debug!(
                    "[scf-rust] lazy provider initialized context_id={} annotation_id={} provider_key={}",
                    context_id, annotation_id, provider.key()
                );
            }

            let collector = self
                .delegated_backend_collector_factory
                .create(context_id, provider.key(), self.writer_generation, min_doc, max_doc)
                .map_err(|e| {
                    format!(
                        "DelegatedBackendCollectorFactory::create(context_id={}, provider={}, writer_generation={}, doc_range=[{},{})): {}",
                        context_id,
                        provider.key(),
                        self.writer_generation,
                        min_doc,
                        max_doc,
                        e
                    )
                })?;
            let bitset = collector
                .collect_packed_u64_bitset(min_doc, max_doc)
                .map_err(|e| {
                    format!(
                        "delegated-backend collector.collect_packed_u64_bitset(rg={}, [{}, {})): {}",
                        rg.index, min_doc, max_doc, e
                    )
                })?;
            if let Some(ref c) = self.ffm_collector_calls {
                c.add(1);
            }
            let offset = (min_doc as i64 - rg.first_row) as usize;
            let num_docs = (max_doc - min_doc) as usize;
            // AND with the peer bitset; bits outside the peer's window are
            // cleared (the window covers the whole RG range queried).
            candidates.and_lsb0_words(offset, &bitset, num_docs);
        }

        let candidates = candidates.freeze();
        if candidates.is_empty() {
            return Ok(None);
        }

        // Keep one authoritative candidate representation. Row-selection
        // planning and optional residual refinement share it by Arc; packed
        // Arrow masks are materialized only by the block/batch path that
        // consumes them.
        let candidates = Arc::new(candidates);
        let context: Box<dyn std::any::Any + Send + Sync> = if self.residual_expr.is_some() {
            Box::new(SingleCollectorState {
                candidates: Arc::clone(&candidates),
            })
        } else {
            Box::new(())
        };
        Ok(Some(PrefetchedRg {
            rows: PrefetchedRgRows::DenseBitmap(candidates),
            eval_nanos: t.elapsed().as_nanos() as u64,
            context,
        }))
    }

    fn on_batch_mask(
        &self,
        rg_state: &dyn std::any::Any,
        _rg_first_row: i64,
        position_map: &PositionMap,
        batch_offset: usize,
        batch_len: usize,
        batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        // No residual → no post-decode work. Stream's current_mask
        // (if built) handles Collector narrowing.
        let Some(ref residual) = self.residual_expr else {
            return Ok(None);
        };

        let state = rg_state
            .downcast_ref::<SingleCollectorState>()
            .ok_or_else(|| {
                "SingleCollectorEvaluator: rg_state is not SingleCollectorState".to_string()
            })?;

        let collector_mask =
            candidate_mask_for_batch(&state.candidates, position_map, batch_offset, batch_len);

        // Evaluate residual against the batch.
        let residual_mask = super::eval_helpers::evaluate_residual(residual, batch, batch_len)?;

        // AND with kleene semantics (NULL → exclude).
        let combined = datafusion::arrow::compute::kernels::boolean::and_kleene(
            &collector_mask,
            &residual_mask,
        )
        .map_err(|e| format!("SingleCollectorEvaluator: and_kleene: {}", e))?;
        Ok(Some(combined))
    }

    /// With a residual, `on_batch_mask` owns exact filtering (collector
    /// candidates ∧ residual) over the RowSelection-delivered rowset and
    /// looks up RG positions via `PositionMap` in block-granular mode.
    /// A parquet RowFilter would (a) drop rows mid-decode and misalign
    /// those lookups, and (b) evaluate the same residual a second time in
    /// row-granular mode. Forbid it and evaluate the residual exactly once
    /// post-decode — the same ownership rule as the predicate-only direct
    /// selection path. Without a residual there is nothing to push.
    fn forbid_parquet_pushdown(&self) -> bool {
        self.residual_expr.is_some()
    }

    /// With a residual, `on_batch_mask` returns `Some` for every batch and
    /// `finalize_batch` applies that mask exclusively — a stream-built
    /// `current_mask` would be constructed and then ignored (it measured
    /// ~140 ms per non-selective query). Only without a residual does the
    /// stream's `current_mask` perform the collector narrowing.
    fn needs_row_mask(&self) -> bool {
        self.residual_expr.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::arrow_reader::{
        ArrowReaderMetadata, ArrowReaderOptions, RowSelection,
    };
    use datafusion::parquet::arrow::ArrowWriter;
    use std::fmt;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    /// Stub collector: returns a pre-defined set of doc IDs, encoded into
    /// the bitset the trait contract requires.
    #[derive(Debug)]
    struct StubCollector {
        docs: Vec<i32>,
    }

    impl RowGroupDocsCollector for StubCollector {
        fn collect_packed_u64_bitset(
            &self,
            min_doc: i32,
            max_doc: i32,
        ) -> Result<Vec<u64>, String> {
            let span = (max_doc - min_doc) as usize;
            let mut bitset = vec![0u64; (span + 63) / 64];
            for &doc in &self.docs {
                if doc >= min_doc && doc < max_doc {
                    let idx = (doc - min_doc) as usize;
                    bitset[idx / 64] |= 1u64 << (idx % 64);
                }
            }
            Ok(bitset)
        }
    }

    fn minimal_page_pruner() -> Arc<PagePruner> {
        // Build a 1-row-group parquet with no filters — page pruner becomes a no-op
        // (filter_row_ids returns input, candidate_row_ids returns [first_row, first_row+num_rows)).
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(
                vec![0i32; 8],
            ))],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        {
            let mut writer =
                ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let file = tmp.reopen().unwrap();
        let options = ArrowReaderOptions::new().with_page_index(true);
        let meta = ArrowReaderMetadata::load(&file, options).unwrap();
        let pruner = PagePruner::new(meta.schema(), meta.metadata().clone());
        Arc::new(pruner)
    }

    fn mask_values(mask: &BooleanArray) -> Vec<bool> {
        (0..mask.len()).map(|index| mask.value(index)).collect()
    }

    fn dense_from_bits(bits: &[usize], len: usize) -> DenseBitset {
        let mut b = DenseBitsetBuilder::zeros(len);
        for &bit in bits {
            b.or_lsb0_words(bit, &[1u64], 1);
        }
        b.freeze()
    }

    #[test]
    fn candidate_batch_mask_slices_identity_positions() {
        let candidates = dense_from_bits(&[1, 3, 4, 8], 10);
        let position_map = PositionMap::from_selection(&RowSelection::from(vec![
            datafusion::parquet::arrow::arrow_reader::RowSelector::select(10),
        ]));

        let mask = candidate_mask_for_batch(&candidates, &position_map, 2, 4);
        assert_eq!(mask_values(&mask), vec![false, true, true, false]);
    }

    #[test]
    fn candidate_batch_mask_is_true_for_exact_bitmap_selection() {
        let candidates = Arc::new(dense_from_bits(&[1, 3, 8], 10));
        let position_map = PositionMap::DenseBitmap {
            delivered_count: candidates.count_ones(),
            bits: Arc::clone(&candidates),
        };

        let mask = candidate_mask_for_batch(&candidates, &position_map, 1, 2);
        assert_eq!(mask_values(&mask), vec![true, true]);
    }

    #[test]
    fn candidate_batch_mask_maps_across_selection_runs() {
        use datafusion::parquet::arrow::arrow_reader::RowSelector;

        let candidates = dense_from_bits(&[4, 8, 9], 11);
        let position_map = PositionMap::from_selection(&RowSelection::from(vec![
            RowSelector::skip(2),
            RowSelector::select(4),
            RowSelector::skip(2),
            RowSelector::select(3),
        ]));

        let mask = candidate_mask_for_batch(&candidates, &position_map, 2, 4);
        assert_eq!(mask_values(&mask), vec![true, false, true, true]);
    }

    #[test]
    fn path_b_and_mode_collects_docs_and_returns_offsets() {
        let collector = Arc::new(StubCollector {
            docs: vec![0, 3, 7],
        }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            pruner,
            None,
            None,
            None,
            None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            0,
            Arc::new(FfmDelegatedBackendCollectorFactory),
            0,
            None,
            None,
            HashMap::new(),
        );

        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        let got: Vec<u32> = prefetched.rows.to_roaring().unwrap().iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
    }

    #[test]
    fn on_batch_mask_returns_none_for_path_b() {
        let collector = Arc::new(StubCollector { docs: vec![0] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            pruner,
            None,
            None,
            None,
            None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            0,
            Arc::new(FfmDelegatedBackendCollectorFactory),
            0,
            None,
            None,
            HashMap::new(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                1, 2, 3,
            ]))],
        )
        .unwrap();
        // Empty position map is fine; SingleCollectorEvaluator ignores it.
        let pm = PositionMap::from_selection(
            &datafusion::parquet::arrow::arrow_reader::RowSelection::from(Vec::<
                datafusion::parquet::arrow::arrow_reader::RowSelector,
            >::new()),
        );
        assert!(eval
            .on_batch_mask(&(), 0, &pm, 0, 3, &batch)
            .unwrap()
            .is_none());
    }

    #[test]
    fn single_collector_mask_and_pushdown_flags_with_residual() {
        // With a residual, on_batch_mask owns exact filtering: the stream
        // must not build a current_mask (it would be ignored) and parquet
        // must not run the residual as a RowFilter (double evaluation +
        // block-granular misalignment).
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::scalar::ScalarValue;
        let residual: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));
        let collector = Arc::new(StubCollector { docs: vec![0] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            pruner,
            None,
            Some(residual),
            None,
            None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            0,
            Arc::new(FfmDelegatedBackendCollectorFactory),
            0,
            None,
            None,
            HashMap::new(),
        );
        assert!(!eval.needs_row_mask());
        assert!(eval.forbid_parquet_pushdown());
    }

    #[test]
    fn single_collector_needs_row_mask() {
        // Without a residual, on_batch_mask returns None, so IndexedStream
        // must build current_mask from candidate offsets (it's the only
        // post-decode filter on this path) and pushdown stays permitted.
        let collector = Arc::new(StubCollector { docs: vec![0] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            pruner,
            None,
            None,
            None,
            None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            0,
            Arc::new(FfmDelegatedBackendCollectorFactory),
            0,
            None,
            None,
            HashMap::new(),
        );
        assert!(eval.needs_row_mask());
        assert!(!eval.forbid_parquet_pushdown());
    }

    #[test]
    fn empty_match_returns_none() {
        let collector = Arc::new(StubCollector { docs: vec![] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            pruner,
            None,
            None,
            None,
            None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            0,
            Arc::new(FfmDelegatedBackendCollectorFactory),
            0,
            None,
            None,
            HashMap::new(),
        );
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        assert!(eval.prefetch_rg(&rg, 0, 8).unwrap().is_none());
    }

    #[test]
    fn empty_pruning_predicates_leave_collector_unchanged() {
        // With no pruning predicates, the evaluator is a pass-through for
        // the collector bitmap: every doc the collector returns remains a
        // candidate. (Contrast with the old BitsetMode::Or path, which
        // would have unioned with page-pruner-derived "anything-allowed"
        // row IDs — semantics that were never wired up in production.)
        let collector = Arc::new(StubCollector {
            docs: vec![0, 3, 7],
        }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            pruner,
            None,
            None,
            None,
            None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            0,
            Arc::new(FfmDelegatedBackendCollectorFactory),
            0,
            None,
            None,
            HashMap::new(),
        );

        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        let got: Vec<u32> = prefetched.rows.to_roaring().unwrap().iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
    }

    #[test]
    fn stats_prune_tree_skips_rg_when_false() {
        let collector = Arc::new(StubCollector {
            docs: vec![0, 3, 7],
        }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let spt = StatsPruneTree {
            rg_can_match: vec![false],
            children: vec![],
        };
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            pruner,
            None,
            None,
            None,
            None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            0,
            Arc::new(FfmDelegatedBackendCollectorFactory),
            0,
            None,
            Some(Arc::new(spt)),
            HashMap::from([(0, 0)]),
        );
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        assert!(eval.prefetch_rg(&rg, 0, 8).unwrap().is_none());
    }

    #[test]
    fn stats_prune_tree_allows_rg_when_true() {
        let collector = Arc::new(StubCollector {
            docs: vec![0, 3, 7],
        }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let spt = StatsPruneTree {
            rg_can_match: vec![true],
            children: vec![],
        };
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            pruner,
            None,
            None,
            None,
            None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            0,
            Arc::new(FfmDelegatedBackendCollectorFactory),
            0,
            None,
            Some(Arc::new(spt)),
            HashMap::from([(0, 0)]),
        );
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval
            .prefetch_rg(&rg, 0, 8)
            .unwrap()
            .expect("should have matches");
        let got: Vec<u32> = prefetched.rows.to_roaring().unwrap().iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
    }

    #[test]
    fn stats_prune_tree_none_does_not_prune() {
        let collector =
            Arc::new(StubCollector { docs: vec![1, 5] }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            pruner,
            None,
            None,
            None,
            None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            0,
            Arc::new(FfmDelegatedBackendCollectorFactory),
            0,
            None,
            None,
            HashMap::new(),
        );
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval
            .prefetch_rg(&rg, 0, 8)
            .unwrap()
            .expect("should have matches");
        let got: Vec<u32> = prefetched.rows.to_roaring().unwrap().iter().collect();
        assert_eq!(got, vec![1u32, 5]);
    }

    // Keep the `fmt` import used
    #[allow(dead_code)]
    fn _use(_: &dyn fmt::Debug) {}
}
