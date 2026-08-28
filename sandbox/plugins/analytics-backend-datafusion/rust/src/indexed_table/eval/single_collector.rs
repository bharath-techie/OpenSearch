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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::OnceLock;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use native_bridge_common::log_debug;
use roaring::RoaringBitmap;

use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::ffm_callbacks::{create_provider, FfmSegmentCollector, ProviderHandle};
use crate::indexed_table::index::{CollectDocsResult, RowGroupDocsCollector};
use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner, StatsPruneTree};
use crate::indexed_table::row_selection::{
    bitmap_to_packed_bits, packed_bits_to_boolean_array, row_selection_to_bitmap, PositionMap,
};
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

/// Full-schema column indices referenced by an expression. These indices are
/// carried into the per-RG prefetch result so the stream can construct the
/// Parquet projection AFTER the DelegationPossible XOR decision.
fn expr_column_indices(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
) -> Vec<usize> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::physical_expr::expressions::Column;

    let mut indices = HashSet::new();
    let _ = expr.apply(|node| {
        if let Some(column) = node.downcast_ref::<Column>() {
            indices.insert(column.index());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    let mut indices: Vec<usize> = indices.into_iter().collect();
    indices.sort_unstable();
    indices
}

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

/// A performance-delegated (dual-viable) leaf: a predicate that BOTH DataFusion (via its
/// original `expr` over doc values) AND a peer backend (Lucene, via `annotation_id`) can
/// evaluate with identical semantics. Per row group the evaluator makes an EXCLUSIVE choice
/// between the two evaluators (never both) using sound stats:
///
/// - if DataFusion's own page-stat pruning of `expr` already narrows the RG below the
///   selectivity threshold, DataFusion is authoritative — `expr` is applied post-decode and
///   the peer is NOT consulted;
/// - otherwise (weak/absent stats) the peer is authoritative — its bitmap is intersected into
///   the candidates and `expr` is NOT applied for that leaf.
///
/// `pruning_predicate` is the `PruningPredicate` compiled from `expr` (or `None` when the
/// column has no usable parquet stats — treated as "consult the peer").
#[derive(Clone)]
pub struct PerformanceLeaf {
    pub annotation_id: i32,
    pub expr: Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    pub pruning_predicate: Option<Arc<PruningPredicate>>,
}

/// Per-RG state the evaluator keeps for refinement. In row-granular
/// mode parquet narrowed fully via `with_predicate` + `RowSelection`
/// and nothing is needed here. In block-granular mode we need the
/// Collector candidate bitmap to build a post-decode mask.
///
/// `mask_buffer` is the candidate bitmap in Arrow's native LSB-first bit
/// layout, wrapped as a refcounted `Buffer`. Sharing an `Arc<Buffer>` lets
/// `on_batch_mask` and `build_mask` build zero-copy `BooleanBuffer`
/// views via `BooleanBuffer::new(buf.clone(), bit_offset, bit_len)`.
/// Length of the underlying buffer covers `mask_len` bits (= rg_num_rows).
struct SingleCollectorState {
    candidates: RoaringBitmap,
    mask_buffer: datafusion::arrow::buffer::Buffer,
    mask_len: usize,
    /// Per-RG residual for the DataFusion-selected performance leaves (their `expr`s AND'd
    /// together), or `None` when no performance leaf chose DataFusion for this RG. Applied
    /// post-decode in `on_batch_mask` on top of the always-native residual. Performance leaves
    /// that chose Lucene for this RG are already reflected in `candidates` (peer bitmap
    /// intersection) and are deliberately absent here — enforcing the per-leaf XOR.
    perf_residual: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Always-native residual to apply post-decode for THIS RG. Equals the
    /// evaluator's `residual_expr` for normal RGs, or the sort-range-stripped
    /// variant for relaxed timestamp-WITHIN RGs (so the sort column, dropped
    /// from this RG's projection, is never referenced by `remap_expr_to_batch`).
    always_residual: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
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
    /// Next matching docId from the last collectDocs call. When next_doc >= rg.max_doc,
    /// the RG can be skipped without an FFM call. Initialized to i32::MIN (no skip info).
    last_next_doc: std::sync::atomic::AtomicI32,
    /// Chunk-scoped delegated collector reused across `count_docs_range`
    /// calls, so successive per-RG counts ride ONE forward Lucene cursor
    /// (one postings pass per chunk) instead of a fresh scorer per RG.
    /// Only valid for non-decreasing, non-overlapping ranges — exactly the
    /// order `IndexReader` iterates row groups. Requires `chunk_doc_bounds`.
    count_collector: OnceLock<Arc<dyn RowGroupDocsCollector>>,
    /// Doc-id bounds `[doc_min, doc_max)` of the chunk this evaluator was
    /// built for. Set by the production factory via
    /// [`with_chunk_doc_bounds`](Self::with_chunk_doc_bounds); `None` (tests,
    /// older callers) falls back to a per-call collector for counts.
    chunk_doc_bounds: Option<(i32, i32)>,
    /// Performance-delegated (dual-viable) leaves. For each leaf, per RG, the evaluator makes an
    /// EXCLUSIVE DataFusion-XOR-Lucene choice (see [`PerformanceLeaf`]). The `expr`s here are
    /// deliberately NOT part of `residual_expr`/`pruning_predicate` (which carry ONLY the
    /// always-applied native predicates) and are NEVER statically pushed to parquet — so a per-RG
    /// Lucene choice can make a leaf authoritative without DataFusion also evaluating it. Empty for
    /// correctness-only queries and for the fuzz harness (which models perf leaves inside its own
    /// residual with no peer available).
    performance_leaves: Vec<PerformanceLeaf>,
    /// Columns needed by the always-native residual on every RG.
    always_residual_columns: Vec<usize>,
    /// Columns needed only when a performance leaf selects DataFusion for an RG.
    performance_leaf_columns: HashMap<i32, Vec<usize>>,
    /// Relaxed timestamp-WITHIN residual: `residual_expr` with the sort-range
    /// conjuncts removed (`None` = residual was solely the sort range). Applied
    /// post-decode (block-granular) ONLY for row groups in `timestamp_within_rgs`
    /// — where the sort-range conjuncts are footer-proven tautologies — so the
    /// sort column is never referenced and can be dropped from the projection.
    residual_expr_sans_sort_range: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Columns of `residual_expr_sans_sort_range` (seed for
    /// `required_predicate_columns` on WITHIN RGs; excludes the sort column).
    always_residual_columns_stripped: Vec<usize>,
    /// Row-group indices (segment-local) whose sort-range residual is a
    /// footer-proven tautology. For these RGs the evaluator uses the stripped
    /// residual + stripped column set. Empty disables the relaxation.
    timestamp_within_rgs: HashSet<usize>,
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
        performance_leaves: Vec<PerformanceLeaf>,
    ) -> Self {
        let always_residual_columns = residual_expr
            .as_ref()
            .map(expr_column_indices)
            .unwrap_or_default();
        let performance_leaf_columns = performance_leaves
            .iter()
            .map(|leaf| (leaf.annotation_id, expr_column_indices(&leaf.expr)))
            .collect();
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
            last_next_doc: std::sync::atomic::AtomicI32::new(i32::MIN),
            count_collector: OnceLock::new(),
            chunk_doc_bounds: None,
            performance_leaves,
            always_residual_columns,
            performance_leaf_columns,
            residual_expr_sans_sort_range: None,
            always_residual_columns_stripped: Vec::new(),
            timestamp_within_rgs: HashSet::new(),
        }
    }

    /// Enable the relaxed timestamp-WITHIN optimization. For every row group in
    /// `timestamp_within_rgs` the sort-range conjuncts are footer-proven
    /// tautologies, so this evaluator seeds `required_predicate_columns` from
    /// the stripped residual's columns (excluding the sort column) and applies
    /// `residual_expr_sans_sort_range` post-decode instead of the full residual.
    /// No-op when `timestamp_within_rgs` is empty.
    pub fn with_relaxed_within(
        mut self,
        residual_expr_sans_sort_range: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        timestamp_within_rgs: HashSet<usize>,
    ) -> Self {
        self.always_residual_columns_stripped = residual_expr_sans_sort_range
            .as_ref()
            .map(expr_column_indices)
            .unwrap_or_default();
        self.residual_expr_sans_sort_range = residual_expr_sans_sort_range;
        self.timestamp_within_rgs = timestamp_within_rgs;
        self
    }

    /// Provide the chunk's doc-id bounds so `count_docs_range` can cache one
    /// forward-cursor collector for the whole chunk (created with exactly
    /// these bounds — Java asserts the partition fits the leaf).
    pub fn with_chunk_doc_bounds(mut self, doc_min: i32, doc_max: i32) -> Self {
        self.chunk_doc_bounds = Some((doc_min, doc_max));
        self
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
    fn count_docs_range(&self, min_doc: i32, max_doc: i32) -> Result<Option<u64>, String> {
        if max_doc <= min_doc {
            return Ok(Some(0));
        }
        match &self.collector {
            // Correctness collector owns the filter: ask for the cardinality
            // directly (Weight#count / cursor count on the Java side). The
            // executor's count-shape gate guarantees no other Lucene leaf
            // exists, so no intersection is needed.
            Some(collector) if self.performance_provider_locks.is_empty() => {
                collector.count_docs(min_doc, max_doc)
            }
            // Collector + performance leaves would need bitset intersection —
            // unsupported on the count path (and excluded by the gate).
            Some(_) => Ok(None),
            None => match self.performance_provider_locks.len() {
                // No Lucene leaf at all: the gate guarantees the residual is a
                // sort-range tautology on this range → every doc matches.
                0 => Ok(Some((max_doc - min_doc).max(0) as u64)),
                // Exactly one performance-delegated leaf: force-delegate it to
                // Lucene and count. This is the q7 shape (indexed equality +
                // timestamp range count) — whole-chunk calls let Java answer
                // via Weight#count (TermQuery docFreq) in O(1) per segment.
                1 => {
                    let (&annotation_id, lock) = self
                        .performance_provider_locks
                        .iter()
                        .next()
                        .expect("len() == 1");
                    let context_id = self.context_id;
                    let mut init_failed = false;
                    let provider = lock.get_or_init(|| {
                        create_provider(context_id, annotation_id).unwrap_or_else(|e| {
                            init_failed = true;
                            log_debug!(
                                "count_docs_range: create_provider failed (annotation_id={}): {}",
                                annotation_id,
                                e
                            );
                            // Poison-free fallback: the dead handle is never
                            // used because we return None below.
                            ProviderHandle::new_dead()
                        })
                    });
                    if init_failed || provider.key() < 0 {
                        return Ok(None);
                    }
                    // With chunk bounds: ONE collector per (chunk × query),
                    // created on first use with the chunk partition so its
                    // forward cursor serves every subsequent per-RG count in
                    // this chunk (and whole-leaf coverage keeps Java's
                    // Weight#count fast path eligible). Without bounds
                    // (tests/legacy): per-call collector.
                    let collector = match (self.chunk_doc_bounds, self.count_collector.get()) {
                        (_, Some(collector)) => Arc::clone(collector),
                        (Some((chunk_min, chunk_max)), None) => {
                            let created = self.delegated_backend_collector_factory.create(
                                context_id,
                                provider.key(),
                                self.writer_generation,
                                chunk_min,
                                chunk_max,
                            )?;
                            Arc::clone(self.count_collector.get_or_init(|| created))
                        }
                        (None, None) => self.delegated_backend_collector_factory.create(
                            context_id,
                            provider.key(),
                            self.writer_generation,
                            min_doc,
                            max_doc,
                        )?,
                    };
                    if let Some(ref c) = self.ffm_collector_calls {
                        c.add(1);
                    }
                    collector.count_docs(min_doc, max_doc)
                }
                _ => Ok(None),
            },
        }
    }

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

        // Skip RG if the previous collectDocs told us the next match is beyond this RG.
        let last_next = self
            .last_next_doc
            .load(std::sync::atomic::Ordering::Acquire);
        // max_doc is exclusive, so nextDoc == max_doc also means "no match in this RG".
        if last_next >= max_doc {
            native_bridge_common::log_debug!(
                "SingleCollector: skipping RG {} — nextDoc={} >= maxDoc={}",
                rg.index,
                last_next,
                max_doc
            );
            return Ok(None);
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
        let mut candidates = match self.collector.as_ref() {
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

                // Call collector for each range, merge into one RG-relative bitmap.
                // Sub-ranges are ascending; carry the freshest next_doc forward so
                // a later sub-range skips/tightens on the position the iterator has
                // already advanced to, not the stale pre-loop value.
                let mut bm = RoaringBitmap::new();
                let mut next_doc_out = last_next;
                for (r_min, r_max) in &call_ranges {
                    if next_doc_out >= *r_max {
                        continue;
                    }
                    let effective_min = next_doc_out.max(*r_min);
                    let result = collector
                        .collect_packed_u64_bitset(effective_min, *r_max)
                        .map_err(|e| {
                            format!(
                                "collector.collect_packed_u64_bitset(rg={}, [{}, {})): {}",
                                rg.index, r_min, r_max, e
                            )
                        })?;
                    if let Some(ref c) = self.ffm_collector_calls {
                        c.add(1);
                    }
                    // Advance only forward — the iterator position is monotonic; guard
                    // against a stale/sentinel next_doc dragging it backward.
                    next_doc_out = next_doc_out.max(result.next_doc);
                    let offset = (effective_min as i64 - rg.first_row) as u32;
                    let num_docs = (*r_max - effective_min) as u32;
                    let bytes: &[u8] = unsafe {
                        std::slice::from_raw_parts(
                            result.words.as_ptr() as *const u8,
                            result.words.len() * 8,
                        )
                    };
                    let mut chunk = RoaringBitmap::from_lsb0_bytes(offset, bytes);
                    let upper = offset.saturating_add(num_docs);
                    if upper < u32::MAX {
                        chunk.remove_range(upper..);
                    }
                    bm |= chunk;
                }
                self.last_next_doc
                    .store(next_doc_out, std::sync::atomic::Ordering::Release);

                // For FullRange and TightenOuterBounds, AND with page bitmap
                // to remove rows in dead pages that the collector scanned.
                if self.call_strategy != CollectorCallStrategy::PageRangeSplit {
                    if let Some(ref ranges) = page_ranges {
                        let mut allowed = RoaringBitmap::new();
                        for (r_min, r_max) in ranges {
                            let lo = (*r_min as i64 - rg.first_row) as u32;
                            let hi = (*r_max as i64 - rg.first_row) as u32;
                            allowed.insert_range(lo..hi);
                        }
                        bm &= allowed;
                    }
                }
                bm
            }
            None => {
                // Performance-only query. Seed candidates with the page-pruned universe
                // (or the full RG if no PruningPredicate). The opportunistic peer branch
                // below may narrow further; otherwise DF's pushdown filter handles the
                // residual at decode time.
                let mut bm = RoaringBitmap::new();
                match &page_ranges {
                    Some(r) if r.is_empty() => return Ok(None),
                    Some(r) => {
                        for (r_min, r_max) in r {
                            let lo = (*r_min as i64 - rg.first_row) as u32;
                            let hi = (*r_max as i64 - rg.first_row) as u32;
                            bm.insert_range(lo..hi);
                        }
                    }
                    None => {
                        bm.insert_range(0..rg.num_rows as u32);
                    }
                }
                bm
            }
        };

        // ── Per-leaf XOR: DataFusion OR Lucene for each performance-delegated leaf ──
        //
        // For each dual-viable leaf, make an EXCLUSIVE choice for THIS RG using sound stats —
        // never evaluate both:
        //   * DataFusion authoritative — the leaf's own page-stat pruning already narrows this RG
        //     at/below the selectivity threshold: apply the leaf's `expr` post-decode (accumulate
        //     into `perf_residual`) and do NOT consult the peer.
        //   * Lucene authoritative — weak/absent stats: intersect the peer bitmap into
        //     `candidates` and do NOT apply the leaf's `expr` (no DataFusion residual / page
        //     pruning for this leaf).
        // Leaves are independent (some may pick DataFusion, others Lucene, in the same RG).
        // Unrelated native predicates live in `residual_expr`/`pruning_predicate` and always
        // apply, regardless of any leaf's choice.
        let mut perf_residual: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = None;
        // Relaxed timestamp-WITHIN: for a WITHIN row group the sort-range
        // conjuncts are footer-proven tautologies. Seed the required columns
        // from the STRIPPED residual (which omits the sort column) and carry
        // the stripped residual as this RG's always-native residual, so the
        // sort column is dropped from the projection and never referenced
        // post-decode. Normal RGs use the full residual + column set.
        let within_relaxed = self.timestamp_within_rgs.contains(&rg.index);
        let (seed_columns, rg_always_residual): (
            &Vec<usize>,
            Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        ) = if within_relaxed {
            (
                &self.always_residual_columns_stripped,
                self.residual_expr_sans_sort_range.clone(),
            )
        } else {
            (&self.always_residual_columns, self.residual_expr.clone())
        };
        let mut required_predicate_columns: HashSet<usize> =
            seed_columns.iter().copied().collect();
        for leaf in &self.performance_leaves {
            // Sound-stats selectivity of THIS leaf on THIS RG (independent of other predicates).
            // `None` == no usable parquet stats for the leaf's column → cannot prove DataFusion is
            // selective → the peer is authoritative (matches `should_consult_lucene`'s None arm).
            let leaf_ranges: Option<Vec<(i32, i32)>> =
                leaf.pruning_predicate.as_ref().and_then(|pp| {
                    self.page_pruner
                        .prune_rg(pp, rg.index, self.page_prune_metrics.as_ref())
                        .map(|sel| {
                            let mut ranges = Vec::new();
                            let mut rg_pos: i64 = 0;
                            for s in sel.iter() {
                                if !s.skip {
                                    let abs_min = min_doc + rg_pos as i32;
                                    let abs_max = min_doc + rg_pos as i32 + s.row_count as i32;
                                    ranges.push((abs_min, abs_max));
                                }
                                rg_pos += s.row_count as i64;
                            }
                            ranges
                        })
                });

            if !should_consult_lucene(&leaf_ranges, rg, HARDCODED_SELECTIVITY_THRESHOLD) {
                // DataFusion authoritative: apply the leaf's expr post-decode; no peer call.
                if let Some(columns) = self.performance_leaf_columns.get(&leaf.annotation_id) {
                    required_predicate_columns.extend(columns.iter().copied());
                }
                perf_residual = Some(match perf_residual {
                    None => Arc::clone(&leaf.expr),
                    Some(acc) => Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                        acc,
                        datafusion::logical_expr::Operator::And,
                        Arc::clone(&leaf.expr),
                    )),
                });
                continue;
            }

            // Lucene authoritative: consult the peer and intersect its bitmap. The leaf's
            // DataFusion `expr` is deliberately NOT added to `perf_residual` — enforcing the XOR.
            let lock = self
                .performance_provider_locks
                .get(&leaf.annotation_id)
                .ok_or_else(|| {
                    format!(
                        "performance leaf annotation_id {} has no provider lock",
                        leaf.annotation_id
                    )
                })?;
            let context_id = self.context_id;
            let annotation_id = leaf.annotation_id;
            let mut just_initialized = false;
            let provider = lock.get_or_init(|| {
                just_initialized = true;
                create_provider(context_id, annotation_id).expect("create_provider FFM upcall failed")
            });
            if just_initialized {
                log_debug!(
                    "[scf-rust] lazy provider initialized context_id={} annotation_id={} provider_key={}",
                    context_id,
                    annotation_id,
                    provider.key()
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
            let result = collector
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
            let offset = (min_doc as i64 - rg.first_row) as u32;
            let num_docs = (max_doc - min_doc) as u32;
            let bytes: &[u8] = unsafe {
                std::slice::from_raw_parts(result.words.as_ptr() as *const u8, result.words.len() * 8)
            };
            let mut peer_bm = RoaringBitmap::from_lsb0_bytes(offset, bytes);
            let upper = offset.saturating_add(num_docs);
            if upper < u32::MAX {
                peer_bm.remove_range(upper..);
            }
            candidates &= peer_bm;
        }

        if candidates.is_empty() {
            return Ok(None);
        }

        let mut required_predicate_columns: Vec<usize> =
            required_predicate_columns.into_iter().collect();
        required_predicate_columns.sort_unstable();

        // Materialise the final RG-relative bitmap as an Arrow `Buffer`
        // in Arrow's native LSB-first layout. This is the ONLY
        // representation the hot paths (`on_batch_mask`, `build_mask`)
        // need; they construct zero-copy `BooleanBuffer` views via
        // `BooleanBuffer::new(buf.clone(), bit_offset, bit_len)`.
        let mask_len = rg.num_rows as usize;
        let packed_bits = bitmap_to_packed_bits(&candidates, mask_len as u32);
        let mask_buffer = datafusion::arrow::buffer::Buffer::from_vec(packed_bits);
        Ok(Some(PrefetchedRg {
            candidates: candidates.clone(),
            eval_nanos: t.elapsed().as_nanos() as u64,
            context: Box::new(SingleCollectorState {
                candidates,
                mask_buffer: mask_buffer.clone(),
                mask_len,
                perf_residual,
                always_residual: rg_always_residual,
            }),
            mask_buffer: Some(mask_buffer),
            required_predicate_columns: Some(required_predicate_columns),
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
        // Fast path: no always-native residual AND no performance leaves ⇒ no post-decode work is
        // possible (a per-RG `perf_residual` can only exist when `performance_leaves` is non-empty).
        // Return before touching `rg_state` so this path never requires a `SingleCollectorState`
        // (the stream's `current_mask`, built from candidates, handles Collector / peer narrowing).
        if self.residual_expr.is_none() && self.performance_leaves.is_empty() {
            return Ok(None);
        }

        let state = rg_state
            .downcast_ref::<SingleCollectorState>()
            .ok_or_else(|| {
                "SingleCollectorEvaluator: rg_state is not SingleCollectorState".to_string()
            })?;

        // Effective post-decode residual = always-native residual AND the per-RG
        // DataFusion-selected performance-leaf residual (see `SingleCollectorState::perf_residual`).
        // Performance leaves that chose Lucene for this RG are already reflected in the candidate
        // bitmap and are intentionally excluded here — the per-leaf XOR. No residual at all → no
        // post-decode work; the stream's `current_mask` (built from candidates) handles Collector /
        // peer narrowing.
        let effective_residual: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
            match (state.always_residual.as_ref(), state.perf_residual.as_ref()) {
                (None, None) => return Ok(None),
                (Some(r), None) => Some(Arc::clone(r)),
                (None, Some(p)) => Some(Arc::clone(p)),
                (Some(r), Some(p)) => Some(Arc::new(
                    datafusion::physical_expr::expressions::BinaryExpr::new(
                        Arc::clone(r),
                        datafusion::logical_expr::Operator::And,
                        Arc::clone(p),
                    ),
                )),
            };
        let residual = effective_residual
            .as_ref()
            .expect("effective_residual is Some (None,None returned early)");

        // Build Collector mask over delivered rows via PositionMap.
        // All paths produce a `BooleanArray` whose underlying
        // `Buffer` is a refcounted view into `state.mask_buffer` —
        // zero allocation for Identity, at most one small packed
        // Vec<u64> for Runs.
        let collector_mask: BooleanArray = match position_map {
            // Identity: delivered row i == rg_position (batch_offset + i).
            // BooleanBuffer::new adjusts bit_offset without copying the
            // underlying Buffer. The returned BooleanArray points into
            // state.mask_buffer; lifecycle is Arc-managed.
            PositionMap::Identity { .. } => {
                let bb = datafusion::arrow::buffer::BooleanBuffer::new(
                    state.mask_buffer.clone(),
                    batch_offset,
                    batch_len,
                );
                BooleanArray::new(bb, None)
            }
            // Every delivered row is by construction a candidate — mask is all-true.
            PositionMap::Bitmap { .. } => BooleanArray::new(
                datafusion::arrow::buffer::BooleanBuffer::new_set(batch_len),
                None,
            ),
            // Runs: gather per-row bit from the shared mask_buffer into
            // a new packed Vec<u64> (small — bounded by batch_len/64).
            PositionMap::Runs { .. } => {
                let words = batch_len.div_ceil(64);
                let mut out = vec![0u64; words];
                let src_bytes = state.mask_buffer.as_slice();
                for i in 0..batch_len {
                    let delivered_idx = batch_offset + i;
                    let rg_pos = position_map.rg_position(delivered_idx).ok_or_else(|| {
                        format!(
                            "SingleCollectorEvaluator: delivered_idx {} out of range",
                            delivered_idx
                        )
                    })?;
                    // Read bit rg_pos from the packed buffer (LSB-first).
                    let hit = rg_pos < state.mask_len
                        && (src_bytes[rg_pos >> 3] >> (rg_pos & 7)) & 1 == 1;
                    if hit {
                        out[i >> 6] |= 1u64 << (i & 63);
                    }
                }
                packed_bits_to_boolean_array(out, batch_len)
            }
        };

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

    /// When we have a residual to apply in `on_batch_mask`, pushdown
    /// must be OFF in **block-granular mode** because we use
    /// `PositionMap` to look up RG positions over the full delivered
    /// rowset — pushdown would drop rows and misalign. In
    /// **row-granular mode** (`min_skip_run == 1`), pushdown is safe
    /// and desirable: parquet applies the residual in lockstep with
    /// decoding, `on_batch_mask` returns `None`, and output is
    /// exact. But the evaluator doesn't know min_skip_run — the
    /// stream does. The stream guards this via its
    /// `alignment_risk = min_skip_run != 1 && needs_row_mask()`
    /// check plus `forbid_parquet_pushdown`. We return `false` here
    /// and rely on `needs_row_mask = true` (default when residual is
    /// present) to trigger the stream's alignment guard in block
    /// mode; in row-granular mode that guard is inactive and
    /// pushdown proceeds.
    fn forbid_parquet_pushdown(&self) -> bool {
        // When there are performance-delegated leaves, the per-RG DataFusion-XOR-Lucene choice
        // means a leaf's residual is decided per row group and must be applied post-decode by
        // `on_batch_mask` (never statically pushed to parquet — a per-RG Lucene choice can make the
        // leaf authoritative instead). Force the post-decode (block-granular) path so that per-RG
        // residual is honored. Correctness-only queries (no perf leaves) keep pushdown enabled.
        !self.performance_leaves.is_empty()
    }

    /// Stream's `current_mask` construction consults this. When
    /// residual is set, we return `true` so the stream knows our
    /// `on_batch_mask` uses PositionMap (alignment risk) — this flag
    /// flips the stream's `alignment_risk` computation which
    /// suppresses pushdown in block-granular mode. In row-granular
    /// mode (min_skip_run == 1) the stream ignores this flag's
    /// pushdown impact and pushes anyway (which is what we want:
    /// parquet applies residual during decode of already-narrowed
    /// rowset, on_batch_mask returns None below).
    ///
    /// Without residual, we return `true` too — stream builds
    /// `current_mask` from Collector bitmap to narrow post-decode
    /// (legacy path for SingleCollector without a residual wasn't
    /// used in production but kept for defensive correctness).
    fn needs_row_mask(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata;
    use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
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
        ) -> Result<CollectDocsResult, String> {
            let span = (max_doc - min_doc) as usize;
            let mut bitset = vec![0u64; (span + 63) / 64];
            for &doc in &self.docs {
                if doc >= min_doc && doc < max_doc {
                    let idx = (doc - min_doc) as usize;
                    bitset[idx / 64] |= 1u64 << (idx % 64);
                }
            }
            Ok(bitset.into())
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
        let pruner = PagePruner::new(
            meta.schema(),
            meta.metadata().clone(),
            meta.schema().clone(),
        );
        Arc::new(pruner)
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
            Vec::new(),
        );

        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![0u32, 3, 7]);
    }

    // ── Relaxed timestamp-WITHIN (projection + residual selection) ──────────
    //
    // Full residual = `(ts >= 2 AND ts <= 5) AND other >= 0`, columns {0, 3}.
    // Stripped residual = `other >= 0`, column {3} (sort col 0 removed).
    // Enabling `with_relaxed_within(stripped, {0})`:
    //   - RG 0 (WITHIN): `required_predicate_columns` must be seeded from the
    //     STRIPPED set → {3}; the sort column 0 is dropped from the projection.
    //     The per-RG `SingleCollectorState.always_residual` must be the stripped
    //     residual (so `remap_expr_to_batch` never references the dropped sort
    //     column post-decode — the block-granular trap).
    //   - RG 1 (boundary / not WITHIN): full residual + full column set {0, 3}.
    //
    // NOT executed on desktop (see task). Full row-/block-granular result
    // equivalence + boundary filtering is covered by the live EC2 validation
    // (sparse-AND exact count + variant sweep), not this unit test.
    #[test]
    fn relaxed_within_drops_sort_column_and_uses_stripped_residual() {
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::physical_expr::PhysicalExpr;
        use std::collections::HashSet;

        let col = |name: &str, idx: usize| -> Arc<dyn PhysicalExpr> {
            Arc::new(Column::new(name, idx))
        };
        let lit = |v: i64| -> Arc<dyn PhysicalExpr> {
            Arc::new(Literal::new(ScalarValue::Int64(Some(v))))
        };
        let ts_ge: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(col("ts", 0), Operator::GtEq, lit(2)));
        let ts_le: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(col("ts", 0), Operator::LtEq, lit(5)));
        let other_ge: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(col("other", 3), Operator::GtEq, lit(0)));
        let ts_range: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(ts_ge, Operator::And, ts_le));
        let full_residual: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(ts_range, Operator::And, Arc::clone(&other_ge)));
        let stripped_residual: Arc<dyn PhysicalExpr> = Arc::clone(&other_ge);

        // Candidates over both RGs so neither prefetch returns None.
        let collector =
            Arc::new(StubCollector { docs: (0..16).collect() }) as Arc<dyn RowGroupDocsCollector>;
        let pruner = minimal_page_pruner();
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            pruner,
            None,
            Some(Arc::clone(&full_residual)),
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
            Vec::new(),
        )
        .with_relaxed_within(Some(Arc::clone(&stripped_residual)), HashSet::from([0usize]));

        // RG 0 is WITHIN: sort column (0) dropped, only stripped column {3}.
        let rg0 = RowGroupInfo { index: 0, first_row: 0, num_rows: 8 };
        let pf0 = eval.prefetch_rg(&rg0, 0, 8).unwrap().expect("rg0 candidates");
        assert_eq!(
            pf0.required_predicate_columns.as_ref().unwrap(),
            &vec![3usize],
            "WITHIN RG must drop the sort column and keep only stripped-residual columns"
        );
        let st0 = pf0
            .context
            .downcast_ref::<SingleCollectorState>()
            .expect("SingleCollectorState");
        assert!(
            Arc::ptr_eq(st0.always_residual.as_ref().unwrap(), &stripped_residual),
            "WITHIN RG must carry the stripped residual for post-decode eval"
        );

        // RG 1 is NOT WITHIN: full residual + full column set {0, 3}.
        let rg1 = RowGroupInfo { index: 1, first_row: 8, num_rows: 8 };
        let pf1 = eval.prefetch_rg(&rg1, 8, 16).unwrap().expect("rg1 candidates");
        assert_eq!(
            pf1.required_predicate_columns.as_ref().unwrap(),
            &vec![0usize, 3usize],
            "boundary RG must keep the full predicate column set"
        );
        let st1 = pf1
            .context
            .downcast_ref::<SingleCollectorState>()
            .expect("SingleCollectorState");
        assert!(
            Arc::ptr_eq(st1.always_residual.as_ref().unwrap(), &full_residual),
            "boundary RG must carry the full residual"
        );
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
            Vec::new(),
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
    fn single_collector_needs_row_mask() {
        // SingleCollectorEvaluator returns None from on_batch_mask, so
        // IndexedStream must build current_mask from candidate offsets
        // (it's the only post-decode filter we have on this path).
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
            Vec::new(),
        );
        assert!(eval.needs_row_mask());
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
            Vec::new(),
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
            Vec::new(),
        );

        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        let got: Vec<u32> = prefetched.candidates.iter().collect();
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
            Vec::new(),
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
            Vec::new(),
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
        let got: Vec<u32> = prefetched.candidates.iter().collect();
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
            Vec::new(),
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
        let got: Vec<u32> = prefetched.candidates.iter().collect();
        assert_eq!(got, vec![1u32, 5]);
    }

    /// Mock collector that returns specific docs AND a configurable next_doc.
    #[derive(Debug)]
    struct NextDocCollector {
        docs: Vec<i32>,
        next_doc: i32,
    }

    impl RowGroupDocsCollector for NextDocCollector {
        fn collect_packed_u64_bitset(
            &self,
            min_doc: i32,
            max_doc: i32,
        ) -> Result<CollectDocsResult, String> {
            let span = (max_doc - min_doc) as usize;
            let mut words = vec![0u64; (span + 63) / 64];
            for &doc in &self.docs {
                if doc >= min_doc && doc < max_doc {
                    let idx = (doc - min_doc) as usize;
                    words[idx / 64] |= 1u64 << (idx % 64);
                }
            }
            Ok(CollectDocsResult {
                words,
                next_doc: self.next_doc,
            })
        }
    }

    #[test]
    fn next_doc_skips_subsequent_rg() {
        // RG0 [0,8): collector returns next_doc=20 (beyond RG1's max_doc=16)
        // RG1 [8,16): should be skipped entirely
        // RG2 [16,24): next_doc=20 < 24, should NOT be skipped (doc 20 is in range)
        let collector = Arc::new(NextDocCollector {
            docs: vec![2, 20],
            next_doc: 20,
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
            Vec::new(),
        );

        let rg0 = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let rg1 = RowGroupInfo {
            index: 1,
            first_row: 8,
            num_rows: 8,
        };
        let rg2 = RowGroupInfo {
            index: 2,
            first_row: 16,
            num_rows: 8,
        };

        // RG0: has doc 2
        let pf = eval.prefetch_rg(&rg0, 0, 8).unwrap().expect("has match");
        assert_eq!(pf.candidates.iter().collect::<Vec<_>>(), vec![2u32]);

        // RG1: skipped (next_doc=20 > max_doc=16)
        assert!(eval.prefetch_rg(&rg1, 8, 16).unwrap().is_none());

        // RG2: NOT skipped (next_doc=20 < max_doc=24)
        let pf = eval.prefetch_rg(&rg2, 16, 24).unwrap();
        assert!(pf.is_some());
    }

    #[test]
    fn next_doc_tightens_min_doc() {
        // Collector has doc at position 5. next_doc=5 means the iterator
        // is at doc 5 after RG0. For RG1 [4,8), effective_min should be
        // max(5, 4) = 5, not 4.
        let collector = Arc::new(NextDocCollector {
            docs: vec![1, 5],
            next_doc: 5,
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
            Vec::new(),
        );

        let rg0 = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 4,
        };
        let rg1 = RowGroupInfo {
            index: 1,
            first_row: 4,
            num_rows: 4,
        };

        // RG0 [0,4): doc 1 matches
        let pf = eval.prefetch_rg(&rg0, 0, 4).unwrap().expect("has match");
        assert_eq!(pf.candidates.iter().collect::<Vec<_>>(), vec![1u32]);

        // RG1 [4,8): doc 5 matches, effective_min tightened to 5
        let pf = eval.prefetch_rg(&rg1, 4, 8).unwrap().expect("has match");
        assert_eq!(pf.candidates.iter().collect::<Vec<_>>(), vec![1u32]); // doc 5 is at RG-relative pos 1
    }

    #[test]
    fn next_doc_max_value_skips_all_remaining() {
        // next_doc = i32::MAX means scorer exhausted — all subsequent RGs skipped
        let collector = Arc::new(NextDocCollector {
            docs: vec![0],
            next_doc: i32::MAX,
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
            Vec::new(),
        );

        let rg0 = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let rg1 = RowGroupInfo {
            index: 1,
            first_row: 8,
            num_rows: 8,
        };

        // RG0: has doc 0
        assert!(eval.prefetch_rg(&rg0, 0, 8).unwrap().is_some());

        // RG1: skipped (next_doc=MAX > any max_doc)
        assert!(eval.prefetch_rg(&rg1, 8, 16).unwrap().is_none());
    }

    #[test]
    fn next_doc_at_rg_boundary_is_not_dropped() {
        // Regression for the exclusive-boundary bug: a match sitting exactly at
        // an RG's start (== previous RG's exclusive max_doc) must NOT be dropped.
        // docs: 5 (in RG0) and 8 (start of RG1, == RG0's max_doc=8).
        let collector = Arc::new(NextDocCollector {
            docs: vec![5, 8],
            next_doc: 8,
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
            Vec::new(),
        );

        let rg0 = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let rg1 = RowGroupInfo {
            index: 1,
            first_row: 8,
            num_rows: 8,
        };

        // RG0 [0,8): doc 5 matches, next_doc=8 (boundary).
        let pf0 = eval
            .prefetch_rg(&rg0, 0, 8)
            .unwrap()
            .expect("RG0 has doc 5");
        assert_eq!(pf0.candidates.iter().collect::<Vec<_>>(), vec![5u32]);

        // RG1 [8,16): next_doc=8 == min_doc, NOT skipped. Doc 8 must be collected.
        let pf1 = eval
            .prefetch_rg(&rg1, 8, 16)
            .unwrap()
            .expect("RG1 has boundary doc 8");
        assert_eq!(pf1.candidates.iter().collect::<Vec<_>>(), vec![0u32]); // doc 8 at RG-relative pos 0
    }

    // ── Performance-leaf per-RG XOR (DataFusion OR Lucene, never both) ──────────

    /// Peer factory that records how many times it was asked to build a collector and
    /// returns a fixed doc set. Used to assert whether Lucene was consulted for an RG.
    #[derive(Debug)]
    struct RecordingPeerFactory {
        docs: Vec<i32>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl DelegatedBackendCollectorFactory for RecordingPeerFactory {
        fn create(
            &self,
            _context_id: i64,
            _provider_key: i32,
            _writer_generation: i64,
            _doc_min: i32,
            _doc_max: i32,
        ) -> Result<Arc<dyn RowGroupDocsCollector>, String> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Arc::new(StubCollector {
                docs: self.docs.clone(),
            }))
        }
    }

    fn int_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    /// Preset provider locks (one per annotation id) so no real FFM upcall is made.
    fn preset_locks(ids: &[i32]) -> Arc<HashMap<i32, Arc<OnceLock<ProviderHandle>>>> {
        let mut m = HashMap::new();
        for &id in ids {
            let lock = Arc::new(OnceLock::new());
            lock.set(ProviderHandle::new_for_test(id)).expect("OnceLock set");
            m.insert(id, lock);
        }
        Arc::new(m)
    }

    fn int_cmp_expr(
        op: datafusion::logical_expr::Operator,
        v: i32,
    ) -> Arc<dyn datafusion::physical_expr::PhysicalExpr> {
        use datafusion::common::ScalarValue;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
        use datafusion::physical_expr::PhysicalExpr;
        let left: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new("a", 0));
        let right: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(v))));
        Arc::new(BinaryExpr::new(left, op, right))
    }

    fn perf_leaf(id: i32, op: datafusion::logical_expr::Operator, v: i32) -> PerformanceLeaf {
        let expr = int_cmp_expr(op, v);
        let pp = crate::indexed_table::page_pruner::build_pruning_predicate(
            &expr,
            int_schema(),
        );
        PerformanceLeaf {
            annotation_id: id,
            expr,
            pruning_predicate: pp,
        }
    }

    fn perf_state(pf: &PrefetchedRg) -> &SingleCollectorState {
        pf.context
            .downcast_ref::<SingleCollectorState>()
            .expect("SingleCollectorState")
    }

    /// A performance leaf with no usable stats (pruning_predicate = None) is Lucene-selected:
    /// the peer IS consulted, its bitmap is AND-intersected into the candidates, and the leaf's
    /// DataFusion expr is NOT added to the post-decode residual (per-RG XOR).
    #[test]
    fn perf_leaf_lucene_selected_consults_peer_and_skips_native_residual() {
        // Collector matches {0,1,2,3}; peer (Lucene) matches {1,3}. Expected candidates = {1,3}.
        let collector = Arc::new(StubCollector {
            docs: vec![0, 1, 2, 3],
        }) as Arc<dyn RowGroupDocsCollector>;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let leaf = PerformanceLeaf {
            annotation_id: 7,
            expr: int_cmp_expr(datafusion::logical_expr::Operator::Eq, 999),
            pruning_predicate: None, // no stats → Lucene authoritative
        };
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            minimal_page_pruner(),
            None,
            None, // no always-native residual
            None,
            None,
            CollectorCallStrategy::FullRange,
            preset_locks(&[7]),
            0,
            Arc::new(RecordingPeerFactory {
                docs: vec![1, 3],
                calls: Arc::clone(&calls),
            }),
            0,
            None,
            None,
            HashMap::new(),
            vec![leaf],
        );
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let pf = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1, "peer must be consulted once");
        let got: Vec<u32> = pf.candidates.iter().collect();
        assert_eq!(got, vec![1u32, 3], "candidates = collector ∩ peer bitmap");
        // XOR: the leaf's native expr must NOT be applied as a residual.
        assert!(perf_state(&pf).perf_residual.is_none(), "Lucene-selected leaf leaves no DF residual");
        assert_eq!(
            pf.required_predicate_columns.as_ref().unwrap(),
            &Vec::<usize>::new(),
            "Lucene-selected leaf column must not be read from Parquet"
        );
    }

    /// A performance leaf whose own page stats prune the RG below the selectivity threshold is
    /// DataFusion-selected: the peer is NOT consulted, and the leaf's DataFusion expr becomes the
    /// per-RG post-decode residual. (`a > 100` over the all-zero column prunes to empty.)
    #[test]
    fn perf_leaf_datafusion_selected_skips_peer_and_evaluates_native() {
        let collector = Arc::new(StubCollector {
            docs: vec![0, 1, 2, 3],
        }) as Arc<dyn RowGroupDocsCollector>;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let leaf = perf_leaf(7, datafusion::logical_expr::Operator::Gt, 100);
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            minimal_page_pruner(),
            None,
            None,
            None,
            None,
            CollectorCallStrategy::FullRange,
            preset_locks(&[7]),
            0,
            Arc::new(RecordingPeerFactory {
                docs: vec![1, 3],
                calls: Arc::clone(&calls),
            }),
            0,
            None,
            None,
            HashMap::new(),
            vec![leaf],
        );
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let pf = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("collector matched");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "DataFusion-selected leaf must NOT consult the peer"
        );
        // Candidates are the collector's docs (unnarrowed by the peer)...
        let got: Vec<u32> = pf.candidates.iter().collect();
        assert_eq!(got, vec![0u32, 1, 2, 3]);
        // ...and the leaf's native expr is carried as the per-RG residual for post-decode.
        assert!(
            perf_state(&pf).perf_residual.is_some(),
            "DataFusion-selected leaf must carry its native residual"
        );
        assert_eq!(
            pf.required_predicate_columns.as_ref().unwrap(),
            &vec![0],
            "DataFusion-selected leaf column must be present in this RG's projection"
        );
    }

    /// Empty performance_leaves: no peer factory call, behavior is unchanged from a plain
    /// correctness+native query. Guards the fuzz-harness / correctness-only path.
    #[test]
    fn no_perf_leaves_does_not_consult_peer() {
        let collector = Arc::new(StubCollector {
            docs: vec![0, 2, 4],
        }) as Arc<dyn RowGroupDocsCollector>;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            minimal_page_pruner(),
            None,
            None,
            None,
            None,
            CollectorCallStrategy::FullRange,
            Arc::new(HashMap::new()),
            0,
            Arc::new(RecordingPeerFactory {
                docs: vec![0],
                calls: Arc::clone(&calls),
            }),
            0,
            None,
            None,
            HashMap::new(),
            Vec::new(),
        );
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let pf = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(pf.candidates.iter().collect::<Vec<_>>(), vec![0u32, 2, 4]);
        assert!(perf_state(&pf).perf_residual.is_none());
    }

    /// `forbid_parquet_pushdown` is true exactly when there are performance leaves — forcing the
    /// per-RG residual to be honored post-decode (never statically pushed to parquet).
    #[test]
    fn forbid_pushdown_iff_perf_leaves_present() {
        let mk = |leaves: Vec<PerformanceLeaf>, locks| {
            SingleCollectorEvaluator::new(
                Some(Arc::new(StubCollector { docs: vec![0] }) as Arc<dyn RowGroupDocsCollector>),
                minimal_page_pruner(),
                None,
                None,
                None,
                None,
                CollectorCallStrategy::FullRange,
                locks,
                0,
                Arc::new(FfmDelegatedBackendCollectorFactory),
                0,
                None,
                None,
                HashMap::new(),
                leaves,
            )
        };
        assert!(!mk(Vec::new(), Arc::new(HashMap::new())).forbid_parquet_pushdown());
        assert!(mk(
            vec![perf_leaf(1, datafusion::logical_expr::Operator::Eq, 5)],
            preset_locks(&[1])
        )
        .forbid_parquet_pushdown());
    }

    /// `should_consult_lucene` boundary: strictly greater-than the threshold consults; at/below
    /// does not; no stats (None) always consults.
    #[test]
    fn should_consult_lucene_threshold_boundary() {
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 100,
        };
        // 6/100 = 0.06 > 0.05 → consult.
        assert!(should_consult_lucene(&Some(vec![(0, 6)]), &rg, 0.05));
        // 5/100 = 0.05, NOT strictly greater → do not consult (DataFusion selective enough).
        assert!(!should_consult_lucene(&Some(vec![(0, 5)]), &rg, 0.05));
        // No usable stats → consult.
        assert!(should_consult_lucene(&None, &rg, 0.05));
        // Empty ranges → 0 surviving → do not consult.
        assert!(!should_consult_lucene(&Some(vec![]), &rg, 0.05));
    }

    /// Mixed choice in a SINGLE row group: two independent performance leaves where one selects
    /// DataFusion (its own page stats prune the RG empty) and the other selects Lucene (no usable
    /// stats). Proves the per-leaf XOR: the Lucene-selected leaf's peer bitmap narrows the
    /// candidates while its native expr is NOT evaluated (never added to the residual), and the
    /// DataFusion-selected leaf contributes its native expr as the per-RG residual with no peer call.
    #[test]
    fn perf_leaf_mixed_datafusion_and_lucene_in_one_rg() {
        // Collector matches {0,1,2,3}; the Lucene-selected leaf's peer matches {1,3}.
        let collector = Arc::new(StubCollector {
            docs: vec![0, 1, 2, 3],
        }) as Arc<dyn RowGroupDocsCollector>;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // Leaf A (annotation 7): no usable stats → Lucene authoritative (peer consulted). Its expr
        // (`a == 42`) must NOT be evaluated post-decode — if it were, it would falsely drop rows.
        let lucene_leaf = PerformanceLeaf {
            annotation_id: 7,
            expr: int_cmp_expr(datafusion::logical_expr::Operator::Eq, 42),
            pruning_predicate: None,
        };
        // Leaf B (annotation 9): `a > 100` over the all-zero fixture prunes the RG empty → DataFusion
        // authoritative (peer NOT consulted); its expr becomes the per-RG residual.
        let df_leaf = perf_leaf(9, datafusion::logical_expr::Operator::Gt, 100);
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            minimal_page_pruner(),
            None,
            None, // no always-native residual
            None,
            None,
            CollectorCallStrategy::FullRange,
            preset_locks(&[7]), // only the Lucene-selected leaf needs a provider lock
            0,
            Arc::new(RecordingPeerFactory {
                docs: vec![1, 3],
                calls: Arc::clone(&calls),
            }),
            0,
            None,
            None,
            HashMap::new(),
            vec![lucene_leaf, df_leaf],
        );
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let pf = eval.prefetch_rg(&rg, 0, 8).unwrap().expect("has matches");
        // Exactly ONE peer consultation — only the Lucene-selected leaf, never the DataFusion one.
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "only the Lucene-selected leaf consults the peer"
        );
        // Candidates narrowed by the Lucene leaf's peer bitmap (collector ∩ {1,3}); the DataFusion
        // leaf defers to a post-decode residual and does not narrow candidates here.
        assert_eq!(pf.candidates.iter().collect::<Vec<_>>(), vec![1u32, 3]);
        // The DataFusion-selected leaf contributed a per-RG residual; the Lucene leaf did NOT
        // (per-leaf XOR — its native expr is proven unevaluated by the candidate set above).
        assert!(
            perf_state(&pf).perf_residual.is_some(),
            "DataFusion-selected leaf carries a per-RG residual"
        );
    }

    /// The `(Some always-native residual, Some DataFusion-selected perf residual)` arm of
    /// `on_batch_mask`: the always-native residual AND the DataFusion-selected leaf's expr are
    /// AND-combined and both applied post-decode. Proves an unrelated always-native residual
    /// remains in force alongside a DataFusion-selected performance leaf.
    #[test]
    fn perf_leaf_and_always_native_residual_both_apply() {
        use datafusion::arrow::array::BooleanArray;
        // Always-native residual `a < 300` — no pruning predicate, so it is always applied and is
        // never itself a performance leaf.
        let native = int_cmp_expr(datafusion::logical_expr::Operator::Lt, 300);
        // DataFusion-selected perf leaf `a > 100` (prunes the all-zero fixture empty → DF authoritative).
        let df_leaf = perf_leaf(7, datafusion::logical_expr::Operator::Gt, 100);
        // Collector matches all 5 rows so the collector mask doesn't mask the residual's effect.
        let collector = Arc::new(StubCollector {
            docs: vec![0, 1, 2, 3, 4],
        }) as Arc<dyn RowGroupDocsCollector>;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let eval = SingleCollectorEvaluator::new(
            Some(collector),
            minimal_page_pruner(),
            None,
            Some(native), // always-native residual
            None,
            None,
            CollectorCallStrategy::FullRange,
            preset_locks(&[7]),
            0,
            Arc::new(RecordingPeerFactory {
                docs: vec![0],
                calls: Arc::clone(&calls),
            }),
            0,
            None,
            None,
            HashMap::new(),
            vec![df_leaf],
        );
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 5,
        };
        let pf = eval.prefetch_rg(&rg, 0, 5).unwrap().expect("has matches");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "DataFusion-selected leaf must not consult the peer"
        );
        assert!(
            perf_state(&pf).perf_residual.is_some(),
            "DataFusion-selected leaf carries a per-RG residual"
        );

        // Post-decode: combined mask = collector(all-true) AND (a < 300) AND (a > 100).
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            int_schema(),
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                50, 150, 350, 200, 500,
            ]))],
        )
        .unwrap();
        let pm = PositionMap::Identity { delivered_count: 5 };
        let mask: BooleanArray = eval
            .on_batch_mask(pf.context.as_ref(), 0, &pm, 0, 5, &batch)
            .unwrap()
            .expect("residual present ⇒ Some(mask)");
        let kept: Vec<usize> = (0..mask.len()).filter(|&i| mask.value(i)).collect();
        // 50: native T, perf F → F | 150: T,T → T | 350: native F → F | 200: T,T → T | 500: native F → F.
        // The result differs from either residual alone, proving BOTH are applied.
        assert_eq!(
            kept,
            vec![1usize, 3],
            "always-native AND DataFusion-selected perf residual are both applied"
        );
    }

    // Keep the `fmt` import used
    #[allow(dead_code)]
    fn _use(_: &dyn fmt::Debug) {}
}
