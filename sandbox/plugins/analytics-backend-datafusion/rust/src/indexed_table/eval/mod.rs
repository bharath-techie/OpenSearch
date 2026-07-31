/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Row-group-level bitset sources — the pluggability seam for where
//! boolean tree evaluation happens.
//!
//! [`IndexedStream`](crate::indexed_table::stream::IndexedStream) only depends
//! on [`RowGroupBitsetSource`]. The source of the bitset is abstracted.
//!
//! # Invariant — row-group-at-a-time
//!
//! The trait methods operate on one RG. There is no `prefetch_shard` or
//! `evaluate_full_filter` method. Even when tree evaluation eventually moves
//! elsewhere:
//!
//! - Bitsets stay small (~512 bytes per RG).
//! - Prefetch overlaps the next RG's bitset with the current RG's parquet read.
//! - Memory stays bounded regardless of shard size.
//!
//! # Pluggable tree evaluation (multi-filter tree path)
//!
//! For tree queries, evaluation has two orthogonal concerns:
//!
//! 1. **Tree evaluation strategy** ([`TreeEvaluator`]) — the algorithm that
//!    walks the tree, combines bitmaps, produces superset candidates +
//!    exact per-batch mask. Today: [`bitmap_tree::BitmapTreeEvaluator`].
//!    This is extensible to different implementations.
//! 2. **Leaf bitmap source** ([`LeafBitmapSource`]) — given a `Collector`
//!    leaf, produce its RoaringBitmap for this RG. Today: backend-backed
//!    (FFM upcall + bitset expansion).
//!
//! [`TreeBitsetSource`] composes any `TreeEvaluator` with any
//! `LeafBitmapSource` and exposes the composite as a `RowGroupBitsetSource`.
//! Swapping impls requires only passing different `Arc`s at construction.

pub mod bitmap_tree;
pub mod decode_predicate;
pub mod eval_helpers;
pub mod predicate_evaluator;
pub mod single_collector;

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use roaring::RoaringBitmap;

/// Rewrite Column indices in a PhysicalExpr to match the delivered batch's
/// schema by name. Substrait-decoded predicates carry indices into the full
/// table schema; the delivered batch is projected (only predicate-referenced
/// columns) so the indices need to be reseated. Both the SingleCollector
/// residual eval and the BitmapTree predicate eval-via-DF path need this —
/// shared here to avoid duplication.
pub(super) fn remap_expr_to_batch(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    batch: &RecordBatch,
) -> Result<Arc<dyn datafusion::physical_expr::PhysicalExpr>, String> {
    use datafusion::common::tree_node::TreeNode;
    use datafusion::physical_expr::expressions::Column;

    expr.clone()
        .transform(|e| {
            if let Some(col) = e.downcast_ref::<Column>() {
                if let Ok(new_idx) = batch.schema().index_of(col.name()) {
                    if new_idx != col.index() {
                        let remapped = Arc::new(Column::new(col.name(), new_idx))
                            as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
                        return Ok(datafusion::common::tree_node::Transformed::yes(remapped));
                    }
                }
            }
            Ok(datafusion::common::tree_node::Transformed::no(e))
        })
        .map(|t| t.data)
        .map_err(|e| format!("remap_expr_to_batch: {}", e))
}

use super::bool_tree::ResolvedNode;
use super::page_pruner::PagePruneMetrics;
use super::page_pruner::PagePruner;
use super::page_pruner::StatsPruneTree;
use super::stream::RowGroupInfo;
use datafusion::parquet::arrow::arrow_reader::RowSelection;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// How a collector's doc-range is narrowed relative to page-pruning or
/// accumulator results. Shared by both the single-collector and
/// bitmap-tree evaluator paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectorCallStrategy {
    /// Call collector once for the full `[min_doc, max_doc)` range.
    /// One FFM call, simple.
    FullRange,
    /// Tighten to `[first_surviving, last_surviving)` before calling.
    /// Skips leading/trailing dead ranges. One FFM call, never regresses.
    TightenOuterBounds,
    /// Call collector once per contiguous surviving range. Fewer docs
    /// scanned per call but more FFM calls. Best when the collector is
    /// expensive and pruning is heavy.
    PageRangeSplit,
}

/// RG-relative positions of the rows in one delivered batch.
///
/// Backed by the `__row_id__` column the indexed scan projects. `__row_id__` is
/// a physical `INT64 REQUIRED` column that every OpenSearch parquet writer
/// appends, holding the row's position within the file (see
/// `parquet-data-format`'s `merge/schema.rs` and the sorting writer's
/// sequential rewrite). Rebasing by the row group's first row yields the
/// RG-relative position an evaluator needs to look up a Lucene bit.
///
/// This replaces the former `PositionMap`, which reconstructed the same
/// information from delivery offsets and therefore had to be kept in lockstep
/// with whatever the decoder skipped.
pub struct RowPositions<'a> {
    /// Delivered `__row_id__` values, or `None` when the scan did not project
    /// the column because no evaluator asked for positions.
    row_ids: Option<&'a datafusion::arrow::array::Int64Array>,
    /// Row count, valid whether or not `row_ids` is present.
    len: usize,
    rg_first_row: i64,
}

impl<'a> RowPositions<'a> {
    /// Wrap the delivered `__row_id__` values for a row group starting at
    /// `rg_first_row` (file-relative).
    pub fn new(row_ids: &'a datafusion::arrow::array::Int64Array, rg_first_row: i64) -> Self {
        Self {
            len: row_ids.len(),
            row_ids: Some(row_ids),
            rg_first_row,
        }
    }

    /// Row positions for a batch whose scan did not project `__row_id__`.
    ///
    /// Only legitimate for evaluators reporting `needs_row_positions() == false`
    /// — their refinement is an expression over the batch's own columns and only
    /// reads `len()`. If an evaluator that *does* consult positions receives
    /// this, that is a contract violation between the evaluator and
    /// `IndexedTableProvider::scan`; callers must reject it rather than let
    /// `rg_position` return `None` for every row, which would silently produce
    /// an empty result. See `RowPositions::require_positions`.
    pub fn unavailable(len: usize) -> Self {
        Self {
            row_ids: None,
            len,
            rg_first_row: 0,
        }
    }

    /// Number of delivered rows.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether positions are actually available.
    ///
    /// Streams call this before handing the value to an evaluator that declared
    /// `needs_row_positions()`, so a missing `__row_id__` column fails loudly
    /// instead of yielding an empty answer.
    #[inline]
    pub fn are_available(&self) -> bool {
        self.row_ids.is_some()
    }

    /// RG-relative position of delivered row `i`.
    ///
    /// `None` when positions were not projected, or when the stored id precedes
    /// this row group — the latter would mean the scan handed us a row from
    /// elsewhere in the file, so callers skip the row rather than guess.
    #[inline]
    pub fn rg_position(&self, i: usize) -> Option<usize> {
        let id = self.row_ids?.value(i);
        usize::try_from(id.checked_sub(self.rg_first_row)?).ok()
    }

    /// Absolute (file-relative) row id of delivered row `i`, when projected.
    #[inline]
    pub fn file_row_id(&self, i: usize) -> Option<i64> {
        Some(self.row_ids?.value(i))
    }

    /// True when the delivered rows form one gapless ascending run, i.e. row
    /// `i` sits at position `rg_position(0) + i`.
    ///
    /// This is the common case — a whole row group, or one contiguous stretch of
    /// a `RowSelection` — and it lets callers index candidate bits by a linear
    /// offset instead of looking up every row. The endpoint check rejects most
    /// gapped batches in O(1) before the interior scan runs.
    pub fn is_contiguous_run(&self) -> bool {
        let Some(row_ids) = self.row_ids else {
            // No positions to be contiguous over; callers must not take the
            // linear fast path.
            return false;
        };
        if self.len <= 1 {
            return true;
        }
        if row_ids.value(self.len - 1) - row_ids.value(0) != self.len as i64 - 1 {
            return false;
        }
        row_ids.values().windows(2).all(|w| w[1] == w[0] + 1)
    }
}

/// Per-row-group bitset producer. Plugs into `IndexedStream`.
pub trait RowGroupBitsetSource: Send + Sync {
    /// Build candidate[pre-scan] bitset for this RG. `None` = skip RG entirely.
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String>;

    /// Produce exact per-batch `BooleanArray` mask for refinement-stage [post-scan]
    /// filtering.
    ///
    /// - `rg_state` is the `context` returned by the last `prefetch_rg` for
    ///   this RG — evaluators downcast it to their own per-RG state type.
    /// - `row_positions` gives, for each delivered row, its RG-relative
    ///   position. It is derived from the `__row_id__` column the scan projects
    ///   (a physical INT64 column whose value is the row's position within the
    ///   file), rebased to the row group. Because the value travels with the
    ///   row, it stays correct no matter what the decoder skipped —
    ///   `RowSelection`, `RowFilter`, or page pruning — which is why evaluators
    ///   no longer need to reconstruct positions from delivery offsets.
    /// - `None` = no refinement mask needed, i.e. the candidate stage's
    ///   `RowSelection` is already authoritative for this batch. Returning it is
    ///   a claim that every delivered row survives, so an evaluator must not do
    ///   so for a selection that was coalesced (see
    ///   [`Self::masks_non_candidates`]).
    ///
    /// The returned mask, when `Some`, is indexed by delivered row and has
    /// length `row_positions.len()`.
    fn on_batch_mask(
        &self,
        rg_state: &dyn Any,
        rg_first_row: i64,
        row_positions: &RowPositions,
        batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String>;

    /// Whether [`Self::on_batch_mask`] rejects rows outside the candidate set
    /// this evaluator returned from [`Self::prefetch_rg`].
    ///
    /// This licenses the *only* transformation that hands the decoder rows the
    /// candidate stage did not ask for: coalescing short skip runs into the
    /// surrounding `select` (see
    /// [`min_skip_run_for`](crate::indexed_table::access_provider)). Coalescing
    /// trades a few over-read rows for a shorter selector list, and is correct
    /// only because refinement then drops those rows by position. An evaluator
    /// that would instead emit an over-read row — because its `on_batch_mask`
    /// returns `None`, so the `RowSelection` *is* the answer — must keep the
    /// selection exactly row-granular.
    ///
    /// Default `false`: an evaluator opts in only when its mask is a function of
    /// the candidate set and not just of the batch's own columns. A residual over
    /// decoded columns is not sufficient on its own — it rejects rows the
    /// predicate excludes, but says nothing about candidate membership.
    ///
    /// Opting in is a claim about the mask, not a guarantee it can be built:
    /// membership is tested by row position, so the caller additionally requires
    /// that the scan deliver `__row_id__` before it coalesces.
    fn masks_non_candidates(&self) -> bool {
        false
    }

    /// Refine *during* decode instead of after it.
    ///
    /// `Some` hands parquet an [`ArrowPredicate`] that is installed in the
    /// scan's `RowFilter`. Parquet then decodes only the predicate's own
    /// columns, applies the mask, and decodes the projected columns for
    /// surviving rows only — so a column the predicate does not read is never
    /// materialized for a row that is about to be dropped. This is strictly
    /// better than post-decode refinement whenever the refinement is selective
    /// and the projection is wider than what the refinement reads.
    ///
    /// `None` (the default) keeps refinement in [`Self::on_batch_mask`], which
    /// runs on fully decoded batches. That is the right choice when the
    /// refinement is *not* selective enough to pay for the `RowFilter`'s extra
    /// decode pass — parquet decodes the predicate columns, then decodes the
    /// projection again for survivors, so a predicate that keeps nearly
    /// everything pays twice for nearly nothing.
    ///
    /// Either way the mask itself comes from [`Self::on_batch_mask`]; this only
    /// chooses *when* parquet asks for it. One
    /// [`RefinementPredicate`](decode_predicate::RefinementPredicate) wraps any
    /// evaluator, so nothing here builds a predicate.
    ///
    /// Requires `needs_row_positions()`: the predicate identifies rows by
    /// `__row_id__`, since a row's position in the batch is meaningless once
    /// earlier predicates have dropped rows.
    ///
    /// [`ArrowPredicate`]: datafusion::parquet::arrow::arrow_reader::ArrowPredicate
    fn refines_during_decode(&self) -> bool {
        false
    }

    /// Names of the columns [`Self::on_batch_mask`] reads from the batch.
    ///
    /// Only consulted when [`Self::refines_during_decode`] is `true`: an
    /// `ArrowPredicate` is handed *only* the columns its `ProjectionMask` names,
    /// so anything the refinement evaluates must be listed here or the
    /// expression will fail against a batch that lacks it. `__row_id__` is added
    /// by the caller and need not be listed.
    ///
    /// Empty means the refinement reads no data columns — it works from row
    /// positions alone, as the pure-collector shapes do.
    fn refinement_columns(&self) -> Vec<String> {
        Vec::new()
    }

    /// Whether `on_batch_mask` consults [`RowPositions`], i.e. whether the scan
    /// must project `__row_id__`.
    ///
    /// `true` for evaluators that refine against external per-row state keyed by
    /// row position — the Lucene-backed shapes. `false` (the default) for
    /// evaluators whose refinement is a plain expression over the delivered
    /// batch, which is the predicate-only path; those scans read no extra column.
    fn needs_row_positions(&self) -> bool {
        false
    }

    /// Whether this evaluator requires parquet's `with_predicate` pushdown
    /// to be OFF. `true` when the evaluator would otherwise evaluate the same
    /// residual twice — once during decode and once in `on_batch_mask`.
    ///
    /// Note this is no longer about index alignment: since refinement reads
    /// positions from the delivered `__row_id__` column, a RowFilter dropping
    /// rows mid-decode cannot misalign anything. The remaining reason to forbid
    /// pushdown is duplicated work / ownership of exactness.
    ///
    /// Default `false`: pushdown decided by the stream's base policy.
    /// Overridden to `true` by evaluators that must see the complete
    /// RowSelection-delivered rowset (e.g.
    /// `SingleCollectorEvaluator` when it owns the residual filter in
    /// `on_batch_mask`, or `TreeBitsetSource` which always refines).
    fn forbid_parquet_pushdown(&self) -> bool {
        false
    }
}

/// Pre-decode row access selected by an evaluator.
pub enum PrefetchedRgRows {
    /// Candidate doc IDs, RG-relative (bit 0 = first row of the RG).
    /// Tree evaluators use this representation because refinement and
    /// row-ID emission need random access to original row positions.
    Bitmap(Arc<RoaringBitmap>),
    /// Candidate doc IDs as packed u64 words, RG-relative. Collector
    /// evaluators use this: Java already returns matches as a packed bitset,
    /// and keeping it packed lets selection building, mask building, and
    /// batch masks work word-at-a-time instead of per set bit — the roaring
    /// round-trip dominated non-selective collector scans.
    DenseBitmap(Arc<crate::indexed_table::dense_bits::DenseBitset>),
    /// A page-granular Parquet selection. Predicate-only scans can pass the
    /// page pruner's result straight to the decoder when row positions are not
    /// needed, avoiding an intermediate bitmap and packed mask.
    Selection {
        selection: RowSelection,
        selected_rows: usize,
    },
}

impl PrefetchedRgRows {
    pub fn matched_rows(&self) -> usize {
        match self {
            Self::Bitmap(candidates) => candidates.len() as usize,
            Self::DenseBitmap(candidates) => candidates.count_ones(),
            Self::Selection { selected_rows, .. } => *selected_rows,
        }
    }

    pub fn bitmap(&self) -> Option<&RoaringBitmap> {
        match self {
            Self::Bitmap(candidates) => Some(candidates.as_ref()),
            Self::DenseBitmap(..) | Self::Selection { .. } => None,
        }
    }

    /// Candidate positions as a `RoaringBitmap`, converting if necessary.
    /// Test/diagnostic helper — hot paths must match on the variant instead.
    pub fn to_roaring(&self) -> Option<RoaringBitmap> {
        match self {
            Self::Bitmap(candidates) => Some(candidates.as_ref().clone()),
            Self::DenseBitmap(candidates) => Some(candidates.to_roaring()),
            Self::Selection { .. } => None,
        }
    }
}

/// Output of `prefetch_rg`.
pub struct PrefetchedRg {
    pub rows: PrefetchedRgRows,
    /// Time spent producing the bitset (nanoseconds). For metrics.
    pub eval_nanos: u64,
    /// Opaque per-RG state threaded to `on_batch_mask` via `rg_state: &dyn Any`.
    /// Evaluators downcast to their own concrete type.
    pub context: Box<dyn Any + Send + Sync>,
}

impl PrefetchedRg {
    /// Helper for evaluators with no per-RG state (e.g. the single-collector
    /// path, which doesn't do refinement [post-scan]).
    pub fn without_context(candidates: RoaringBitmap, eval_nanos: u64) -> Self {
        Self {
            rows: PrefetchedRgRows::Bitmap(Arc::new(candidates)),
            eval_nanos,
            context: Box::new(()),
        }
    }
}

/// Multi-filter tree path: pluggable tree evaluator + leaf bitmap source
///
/// Context for evaluating a tree against one row group.
#[derive(Debug, Clone)]
pub struct RgEvalContext {
    pub rg_idx: usize,
    pub rg_first_row: i64,
    pub rg_num_rows: i64,
    pub min_doc: i32,
    pub max_doc: i32,
    /// Candidate-stage leaf-reorder cost for `ResolvedNode::Predicate`.
    /// Plumbed from `DatafusionQueryConfig`; read on the hot path.
    pub cost_predicate: u32,
    /// Candidate-stage leaf-reorder cost for `ResolvedNode::Collector`.
    pub cost_collector: u32,
    /// Narrowed doc-id ranges for Collector FFM calls. Computed by the
    /// AND evaluator from the accumulator bitmap after earlier children
    /// shrink the candidate set.
    /// `None` = no narrowing (use full `[min_doc, max_doc)`).
    /// `Some(ranges)` = call collector once per range.
    pub collector_call_ranges: Option<Vec<(i32, i32)>>,
    /// Controls how the AND evaluator narrows collector ranges from the
    /// accumulator bitmap.
    pub collector_strategy: CollectorCallStrategy,
}

/// Candidate-stage output of a `TreeEvaluator`. `candidates` is a superset
/// bitmap of doc IDs relative to `ctx.min_doc`; `per_leaf` maps leaf
/// identity (implementation-defined — pointer or index) to that leaf's
/// bitmap in the same domain, which the refinement stage looks up per
/// batch.
pub struct TreePrefetch {
    pub candidates: RoaringBitmap,
    pub per_leaf: Vec<(usize, RoaringBitmap)>,
    /// Anchor doc ID (same as `ctx.min_doc` at prefetch time) so the
    /// refinement stage can convert batch offsets to doc IDs.
    pub min_doc: i32,
}

/// Produces per-leaf bitmaps for one row group.
///
/// Identified by DFS index in `tree`. Bitmap domain is `[ctx.min_doc, ctx.max_doc)`.
pub trait LeafBitmapSource: Send + Sync {
    fn leaf_bitmap(
        &self,
        tree: &ResolvedNode,
        leaf_dfs_index: usize,
        ctx: &RgEvalContext,
    ) -> Result<RoaringBitmap, String>;
}

/// Pluggable tree-evaluation strategy. The algorithm that walks the tree,
/// combines per-leaf bitmaps, produces candidates + per-batch masks.
pub trait TreeEvaluator: Send + Sync {
    /// Candidate stage: walk the tree for one row group and produce a
    /// superset RoaringBitmap of candidate doc IDs plus the per-leaf
    /// bitmap side-table that the refinement stage will read.
    ///
    /// `pruning_predicates` maps each `Predicate(expr)` leaf (keyed by
    /// its
    /// `Arc::as_ptr` identity) to a pre-built `PruningPredicate`. Empty
    /// map = no page-level predicate pruning; each Predicate leaf falls
    /// back to "every row is a candidate" (safe, identity for the
    /// candidate stage).
    fn prefetch(
        &self,
        tree: &ResolvedNode,
        ctx: &RgEvalContext,
        leaves: &dyn LeafBitmapSource,
        page_pruner: &PagePruner,
        pruning_predicates: &HashMap<usize, Arc<PruningPredicate>>,
        page_prune_metrics: Option<&PagePruneMetrics>,
        stats_prune_tree: Option<&StatsPruneTree>,
        rg_index_to_pos: &HashMap<usize, usize>,
    ) -> Result<TreePrefetch, String>;

    /// Refinement stage: produce the exact per-row `BooleanArray` for one
    /// record batch, consuming the candidate-stage `state` for the RG this
    /// batch belongs to.
    ///
    /// `row_positions` gives each delivered row's RG-relative position, taken
    /// from the projected `__row_id__` column.
    fn on_batch(
        &self,
        tree: &ResolvedNode,
        state: &TreePrefetch,
        batch: &RecordBatch,
        rg_first_row: i64,
        row_positions: &RowPositions,
    ) -> Result<BooleanArray, String>;
}

/// Composes a `TreeEvaluator` + `LeafBitmapSource` + `PagePruner` + resolved
/// tree into a `RowGroupBitsetSource`.
///
/// Usage:
/// ```ignore
/// let source = TreeBitsetSource {
///     tree: Arc::new(resolved),
///     evaluator: Arc::new(BitmapTreeEvaluator),        // or JavaTreeEvaluator
///     leaves: Arc::new(CollectorLeafBitmaps::without_metrics()),           // or ParquetStatsLeaves
///     page_pruner: Arc::new(pruner),
/// };
/// ```
///
/// # Batch projection requirement
///
/// The refinement stage evaluates `Predicate` leaves via Arrow cmp kernels
/// on the current `RecordBatch`. Every column referenced by a
/// `ResolvedNode::Predicate` in the tree **must be present in the batch**
/// at eval time, i.e. the physical plan's projection must include
/// predicate columns, not just the final
/// SELECT list. In production, substrait plans emitted by the planner project
/// predicate columns as part of the filter node, so this is naturally
/// satisfied. Test harnesses that bypass substrait and select only output
/// columns must explicitly expand the SELECT to include predicate columns.
pub struct TreeBitsetSource {
    pub tree: Arc<ResolvedNode>,
    pub evaluator: Arc<dyn TreeEvaluator>,
    pub leaves: Arc<dyn LeafBitmapSource>,
    pub page_pruner: Arc<PagePruner>,
    /// Pre-extracted from `DatafusionQueryConfig` at source-construction
    /// time so `prefetch_rg` doesn't need an `Arc` deref on the hot path.
    pub cost_predicate: u32,
    pub cost_collector: u32,
    /// Max number of Collector leaves whose bitmaps are produced in
    /// parallel per RG prefetch. 1 = sequential (preserves short-circuit
    /// savings). Higher values trade short-circuit savings for latency
    /// reduction on multi-collector trees; bounded by caller's config.
    pub max_collector_parallelism: usize,
    /// Per-predicate `PruningPredicate` cache, keyed by
    /// `Arc::as_ptr(resolved_predicate) as usize`. Built once per query at
    /// dispatch time by the caller. Empty = page-level predicate pruning
    /// disabled (the tree path still works, each Predicate leaf falls
    /// back to "every row is a candidate").
    pub pruning_predicates: Arc<HashMap<usize, Arc<PruningPredicate>>>,
    /// Counters recorded by `page_pruner.prune_rg` at each Predicate
    /// leaf in the tree walk. Populated from the stream's
    /// `PartitionMetrics` at dispatch time.
    pub page_prune_metrics: Option<PagePruneMetrics>,
    /// Controls how the AND evaluator narrows collector doc ranges.
    /// `TightenOuterBounds` (default) uses a single `[min, max)` range.
    /// `FullRange` disables narrowing. `PageRangeSplit` is not
    /// recommended here — multiple FFM calls per collector per RG can
    /// be expensive in multi-collector trees.
    pub collector_strategy: CollectorCallStrategy,
    /// Precomputed per-subtree RG match vectors. Built once at construction.
    pub stats_prune_tree: Option<Arc<StatsPruneTree>>,
    /// Reverse map: absolute RG index → position in `rg_can_match` vectors.
    pub rg_index_to_pos: HashMap<usize, usize>,
}

impl RowGroupBitsetSource for TreeBitsetSource {
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let t = Instant::now();

        // RG-level early-exit: precomputed from column stats at construction.
        if let Some(ref ann) = self.stats_prune_tree {
            if let Some(&pos) = self.rg_index_to_pos.get(&rg.index) {
                if let Some(&false) = ann.rg_can_match.get(pos) {
                    native_bridge_common::log_debug!(
                        "BitmapTree: skipping RG {} — pruned by RG-level stats",
                        rg.index
                    );
                    return Ok(None);
                }
            }
        }

        let ctx = RgEvalContext {
            rg_idx: rg.index,
            rg_first_row: rg.first_row,
            rg_num_rows: rg.num_rows,
            min_doc,
            max_doc,
            cost_predicate: self.cost_predicate,
            cost_collector: self.cost_collector,
            collector_call_ranges: None,
            collector_strategy: self.collector_strategy,
        };

        // Optional: materialise all Collector leaves in parallel before
        // running the tree walk. Preserves correctness; sacrifices AND/OR
        // short-circuit savings (all collectors run even if an earlier
        // AND child already emptied the accumulator). Governed by
        // `max_collector_parallelism`: 1 = sequential (today).
        let precomputed = if self.max_collector_parallelism > 1 {
            Some(precompute_collector_leaves(
                &self.tree,
                &ctx,
                &*self.leaves,
                self.max_collector_parallelism,
            )?)
        } else {
            None
        };

        // Use the precomputed cache as the LeafBitmapSource if present;
        // otherwise delegate directly to the original source (sequential).
        let leaves_ref: &dyn LeafBitmapSource = match &precomputed {
            Some(c) => c,
            None => &*self.leaves,
        };

        let prefetch = self
            .evaluator
            .prefetch(
                &self.tree,
                &ctx,
                leaves_ref,
                &self.page_pruner,
                &self.pruning_predicates,
                // Don't pass metrics here — per-leaf prune_rg calls would
                // inflate counts. We compute final page-level metrics below
                // after the bitmap tree is fully resolved.
                None,
                self.stats_prune_tree.as_deref(),
                &self.rg_index_to_pos,
            )
            .map_err(|e| format!("TreeBitsetSource::prefetch_rg(rg={}): {}", rg.index, e))?;
        if prefetch.candidates.is_empty() {
            // All candidates pruned — record that every page was pruned.
            if let Some(ref m) = self.page_prune_metrics {
                if let Some(page_row_counts) = self.page_pruner.page_row_counts(rg.index) {
                    let num_pages = page_row_counts.len();
                    if let Some(ref c) = m.pages_total {
                        c.add(num_pages);
                    }
                    if let Some(ref c) = m.pages_pruned {
                        c.add(num_pages);
                    }
                }
            }
            return Ok(None);
        }
        // `prefetch.candidates` is in min_doc-relative space [0, max_doc - min_doc).
        // `PrefetchedRg.candidates` is in RG-relative space [0, rg.num_rows).
        // anchor = (min_doc - rg.first_row) shifts each relative bit.
        //
        // Fast path: if `anchor == 0`, clone directly — no shift
        // needed. Otherwise walk the source in sorted order and
        // coalesce consecutive bits into `insert_range` calls so we
        // get one O(log n) call per run instead of O(1) per bit.
        let anchor = (min_doc as i64) - rg.first_row;
        let rg_candidates = if anchor == 0 {
            prefetch.candidates.clone()
        } else {
            let mut rg_candidates = RoaringBitmap::new();
            let mut run_start: Option<u32> = None;
            let mut run_end: u32 = 0; // inclusive
            let flush = |bm: &mut RoaringBitmap, start: u32, end_inclusive: u32| {
                // Range API is half-open; end_inclusive+1 handles the
                // edge case at u32::MAX via saturating add (roaring
                // clamps at u32::MAX internally).
                let end = end_inclusive.saturating_add(1);
                bm.insert_range(start..end);
            };
            for rel in prefetch.candidates.iter() {
                let shifted = rel as i64 + anchor;
                if shifted < 0 || shifted > u32::MAX as i64 {
                    continue;
                }
                let v = shifted as u32;
                match run_start {
                    None => {
                        run_start = Some(v);
                        run_end = v;
                    }
                    Some(_) if v == run_end + 1 => {
                        run_end = v;
                    }
                    Some(s) => {
                        flush(&mut rg_candidates, s, run_end);
                        run_start = Some(v);
                        run_end = v;
                    }
                }
            }
            if let Some(s) = run_start {
                flush(&mut rg_candidates, s, run_end);
            }
            rg_candidates
        };

        // Compute final page-level pruning metrics from the resolved
        // bitmap. A page is "pruned" if zero candidate bits fall within
        // its row range; "kept" otherwise. This reflects the actual
        // page-level decision after AND/OR/NOT combination, not the
        // per-leaf intermediate results.
        if let Some(ref m) = self.page_prune_metrics {
            if let Some(page_row_counts) = self.page_pruner.page_row_counts(rg.index) {
                let num_pages = page_row_counts.len();
                let mut pruned = 0usize;
                let mut row_offset = 0u32;
                for &count in &page_row_counts {
                    let page_end = row_offset + count as u32;
                    if rg_candidates.range(row_offset..page_end).next().is_none() {
                        pruned += 1;
                    }
                    row_offset = page_end;
                }
                if let Some(ref c) = m.pages_total {
                    c.add(num_pages);
                }
                if let Some(ref c) = m.pages_pruned {
                    c.add(pruned);
                }
            }
        }

        Ok(Some(PrefetchedRg {
            rows: PrefetchedRgRows::Bitmap(Arc::new(rg_candidates)),
            eval_nanos: t.elapsed().as_nanos() as u64,
            context: Box::new(prefetch),
        }))
    }

    fn on_batch_mask(
        &self,
        rg_state: &dyn Any,
        rg_first_row: i64,
        row_positions: &RowPositions,
        batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        let state = rg_state.downcast_ref::<TreePrefetch>().ok_or_else(|| {
            "TreeBitsetSource::on_batch_mask: rg_state is not TreePrefetch".to_string()
        })?;
        let mask =
            self.evaluator
                .on_batch(&self.tree, state, batch, rg_first_row, row_positions)?;
        Ok(Some(mask))
    }

    /// Collector leaves are looked up by row position, so the tree path always
    /// needs `__row_id__`.
    fn needs_row_positions(&self) -> bool {
        true
    }

    /// The tree walk re-evaluates every leaf against the delivered rows,
    /// resolving `Collector` leaves by row position out of the per-leaf bitmaps
    /// the candidate stage stored. A row the candidate stage did not select
    /// therefore misses those bitmaps and the mask rejects it — so the tree path
    /// tolerates over-read rows.
    fn masks_non_candidates(&self) -> bool {
        true
    }

    /// BitmapTree owns the exact answer in `on_batch_mask`, so a parquet
    /// RowFilter would only duplicate work. Row alignment is no longer a
    /// concern — Collector lookups use the delivered `__row_id__` values, which
    /// stay correct however many rows the decoder dropped. The remaining hard
    /// reason is that a pushdown predicate reaching us via `scan(filters)` can
    /// contain the `index_filter(...)` UDF marker, whose body panics.
    fn forbid_parquet_pushdown(&self) -> bool {
        true
    }

    /// The tree path always refines: the candidate stage yields a superset and
    /// `on_batch_mask` produces the exact answer. Deciding that during decode
    /// keeps the projection from being materialized for superset rows the
    /// refinement rejects.
    fn refines_during_decode(&self) -> bool {
        true
    }

    /// Every column any `Predicate` leaf of the tree references. Collector
    /// leaves contribute nothing — they are resolved from row positions.
    fn refinement_columns(&self) -> Vec<String> {
        fn walk(node: &ResolvedNode, out: &mut HashSet<String>) {
            match node {
                ResolvedNode::Predicate(expr) => {
                    for column in datafusion::physical_expr::utils::collect_columns(expr) {
                        out.insert(column.name().to_string());
                    }
                }
                ResolvedNode::And(children) | ResolvedNode::Or(children) => {
                    children.iter().for_each(|c| walk(c, out))
                }
                ResolvedNode::Not(inner) => walk(inner, out),
                ResolvedNode::Collector { .. } => {}
                ResolvedNode::DelegationPossible { original_expr, .. } => {
                    for column in datafusion::physical_expr::utils::collect_columns(original_expr) {
                        out.insert(column.name().to_string());
                    }
                }
            }
        }
        let mut out = HashSet::new();
        walk(&self.tree, &mut out);
        out.into_iter().collect()
    }
}

/// LeafBitmapSource that serves from a pre-populated map keyed by
/// `Arc::as_ptr(collector)`. Falls back to the inner source for leaves
/// not in the map (shouldn't happen in practice — we populate the map
/// with every Collector leaf in the tree before invoking the evaluator).
struct PrecomputedLeafCache<'a> {
    map: HashMap<usize, RoaringBitmap>,
    fallback: &'a dyn LeafBitmapSource,
}

impl<'a> LeafBitmapSource for PrecomputedLeafCache<'a> {
    fn leaf_bitmap(
        &self,
        tree: &ResolvedNode,
        leaf_dfs_index: usize,
        ctx: &RgEvalContext,
    ) -> Result<RoaringBitmap, String> {
        if let ResolvedNode::Collector { collector, .. } = tree {
            let key = Arc::as_ptr(collector) as *const () as usize;
            if let Some(bm) = self.map.get(&key) {
                return Ok(bm.clone());
            }
        }
        self.fallback.leaf_bitmap(tree, leaf_dfs_index, ctx)
    }
}

/// Walk the resolved tree and collect (key, collector-node-reference)
/// pairs for every Collector leaf, in DFS order (matching the
/// evaluator's walk order — we don't care about order beyond determinism).
/// Duplicates (same Arc pointing at the same collector instance) are
/// deduplicated by `Arc::as_ptr` so we don't call Lucene twice for the
/// same leaf.
fn collect_unique_collector_nodes<'a>(
    node: &'a ResolvedNode,
    out: &mut Vec<(usize, &'a ResolvedNode)>,
    seen: &mut HashSet<usize>,
) {
    match node {
        ResolvedNode::And(children) | ResolvedNode::Or(children) => {
            for c in children {
                collect_unique_collector_nodes(c, out, seen);
            }
        }
        ResolvedNode::Not(c) => collect_unique_collector_nodes(c, out, seen),
        ResolvedNode::Collector { collector, .. } => {
            let key = Arc::as_ptr(collector) as *const () as usize;
            if seen.insert(key) {
                out.push((key, node));
            }
        }
        ResolvedNode::Predicate(_) => {}
        ResolvedNode::DelegationPossible { .. } => {
            // Invariant: planner drops performance peers under OR/NOT before
            // fragment conversion, so DelegationPossible should never reach the
            // Tree-path evaluator. Reaching this is a planner-contract violation.
            unimplemented!(
                "invariant violation: DelegationPossible reached \
                 collect_unique_collector_nodes. Planner must drop performance peers \
                 under OR/NOT before fragment conversion."
            )
        }
    }
}

/// Materialise all Collector leaves of `tree` by running their
/// `LeafBitmapSource::leaf_bitmap` calls in parallel via `std::thread::scope`,
/// bounded by `max_parallel`. Returns a cache keyed by `Arc::as_ptr(collector)`.
///
/// Uses an `Arc<AtomicUsize>`-driven round-robin over pre-spawned worker
/// threads so we never exceed `max_parallel` concurrent Lucene calls.
/// On error, returns the first error encountered.
fn precompute_collector_leaves<'a>(
    tree: &'a ResolvedNode,
    ctx: &RgEvalContext,
    leaves: &'a dyn LeafBitmapSource,
    max_parallel: usize,
) -> Result<PrecomputedLeafCache<'a>, String> {
    let mut collectors: Vec<(usize, &ResolvedNode)> = Vec::new();
    let mut seen = HashSet::new();
    collect_unique_collector_nodes(tree, &mut collectors, &mut seen);

    // Zero or one collector → no benefit from parallelism, fall back to
    // an empty cache (evaluator will use the fallback synchronously).
    if collectors.len() <= 1 {
        return Ok(PrecomputedLeafCache {
            map: HashMap::new(),
            fallback: leaves,
        });
    }

    let n = collectors.len();
    let parallel = max_parallel.min(n).max(1);

    // Bounded parallelism via std::thread::scope + a work queue Mutex.
    // Each worker pulls the next collector to evaluate, calls
    // leaf_bitmap, writes result into a shared Vec<Option<Result<...>>>
    // at the collector's index.
    let mut results: Vec<Option<Result<RoaringBitmap, String>>> = (0..n).map(|_| None).collect();
    let next_idx = std::sync::atomic::AtomicUsize::new(0);
    let results_mutex = std::sync::Mutex::new(&mut results);

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(parallel);
        for _worker in 0..parallel {
            let collectors_ref = &collectors;
            let leaves_ref = leaves;
            let ctx_ref = ctx;
            let next_idx_ref = &next_idx;
            let results_mutex_ref = &results_mutex;
            handles.push(scope.spawn(move || {
                loop {
                    let i = next_idx_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= collectors_ref.len() {
                        break;
                    }
                    let (_key, node) = collectors_ref[i];
                    // Use i as the leaf_dfs_index — the cache doesn't
                    // use it for lookup (keys by Arc::as_ptr), so any
                    // stable value works.
                    let result = leaves_ref.leaf_bitmap(node, i, ctx_ref);
                    let mut guard = results_mutex_ref.lock().unwrap();
                    guard[i] = Some(result);
                }
            }));
        }
        // Scope ensures all threads complete before returning.
        for h in handles {
            let _ = h.join();
        }
    });

    // Assemble results. Fail fast on the first error.
    let mut map = HashMap::with_capacity(n);
    for (i, slot) in results.into_iter().enumerate() {
        let bm =
            slot.ok_or_else(|| format!("precompute: worker did not populate slot {}", i))??;
        map.insert(collectors[i].0, bm);
    }

    Ok(PrecomputedLeafCache {
        map,
        fallback: leaves,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::bool_tree::ResolvedNode;
    use crate::indexed_table::index::RowGroupDocsCollector;
    use crate::indexed_table::page_pruner::PagePruner;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;

    fn empty_pruner() -> Arc<PagePruner> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![0i32; 4]))],
        )
        .unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let meta = ArrowReaderMetadata::load(
            &tmp.reopen().unwrap(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        Arc::new(PagePruner::new(meta.schema(), meta.metadata().clone()))
    }

    /// Leaf source that returns empty bitmaps — enough to compose a
    /// TreeBitsetSource for trait-level tests.
    struct NoopLeaves;
    impl LeafBitmapSource for NoopLeaves {
        fn leaf_bitmap(
            &self,
            _tree: &ResolvedNode,
            _idx: usize,
            _ctx: &RgEvalContext,
        ) -> Result<roaring::RoaringBitmap, String> {
            Ok(roaring::RoaringBitmap::new())
        }
    }

    /// Evaluator that mirrors the shape of BitmapTreeEvaluator for the trait
    /// needs_row_mask test (we don't import BitmapTreeEvaluator here to avoid
    /// a circular dependency with the bitmap_tree module's own tests).
    struct NoopTreeEvaluator;
    impl TreeEvaluator for NoopTreeEvaluator {
        fn prefetch(
            &self,
            _tree: &ResolvedNode,
            _ctx: &RgEvalContext,
            _leaves: &dyn LeafBitmapSource,
            _page_pruner: &PagePruner,
            _pruning_predicates: &HashMap<usize, Arc<PruningPredicate>>,
            _page_prune_metrics: Option<&PagePruneMetrics>,
            _stats_prune_tree: Option<&StatsPruneTree>,
            _rg_index_to_pos: &HashMap<usize, usize>,
        ) -> Result<TreePrefetch, String> {
            Ok(TreePrefetch {
                candidates: roaring::RoaringBitmap::new(),
                per_leaf: Vec::new(),
                min_doc: 0,
            })
        }
        fn on_batch(
            &self,
            _tree: &ResolvedNode,
            _state: &TreePrefetch,
            _batch: &RecordBatch,
            _rg_first_row: i64,
            row_positions: &RowPositions,
        ) -> Result<BooleanArray, String> {
            Ok(BooleanArray::from(vec![false; row_positions.len()]))
        }
    }
}
