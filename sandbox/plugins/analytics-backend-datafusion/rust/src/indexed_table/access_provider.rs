/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Bridges the indexed evaluator to DataFusion's [`RowGroupAccessProvider`].
//!
//! DataFusion owns the decoder, byte scheduling, projection, limits and
//! metrics. This module supplies the one thing DataFusion cannot derive from
//! parquet alone: which rows of each row group the external index says are
//! candidates.
//!
//! ```text
//!   boundary n   ─▶ access_for([n, n+1, ..])
//!                     ├─ await prefetch(n)   (dispatched at boundary n-1)
//!                     ├─ start prefetch(n+1) ── overlaps decode of n ──┐
//!                     └─ return [Selection(n)]                         │
//!   decode RG n  ────────────────────────────────────────────────────── ┘
//! ```
//!
//! The prefetch for row group *n + 1* is dispatched before the decision for
//! *n* is returned, so the Lucene/FFM evaluation of *n + 1* overlaps the decode
//! of *n* — the same overlap the hand-written driver got from its
//! `Poll::Pending` + waker loop, without needing one.
//!
//! Per-row-group evaluator state (`PrefetchedRg::context`) cannot travel
//! through the DataFusion interface, which carries only access decisions. It is
//! parked in [`RgContextStore`], keyed by row-group index, and picked up by the
//! refinement stage on the other side of the decoder. That store is private to
//! OpenSearch; DataFusion never sees it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::parquet::{RowGroupAccess, RowGroupAccessProvider};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion_common::DataFusionError;

use super::eval::RowGroupBitsetSource;
use super::metrics::StreamMetrics;
use super::row_selection::build_row_selection_with_min_skip_run;
use super::stream::RowGroupInfo;
use crate::datafusion_query_config::FilterStrategy;

/// Per-row-group evaluator state, published by the access provider and read by
/// the refinement stage.
///
/// `access_for` runs the evaluator for a row group and produces two things: a
/// selection, which DataFusion's interface can carry, and an opaque context for
/// the refinement, which it cannot. The context is published here instead.
///
/// Refinement reads an entry once per *batch*, not once per row group, so
/// entries are borrowed rather than claimed. They are dropped when the scan
/// ends; the map is bounded by the chunk's row-group count.
#[derive(Debug, Default)]
pub(super) struct RgContextStore {
    inner: Mutex<HashMap<usize, Arc<RgContext>>>,
}

/// What refinement needs for one row group, beyond the batch itself.
pub(super) struct RgContext {
    /// Opaque per-RG evaluator state, downcast by `on_batch_mask`.
    context: Box<dyn std::any::Any + Send + Sync>,
    /// First row of the row group within the file, for `RowPositions`.
    pub(super) first_row: i64,
}

impl RgContext {
    /// The evaluator's own per-row-group state.
    pub(super) fn state(&self) -> &(dyn std::any::Any + Send + Sync) {
        self.context.as_ref()
    }
}

impl std::fmt::Debug for RgContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RgContext")
            .field("first_row", &self.first_row)
            .finish_non_exhaustive()
    }
}

impl RgContextStore {
    pub(super) fn new() -> Self {
        Self::default()
    }

    fn put(&self, rg_index: usize, context: RgContext) {
        self.inner
            .lock()
            .unwrap()
            .insert(rg_index, Arc::new(context));
    }

    /// Borrow the context for `rg_index`, leaving it in place for later batches
    /// of the same row group.
    pub(super) fn peek(&self, rg_index: usize) -> Option<Arc<RgContext>> {
        self.inner.lock().unwrap().get(&rg_index).map(Arc::clone)
    }
}

/// Outcome of evaluating one row group's index.
enum Evaluated {
    /// Candidate rows, with the evaluator state refinement will need.
    Candidates {
        access: RowGroupAccess,
        context: RgContext,
        eval_nanos: u64,
        matched_rows: usize,
        total_rows: usize,
    },
    /// No candidates — the row group contributes nothing and is not read.
    Empty,
}

/// Supplies indexed row-group access decisions to DataFusion's parquet scan.
///
/// One per file scan, created by [`IndexedAccessProviderFactory`].
pub(super) struct IndexedAccessProvider {
    evaluator: Arc<dyn RowGroupBitsetSource>,
    /// Chunk row groups in the order the decoder reads them.
    row_groups: Vec<RowGroupInfo>,
    /// Prefetch dispatched at the previous boundary, awaited at this one.
    pending: Option<(usize, tokio::task::JoinHandle<Result<Evaluated, String>>)>,
    doc_range: Option<(i32, i32)>,
    contexts: Arc<RgContextStore>,
    metrics: StreamMetrics,
    /// Runtime dynamic filter (TopK / join), when one was pushed down. Consulted
    /// at each boundary so a row group the tightening threshold provably excludes
    /// is skipped without running the index or reading any bytes.
    dynamic_pruner: Option<super::dynamic_filter::DynamicRgPruner>,
    /// Row groups excluded by the dynamic filter *before* their index evaluation
    /// was dispatched. Recorded so the skip is attributed to the prefetch phase
    /// once, rather than counted again as a poll-phase prune when the decoder
    /// reaches the row group.
    pruned_at_prefetch: std::collections::HashSet<usize>,
    cancellation_token: Option<tokio_util::sync::CancellationToken>,
    /// How dense a row group's candidates must be before short skips are
    /// coalesced. See [`min_skip_run_for`].
    granularity: SelectionGranularity,
}

impl std::fmt::Debug for IndexedAccessProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexedAccessProvider")
            .field("row_groups", &self.row_groups.len())
            .field("pending", &self.pending.as_ref().map(|(rg, _)| *rg))
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Evaluated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Candidates { matched_rows, .. } => f
                .debug_struct("Candidates")
                .field("matched_rows", matched_rows)
                .finish_non_exhaustive(),
            Self::Empty => f.write_str("Empty"),
        }
    }
}

impl IndexedAccessProvider {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        evaluator: Arc<dyn RowGroupBitsetSource>,
        row_groups: Vec<RowGroupInfo>,
        doc_range: Option<(i32, i32)>,
        contexts: Arc<RgContextStore>,
        metrics: StreamMetrics,
        dynamic_pruner: Option<super::dynamic_filter::DynamicRgPruner>,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
        granularity: SelectionGranularity,
    ) -> Self {
        Self {
            evaluator,
            row_groups,
            pending: None,
            doc_range,
            contexts,
            metrics,
            dynamic_pruner,
            pruned_at_prefetch: std::collections::HashSet::new(),
            cancellation_token,
            granularity,
        }
    }

    /// Whether the dynamic filter's tightening so far proves `rg_index` cannot
    /// contribute. Always `false` when no dynamic filter was pushed, and
    /// conservative otherwise: it only skips on proof.
    fn dynamically_excluded(&mut self, rg_index: usize, metadata: &ParquetMetaData) -> bool {
        self.dynamic_pruner
            .as_mut()
            .is_some_and(|pruner| pruner.should_prune_rg(metadata, rg_index))
    }

    /// Start the *next* row group's index evaluation, so it overlaps the decode
    /// of the one just answered for.
    ///
    /// A row group the dynamic filter already excludes is not dispatched at all:
    /// that is the prefetch-phase win, saving the Lucene/FFM evaluation as well
    /// as the decode. The exclusion is recorded so the boundary-phase check
    /// attributes the skip once rather than twice.
    fn dispatch_next(&mut self, pending: &[usize], metadata: &ParquetMetaData) {
        let Some(&next) = pending.get(1) else {
            return;
        };
        if self.is_cancelled() {
            return;
        }
        if self.dynamically_excluded(next, metadata) {
            if let Some(ref count) = self.metrics.dynamic_filter_rg_pruned_at_prefetch {
                count.add(1);
            }
            self.pruned_at_prefetch.insert(next);
            return;
        }
        self.dispatch(next);
    }

    fn is_cancelled(&self) -> bool {
        self.cancellation_token
            .as_ref()
            .is_some_and(|t| t.is_cancelled())
    }

    /// Run the evaluator for one row group. Executed on a blocking thread: the
    /// Lucene/FFM call is CPU-bound and must not occupy an async worker.
    fn evaluate(
        evaluator: &Arc<dyn RowGroupBitsetSource>,
        rg: &RowGroupInfo,
        doc_range: Option<(i32, i32)>,
        cancellation_token: Option<&tokio_util::sync::CancellationToken>,
        granularity: &SelectionGranularity,
    ) -> Result<Evaluated, String> {
        // The job may have been queued before cancellation and started after.
        if cancellation_token.is_some_and(|t| t.is_cancelled()) {
            return Err("query cancelled".to_string());
        }

        let mut min_doc = rg.first_row as i32;
        let mut max_doc = (rg.first_row + rg.num_rows) as i32;
        if let Some((range_min, range_max)) = doc_range {
            min_doc = min_doc.max(range_min);
            max_doc = max_doc.min(range_max);
            if min_doc >= max_doc {
                return Ok(Evaluated::Empty);
            }
        }

        let started = Instant::now();
        let Some(prefetched) = evaluator.prefetch_rg(rg, min_doc, max_doc)? else {
            return Ok(Evaluated::Empty);
        };
        let eval_nanos = started.elapsed().as_nanos() as u64;

        let matched_rows = prefetched.rows.matched_rows();
        let total_rows = rg.num_rows as usize;
        let selection = selection_from_rows(
            prefetched.rows,
            total_rows,
            evaluator.masks_non_candidates(),
            granularity,
        );

        // A selection that keeps nothing is a skip: DataFusion must not fetch
        // bytes for a row group with no surviving rows.
        if !selection.selects_any() {
            return Ok(Evaluated::Empty);
        }

        Ok(Evaluated::Candidates {
            access: RowGroupAccess::Selection(selection),
            context: RgContext {
                context: prefetched.context,
                first_row: rg.first_row,
            },
            eval_nanos: prefetched.eval_nanos.max(eval_nanos),
            matched_rows,
            total_rows,
        })
    }

    /// Dispatch the evaluation of `rg_index` to a blocking thread.
    fn dispatch(&mut self, rg_index: usize) {
        let Some(rg) = self
            .row_groups
            .iter()
            .find(|candidate| candidate.index == rg_index)
            .cloned()
        else {
            return;
        };
        let evaluator = Arc::clone(&self.evaluator);
        let doc_range = self.doc_range;
        let token = self.cancellation_token.clone();
        let granularity = self.granularity;
        let handle = tokio::task::spawn_blocking(move || {
            Self::evaluate(&evaluator, &rg, doc_range, token.as_ref(), &granularity)
        });
        self.pending = Some((rg_index, handle));
    }

    /// Await the evaluation of `rg_index`, dispatching it first if the pending
    /// handle is for some other row group (or absent).
    async fn evaluated(&mut self, rg_index: usize) -> Result<Evaluated> {
        match self.pending.take() {
            Some((pending_index, handle)) if pending_index == rg_index => self.join(handle).await,
            other => {
                // Either nothing was dispatched (first boundary) or the decoder
                // moved to a row group we did not predict — a dynamic filter or
                // a limit can drop the row group we prefetched. Drop that work
                // and evaluate what was actually asked for.
                if other.is_some() {
                    if let Some(ref count) = self.metrics.prefetch_wait_count {
                        count.add(1);
                    }
                }
                self.dispatch(rg_index);
                let handle = self
                    .pending
                    .take()
                    .map(|(_, handle)| handle)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "row group {rg_index} is not in the chunk plan"
                        ))
                    })?;
                self.join(handle).await
            }
        }
    }

    async fn join(
        &self,
        handle: tokio::task::JoinHandle<Result<Evaluated, String>>,
    ) -> Result<Evaluated> {
        // Time spent here is time the decoder could not proceed: the prefetch
        // for this row group had not finished when its bytes were needed.
        let started = Instant::now();
        let joined = handle.await;
        if let Some(ref time) = self.metrics.prefetch_wait_time {
            time.add_duration(started.elapsed());
        }

        match joined {
            Ok(Ok(evaluated)) => Ok(evaluated),
            Ok(Err(message)) => Err(DataFusionError::External(message.into())),
            Err(join_error) if join_error.is_panic() => {
                // Deterministic failure (e.g. a subtree_cost invariant
                // violation). Propagate rather than retry: retrying would loop
                // forever and hang the calling Java thread.
                let payload = join_error.into_panic();
                let message = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_else(|| "unknown panic".into());
                Err(DataFusionError::Execution(format!(
                    "row group index evaluation panicked: {message}"
                )))
            }
            Err(_) => Err(DataFusionError::Execution(
                "row group index evaluation was cancelled".to_string(),
            )),
        }
    }
}

#[async_trait]
impl RowGroupAccessProvider for IndexedAccessProvider {
    async fn access_for(
        &mut self,
        pending: &[usize],
        metadata: &ParquetMetaData,
    ) -> Result<Vec<RowGroupAccess>> {
        if self.is_cancelled() {
            return Err(DataFusionError::Execution("query cancelled".to_string()));
        }
        let Some(&head) = pending.first() else {
            return Ok(vec![]);
        };

        // Boundary-phase dynamic-filter prune. The filter runs ~1 row group
        // behind the prefetch, so it may have tightened since `head` was
        // dispatched; re-checking here catches row groups that became prunable in
        // between. Skipping now still avoids the decode, just not the index work.
        if self.dynamically_excluded(head, metadata) {
            // Drop the evaluation for a row group that will not be read.
            if let Some((pending_index, handle)) = self.pending.take() {
                if pending_index == head {
                    handle.abort();
                } else {
                    self.pending = Some((pending_index, handle));
                }
            }
            let counter = if self.pruned_at_prefetch.remove(&head) {
                // Already attributed when the dispatch was withheld.
                None
            } else {
                self.metrics.dynamic_filter_rg_pruned_at_poll.as_ref()
            };
            if let Some(count) = counter {
                count.add(1);
            }
            self.dispatch_next(pending, metadata);
            return Ok(vec![RowGroupAccess::Skip]);
        }

        let evaluated = self.evaluated(head).await?;

        // Overlap: start the next row group's evaluation before returning, so
        // it runs while the decoder works through `head`.
        self.dispatch_next(pending, metadata);

        match evaluated {
            Evaluated::Candidates {
                access,
                context,
                eval_nanos,
                matched_rows,
                total_rows,
            } => {
                if let Some(ref time) = self.metrics.index_time {
                    time.add_duration(Duration::from_nanos(eval_nanos));
                }
                if let Some(ref count) = self.metrics.rows_matched {
                    count.add(matched_rows);
                }
                if let Some(ref count) = self.metrics.rows_pruned {
                    count.add(total_rows.saturating_sub(matched_rows));
                }
                if let Some(ref count) = self.metrics.rg_processed {
                    count.add(1);
                }
                self.contexts.put(head, context);
                Ok(vec![access])
            }
            Evaluated::Empty => {
                // No candidates: skip without reading any of the row group's
                // bytes. This is the indexed path's main I/O win.
                if let Some(ref count) = self.metrics.rg_skipped {
                    count.add(1);
                }
                Ok(vec![RowGroupAccess::Skip])
            }
        }
    }
}

/// Creates one [`IndexedAccessProvider`] per scanned file.
///
/// A chunk is always a single file, so the evaluator is consumed by the first
/// (and only) `create` call. A second call is a planning bug rather than a
/// recoverable condition, so it errors instead of silently scanning everything.
pub(super) struct IndexedAccessProviderFactory {
    inner: Mutex<Option<ProviderSeed>>,
}

impl std::fmt::Debug for IndexedAccessProviderFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let consumed = self.inner.lock().map(|g| g.is_none()).unwrap_or(true);
        f.debug_struct("IndexedAccessProviderFactory")
            .field("consumed", &consumed)
            .finish_non_exhaustive()
    }
}

struct ProviderSeed {
    evaluator: Arc<dyn RowGroupBitsetSource>,
    row_groups: Vec<RowGroupInfo>,
    doc_range: Option<(i32, i32)>,
    contexts: Arc<RgContextStore>,
    metrics: StreamMetrics,
    /// The pushed-down dynamic filter, still unsnapshotted. The pruner itself is
    /// built in `create` rather than here: it caches a snapshot generation, so
    /// each provider needs its own and none may be shared between segments.
    dynamic_filter: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Schema the dynamic filter's pruning predicate is built against.
    full_schema: datafusion::arrow::datatypes::SchemaRef,
    cancellation_token: Option<tokio_util::sync::CancellationToken>,
    granularity: SelectionGranularity,
}

impl IndexedAccessProviderFactory {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        evaluator: Arc<dyn RowGroupBitsetSource>,
        row_groups: Vec<RowGroupInfo>,
        doc_range: Option<(i32, i32)>,
        contexts: Arc<RgContextStore>,
        metrics: StreamMetrics,
        dynamic_filter: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        full_schema: datafusion::arrow::datatypes::SchemaRef,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
        granularity: SelectionGranularity,
    ) -> Self {
        Self {
            inner: Mutex::new(Some(ProviderSeed {
                evaluator,
                row_groups,
                doc_range,
                contexts,
                metrics,
                dynamic_filter,
                full_schema,
                cancellation_token,
                granularity,
            })),
        }
    }
}

impl datafusion::datasource::physical_plan::parquet::RowGroupAccessProviderFactory
    for IndexedAccessProviderFactory
{
    fn create(
        &self,
        _file: &datafusion::datasource::listing::PartitionedFile,
        _metadata: &Arc<ParquetMetaData>,
    ) -> Result<Option<Box<dyn RowGroupAccessProvider>>> {
        let seed = self.inner.lock().unwrap().take().ok_or_else(|| {
            DataFusionError::Internal(
                "indexed access provider requested twice for one chunk".to_string(),
            )
        })?;
        // Built here, not in the seed: the pruner caches a snapshot generation,
        // so it must not be shared across sibling segment scans.
        let dynamic_pruner =
            super::dynamic_filter::DynamicRgPruner::new(seed.dynamic_filter, seed.full_schema);
        Ok(Some(Box::new(IndexedAccessProvider::new(
            seed.evaluator,
            seed.row_groups,
            seed.doc_range,
            seed.contexts,
            seed.metrics,
            dynamic_pruner,
            seed.cancellation_token,
            seed.granularity,
        ))))
    }
}

/// Creates the decode-time [`RefinementPredicate`] for a chunk scan.
///
/// Installed only when the evaluator declared `refines_during_decode()`. Unlike
/// the access-provider factory this is *not* one-shot: DataFusion asks per file,
/// and the predicate it returns is cheap to build (it only clones handles), so a
/// repeat call is served rather than rejected.
///
/// [`RefinementPredicate`]: super::eval::decode_predicate::RefinementPredicate
pub(super) struct RefinementPredicateFactory {
    evaluator: Arc<dyn RowGroupBitsetSource>,
    locator: super::eval::decode_predicate::RowGroupLocator,
    contexts: Arc<RgContextStore>,
}

impl RefinementPredicateFactory {
    pub(super) fn new(
        evaluator: Arc<dyn RowGroupBitsetSource>,
        row_groups: Vec<RowGroupInfo>,
        contexts: Arc<RgContextStore>,
    ) -> Self {
        Self {
            evaluator,
            locator: super::eval::decode_predicate::RowGroupLocator::new(row_groups),
            contexts,
        }
    }
}

impl std::fmt::Debug for RefinementPredicateFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefinementPredicateFactory")
            .finish_non_exhaustive()
    }
}

impl datafusion::datasource::physical_plan::parquet::ArrowPredicateFactory
    for RefinementPredicateFactory
{
    fn create(
        &self,
        _file: &datafusion::datasource::listing::PartitionedFile,
        metadata: &Arc<ParquetMetaData>,
    ) -> Result<Vec<Box<dyn datafusion::parquet::arrow::arrow_reader::ArrowPredicate>>> {
        let schema_descr = metadata.file_metadata().schema_descr();
        // Without `__row_id__` the predicate cannot identify rows, so refining
        // during decode is impossible. Fail rather than silently returning no
        // predicate, which would emit the unrefined candidate superset.
        let projection = super::eval::decode_predicate::refinement_projection(
            schema_descr,
            &self.evaluator.refinement_columns(),
        )
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "decode-time refinement requires the {} column",
                crate::ROW_ID_COLUMN_NAME
            ))
        })?;
        Ok(vec![Box::new(
            super::eval::decode_predicate::RefinementPredicate::new(
                Arc::clone(&self.evaluator),
                self.locator.clone(),
                projection,
                Arc::clone(&self.contexts),
            ),
        )])
    }
}

/// Selection granularity for one row group's candidate set.
///
/// Sparse candidates get a row-granular selection: the skips are long, so each
/// one saves real bytes. Dense candidates get short skips absorbed into the
/// surrounding `select` — the decoder's per-selector cost, paid on every column,
/// then outweighs the handful of rows over-read, and the refinement stage drops
/// those rows anyway. `selectivity_threshold` is the crossover.
///
/// `refinement_masks_non_candidates` is that "anyway": coalescing hands the
/// decoder rows the candidate stage rejected, so it is sound only when
/// refinement drops them again. When the evaluator's `on_batch_mask` returns
/// `None` — the `RowSelection` *is* the answer — every over-read row would reach
/// the consumer, so the selection stays row-granular no matter how dense the
/// candidates are, and no matter what `force_strategy` asks for.
///
/// Returns `1` when the selection must stay row-granular.
fn min_skip_run_for(
    matched_rows: usize,
    total_rows: usize,
    already_page_granular: bool,
    refinement_masks_non_candidates: bool,
    config: &SelectionGranularity,
) -> usize {
    // Checked ahead of `force_strategy` because it is a correctness bound, not a
    // preference: `boolean_mask` on an evaluator that cannot mask would emit
    // non-candidate rows. The diagnostic knob loses.
    if !refinement_masks_non_candidates {
        return 1;
    }
    match config.force_strategy {
        Some(FilterStrategy::RowSelection) => return 1,
        Some(FilterStrategy::BooleanMask) => return total_rows + 1,
        None => {}
    }
    // A pruner selection is page-granular already: its skips are whole pages,
    // never the short runs coalescing exists to remove.
    if already_page_granular || total_rows == 0 {
        return 1;
    }
    let selectivity = matched_rows as f64 / total_rows as f64;
    if selectivity < config.selectivity_threshold {
        1
    } else {
        config.min_skip_run_default
    }
}

/// The knobs [`min_skip_run_for`] reads, lifted out of the query config so the
/// decision is unit-testable without building one.
#[derive(Debug, Clone, Copy)]
pub(super) struct SelectionGranularity {
    pub(super) min_skip_run_default: usize,
    pub(super) selectivity_threshold: f64,
    pub(super) force_strategy: Option<FilterStrategy>,
}

impl SelectionGranularity {
    /// Read the knobs out of the per-query config once, at stream setup.
    pub(super) fn from_config(config: &crate::datafusion_query_config::DatafusionQueryConfig) -> Self {
        Self {
            min_skip_run_default: config.min_skip_run_default,
            selectivity_threshold: config.min_skip_run_selectivity_threshold,
            force_strategy: config.force_strategy,
        }
    }

    /// Row-granular always — the behaviour when coalescing is unavailable
    /// (no `__row_id__` to refine by) or unwanted.
    pub(super) fn row_granular() -> Self {
        Self {
            min_skip_run_default: 1,
            selectivity_threshold: 0.0,
            force_strategy: Some(FilterStrategy::RowSelection),
        }
    }
}

/// Build the decoder's `RowSelection` for a prefetched candidate set.
///
/// `refinement_masks_non_candidates` comes from
/// [`RowGroupBitsetSource::masks_non_candidates`] and decides whether the
/// selection may be coalesced at all — see [`min_skip_run_for`].
pub(super) fn selection_from_rows(
    rows: super::eval::PrefetchedRgRows,
    total_rows: usize,
    refinement_masks_non_candidates: bool,
    granularity: &SelectionGranularity,
) -> datafusion::parquet::arrow::arrow_reader::RowSelection {
    use super::eval::PrefetchedRgRows;
    let matched_rows = rows.matched_rows();
    let page_granular = matches!(rows, PrefetchedRgRows::Selection { .. });
    let min_skip_run = min_skip_run_for(
        matched_rows,
        total_rows,
        page_granular,
        refinement_masks_non_candidates,
        granularity,
    );
    match rows {
        PrefetchedRgRows::Bitmap(candidates) => {
            build_row_selection_with_min_skip_run(&candidates, total_rows, min_skip_run)
        }
        PrefetchedRgRows::DenseBitmap(candidates) => candidates.to_row_selection(min_skip_run),
        // Already page-granular from the pruner; pass it straight through.
        PrefetchedRgRows::Selection { selection, .. } => selection,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{BooleanArray, RecordBatch};
    use roaring::RoaringBitmap;

    use super::*;
    use crate::indexed_table::eval::PrefetchedRg;

    /// Records the order in which row groups are evaluated.
    #[derive(Debug, Default)]
    struct RecordingEvaluator {
        /// Row groups whose evaluation started, in order.
        started: Mutex<Vec<usize>>,
        /// Row groups the evaluator should report as having no candidates.
        empty: Vec<usize>,
    }

    impl RecordingEvaluator {
        fn new(empty: Vec<usize>) -> Self {
            Self {
                empty,
                ..Default::default()
            }
        }

        fn started(&self) -> Vec<usize> {
            self.started.lock().unwrap().clone()
        }
    }

    impl RowGroupBitsetSource for RecordingEvaluator {
        fn prefetch_rg(
            &self,
            rg: &RowGroupInfo,
            _min_doc: i32,
            _max_doc: i32,
        ) -> std::result::Result<Option<PrefetchedRg>, String> {
            self.started.lock().unwrap().push(rg.index);
            if self.empty.contains(&rg.index) {
                return Ok(None);
            }
            // Every row of the row group is a candidate.
            let mut candidates = RoaringBitmap::new();
            candidates.insert_range(0..rg.num_rows as u32);
            Ok(Some(PrefetchedRg::without_context(candidates, 0)))
        }

        fn on_batch_mask(
            &self,
            _rg_state: &dyn std::any::Any,
            _rg_first_row: i64,
            _row_positions: &crate::indexed_table::eval::RowPositions,
            _batch: &RecordBatch,
        ) -> std::result::Result<Option<BooleanArray>, String> {
            Ok(None)
        }
    }

    fn row_groups(count: usize, rows_each: i64) -> Vec<RowGroupInfo> {
        (0..count)
            .map(|index| RowGroupInfo {
                index,
                first_row: index as i64 * rows_each,
                num_rows: rows_each,
            })
            .collect()
    }

    fn provider(
        evaluator: Arc<RecordingEvaluator>,
        row_groups: Vec<RowGroupInfo>,
    ) -> IndexedAccessProvider {
        provider_with_pruner(evaluator, row_groups, StreamMetrics::empty(), None)
    }

    fn provider_with_pruner(
        evaluator: Arc<RecordingEvaluator>,
        row_groups: Vec<RowGroupInfo>,
        metrics: StreamMetrics,
        dynamic_pruner: Option<super::super::dynamic_filter::DynamicRgPruner>,
    ) -> IndexedAccessProvider {
        IndexedAccessProvider::new(
            evaluator,
            row_groups,
            None,
            Arc::new(RgContextStore::new()),
            metrics,
            dynamic_pruner,
            None,
            SelectionGranularity::row_granular(),
        )
    }

    /// `ParquetMetaData` is part of the trait signature but this provider does
    /// not read it, so tests supply a minimal file's metadata.
    fn empty_metadata() -> Arc<ParquetMetaData> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let reader = datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
            &bytes::Bytes::from(buffer),
            Default::default(),
        )
        .unwrap();
        Arc::clone(reader.metadata())
    }

    /// A file of `count` single-row-group chunks is awkward to build, so the
    /// dynamic-filter tests write one file whose row group `i` holds `v = i`.
    /// That gives each row group real statistics for the pruner to read.
    fn metadata_with_v_per_row_group(count: usize) -> Arc<ParquetMetaData> {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let mut buffer = Vec::new();
        // One row per row group keeps min == max == the row group's own index.
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .build();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, Arc::clone(&schema), Some(props)).unwrap();
        for index in 0..count {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![index as i32]))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();

        let reader = datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
            &bytes::Bytes::from(buffer),
            Default::default(),
        )
        .unwrap();
        Arc::clone(reader.metadata())
    }

    /// A pruner over `v > threshold`, matching the shape a TopK pushes down once
    /// its heap has filled.
    fn pruner_v_greater_than(threshold: i32) -> super::super::dynamic_filter::DynamicRgPruner {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::physical_expr::PhysicalExpr;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("v", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(threshold)))),
        ));
        super::super::dynamic_filter::DynamicRgPruner::new(Some(expr), schema)
            .expect("a pruner is built whenever a filter is supplied")
    }

    /// The invariant the indexed path depends on: answering for the head row
    /// group also kicks off the *next* one, so its index evaluation overlaps
    /// the decode of the head instead of serializing behind it.
    #[tokio::test]
    async fn answering_for_head_dispatches_the_next_row_group() {
        let evaluator = Arc::new(RecordingEvaluator::new(vec![]));
        let metadata = empty_metadata();
        let mut provider = provider(Arc::clone(&evaluator), row_groups(3, 8));

        let access = provider.access_for(&[0, 1, 2], metadata.as_ref()).await;
        assert!(matches!(
            access.unwrap().as_slice(),
            [RowGroupAccess::Selection(_)]
        ));

        // Row group 1's evaluation is in flight even though only row group 0 was
        // answered for, so it runs while the decoder works through 0. Asserting
        // on `pending` rather than on `started` keeps this deterministic: the
        // dispatch is synchronous, the blocking task's first line is not.
        assert_eq!(
            provider.pending.as_ref().map(|(rg, _)| *rg),
            Some(1),
            "answering for row group 0 must leave row group 1's evaluation in flight"
        );
    }

    /// The last row group has no successor to prefetch; the provider must not
    /// leave a handle behind for a row group that will never be asked for.
    #[tokio::test]
    async fn last_row_group_dispatches_nothing() {
        let evaluator = Arc::new(RecordingEvaluator::new(vec![]));
        let metadata = empty_metadata();
        let mut provider = provider(Arc::clone(&evaluator), row_groups(1, 8));

        provider.access_for(&[0], metadata.as_ref()).await.unwrap();

        assert!(provider.pending.is_none());
        assert_eq!(evaluator.started(), vec![0]);
    }

    /// The prefetch dispatched at the previous boundary is consumed rather than
    /// re-run, so a row group's index is evaluated exactly once.
    #[tokio::test]
    async fn prefetched_row_group_is_not_evaluated_twice() {
        let evaluator = Arc::new(RecordingEvaluator::new(vec![]));
        let metadata = empty_metadata();
        let mut provider = provider(Arc::clone(&evaluator), row_groups(3, 8));

        provider
            .access_for(&[0, 1, 2], metadata.as_ref())
            .await
            .unwrap();
        provider
            .access_for(&[1, 2], metadata.as_ref())
            .await
            .unwrap();
        provider.access_for(&[2], metadata.as_ref()).await.unwrap();

        assert_eq!(
            evaluator.started(),
            vec![0, 1, 2],
            "each row group's index should be evaluated exactly once, in order"
        );
    }

    /// A row group the index rules out becomes `Skip`, so DataFusion never
    /// fetches its bytes — the indexed path's main I/O win.
    #[tokio::test]
    async fn row_group_without_candidates_is_skipped() {
        let evaluator = Arc::new(RecordingEvaluator::new(vec![0]));
        let metadata = empty_metadata();
        let mut provider = provider(Arc::clone(&evaluator), row_groups(2, 8));

        let access = provider
            .access_for(&[0, 1], metadata.as_ref())
            .await
            .unwrap();
        assert!(matches!(access.as_slice(), [RowGroupAccess::Skip]));
    }

    /// When the decoder lands on a row group other than the one prefetched —
    /// a limit or dynamic filter can drop it — the stale work is discarded and
    /// the row group actually asked for is evaluated.
    #[tokio::test]
    async fn unexpected_head_evaluates_the_row_group_asked_for() {
        let evaluator = Arc::new(RecordingEvaluator::new(vec![]));
        let metadata = empty_metadata();
        let mut provider = provider(Arc::clone(&evaluator), row_groups(4, 8));

        provider
            .access_for(&[0, 1, 2, 3], metadata.as_ref())
            .await
            .unwrap();
        // Row group 1 was prefetched, but it got dropped and the decoder is
        // now at 2.
        provider
            .access_for(&[2, 3], metadata.as_ref())
            .await
            .unwrap();

        // Answered for the row group actually asked for, not the prefetched one.
        assert!(
            provider.contexts.peek(2).is_some(),
            "row group 2 must be evaluated even though row group 1 was prefetched"
        );
        assert!(
            provider.contexts.peek(1).is_none(),
            "the discarded prefetch must not publish a context"
        );
        // Overlap still holds after the correction.
        assert_eq!(provider.pending.as_ref().map(|(rg, _)| *rg), Some(3));
    }

    /// A row group the dynamic filter provably excludes is skipped *and* its
    /// index is never evaluated: withholding the dispatch is the prefetch-phase
    /// win, saving the Lucene/FFM call on top of the decode.
    #[tokio::test]
    async fn dynamically_excluded_row_group_is_skipped_without_being_evaluated() {
        let evaluator = Arc::new(RecordingEvaluator::new(vec![]));
        let metadata = metadata_with_v_per_row_group(3);
        let metrics = StreamMetrics::counting_dynamic_prunes();
        // `v > 1` excludes row groups 0 and 1 (v = 0, 1) and keeps 2.
        let mut provider = provider_with_pruner(
            Arc::clone(&evaluator),
            row_groups(3, 1),
            metrics.clone(),
            Some(pruner_v_greater_than(1)),
        );

        let head = provider
            .access_for(&[0, 1, 2], metadata.as_ref())
            .await
            .unwrap();
        assert!(
            matches!(head.as_slice(), [RowGroupAccess::Skip]),
            "row group 0 (v = 0) cannot satisfy v > 1"
        );
        // Nothing was dispatched for row group 1, the excluded successor: it is
        // not handed to a blocking thread at all. This is the assertion that
        // fails if the prefetch-phase withholding is dropped — `pending` is set
        // synchronously, so it does not depend on whether a spawned task ran.
        assert!(
            provider.pending.is_none(),
            "an excluded row group must not have its index evaluation dispatched"
        );

        let next = provider
            .access_for(&[1, 2], metadata.as_ref())
            .await
            .unwrap();
        assert!(matches!(next.as_slice(), [RowGroupAccess::Skip]));
        // Withholding is per row group, not a latch: row group 2 survives the
        // filter, so the overlap resumes for it.
        assert_eq!(
            provider.pending.as_ref().map(|(rg, _)| *rg),
            Some(2),
            "a surviving successor must still be prefetched"
        );

        let kept = provider.access_for(&[2], metadata.as_ref()).await.unwrap();
        assert!(
            matches!(kept.as_slice(), [RowGroupAccess::Selection(_)]),
            "row group 2 (v = 2) satisfies v > 1 and must be read"
        );

        assert_eq!(
            evaluator.started(),
            vec![2],
            "only the surviving row group's index should be evaluated"
        );
    }

    /// The two prune phases are attributed separately and never double-counted:
    /// a row group whose dispatch was withheld counts once, at the prefetch
    /// phase, even though the boundary check also excludes it.
    #[tokio::test]
    async fn withheld_dispatch_is_counted_once_at_the_prefetch_phase() {
        let evaluator = Arc::new(RecordingEvaluator::new(vec![]));
        let metadata = metadata_with_v_per_row_group(3);
        let metrics = StreamMetrics::counting_dynamic_prunes();
        // `v > 2` excludes every row group in the file.
        let mut provider = provider_with_pruner(
            Arc::clone(&evaluator),
            row_groups(3, 1),
            metrics.clone(),
            Some(pruner_v_greater_than(2)),
        );

        for boundary in 0..3 {
            let pending: Vec<usize> = (boundary..3).collect();
            let access = provider
                .access_for(&pending, metadata.as_ref())
                .await
                .unwrap();
            assert!(matches!(access.as_slice(), [RowGroupAccess::Skip]));
        }

        let at_prefetch = metrics
            .dynamic_filter_rg_pruned_at_prefetch
            .as_ref()
            .unwrap()
            .value();
        let at_poll = metrics
            .dynamic_filter_rg_pruned_at_poll
            .as_ref()
            .unwrap()
            .value();
        // Row group 0 has no prior boundary to withhold its dispatch, so it is
        // caught at the boundary; 1 and 2 are withheld a boundary earlier.
        assert_eq!(
            (at_prefetch, at_poll),
            (2, 1),
            "each pruned row group must be attributed to exactly one phase"
        );
        assert_eq!(at_prefetch + at_poll, 3, "3 row groups, 3 prunes");
        assert!(
            evaluator.started().is_empty(),
            "no index evaluation should run when every row group is excluded"
        );
    }

    /// A filter that excludes nothing must not prune: pruning is proof-based, and
    /// a satisfiable row group is read normally.
    #[tokio::test]
    async fn satisfiable_row_groups_are_not_pruned() {
        let evaluator = Arc::new(RecordingEvaluator::new(vec![]));
        let metadata = metadata_with_v_per_row_group(2);
        let metrics = StreamMetrics::counting_dynamic_prunes();
        // `v > -1` is true for every row group.
        let mut provider = provider_with_pruner(
            Arc::clone(&evaluator),
            row_groups(2, 1),
            metrics.clone(),
            Some(pruner_v_greater_than(-1)),
        );

        let access = provider
            .access_for(&[0, 1], metadata.as_ref())
            .await
            .unwrap();
        assert!(matches!(access.as_slice(), [RowGroupAccess::Selection(_)]));
        assert_eq!(
            metrics
                .dynamic_filter_rg_pruned_at_prefetch
                .as_ref()
                .unwrap()
                .value()
                + metrics
                    .dynamic_filter_rg_pruned_at_poll
                    .as_ref()
                    .unwrap()
                    .value(),
            0,
            "nothing is provably excluded, so nothing may be pruned"
        );
        // The overlap still holds: the surviving successor was dispatched.
        assert_eq!(provider.pending.as_ref().map(|(rg, _)| *rg), Some(1));
    }

    // ── Selection granularity ──

    /// Production defaults: coalesce at or above 3% candidate density.
    fn heuristic() -> SelectionGranularity {
        SelectionGranularity {
            min_skip_run_default: 1024,
            selectivity_threshold: 0.03,
            force_strategy: None,
        }
    }

    /// Sparse candidates make long skips, each one saving real bytes, so the
    /// selection stays row-granular. Dense candidates make short skips whose
    /// per-selector cost — paid on every column — outweighs the rows over-read.
    #[test]
    fn granularity_follows_candidate_density_around_the_threshold() {
        // 2% — below the 3% threshold.
        assert_eq!(min_skip_run_for(200, 10_000, false, true, &heuristic()), 1);
        // 5% — above it.
        assert_eq!(
            min_skip_run_for(500, 10_000, false, true, &heuristic()),
            1024
        );
        // Exactly at the threshold coalesces: the comparison is `<`.
        assert_eq!(
            min_skip_run_for(300, 10_000, false, true, &heuristic()),
            1024
        );
    }

    /// Coalescing emits rows the candidate stage rejected, so it is licensed
    /// only by a refinement that drops them again. An evaluator whose
    /// `on_batch_mask` returns `None` would emit them, so density is irrelevant
    /// — the selection must stay exactly row-granular.
    #[test]
    fn granularity_stays_row_granular_without_a_masking_refinement() {
        // Same dense input that coalesced above.
        assert_eq!(min_skip_run_for(500, 10_000, false, false, &heuristic()), 1);
    }

    /// The guard is a correctness bound, not a preference: forcing
    /// `boolean_mask` on an evaluator that cannot mask would emit non-candidate
    /// rows, so the diagnostic knob loses to the guard.
    #[test]
    fn force_strategy_cannot_override_the_masking_guard() {
        let forced = SelectionGranularity {
            force_strategy: Some(FilterStrategy::BooleanMask),
            ..heuristic()
        };
        assert_eq!(min_skip_run_for(500, 10_000, false, false, &forced), 1);
        // With a masking refinement the override applies: one whole-RG select.
        assert_eq!(
            min_skip_run_for(500, 10_000, false, true, &forced),
            10_001,
            "a min_skip_run past the row count absorbs every skip"
        );
    }

    /// A pruner's selection skips whole pages, never the short runs coalescing
    /// exists to remove, so it is passed through untouched however dense it is.
    #[test]
    fn page_granular_selections_are_not_coalesced() {
        assert_eq!(min_skip_run_for(9_000, 10_000, true, true, &heuristic()), 1);
    }

    /// An empty row group has no selectivity to compute; guard the division.
    #[test]
    fn empty_row_group_stays_row_granular() {
        assert_eq!(min_skip_run_for(0, 0, false, true, &heuristic()), 1);
    }

    /// Without a masking refinement the coalescing guard must hold end to end,
    /// not just in `min_skip_run_for`: every candidate row is selected and every
    /// non-candidate row is skipped, so the decoder delivers exactly the
    /// candidates.
    #[test]
    fn selection_without_masking_refinement_selects_only_candidates() {
        use crate::indexed_table::eval::PrefetchedRgRows;

        // Alternating candidates: every skip run is length 1, the shape
        // coalescing would absorb entirely.
        let mut candidates = RoaringBitmap::new();
        for row in (0..2_048u32).step_by(2) {
            candidates.insert(row);
        }
        let rows = PrefetchedRgRows::Bitmap(Arc::new(candidates));

        let selection = selection_from_rows(rows, 2_048, false, &heuristic());

        let selected: usize = selection
            .iter()
            .filter(|selector| !selector.skip)
            .map(|selector| selector.row_count)
            .sum();
        assert_eq!(
            selected, 1_024,
            "half the row group is a candidate, so exactly half may be delivered"
        );
    }

    /// The same input *with* a masking refinement coalesces into one run — the
    /// win the guard above deliberately forgoes.
    #[test]
    fn selection_with_masking_refinement_coalesces_short_skips() {
        use crate::indexed_table::eval::PrefetchedRgRows;

        let mut candidates = RoaringBitmap::new();
        for row in (0..2_048u32).step_by(2) {
            candidates.insert(row);
        }
        let rows = PrefetchedRgRows::Bitmap(Arc::new(candidates));

        let selection = selection_from_rows(rows, 2_048, true, &heuristic());

        let selected: usize = selection
            .iter()
            .filter(|selector| !selector.skip)
            .map(|selector| selector.row_count)
            .sum();
        assert_eq!(
            selected, 2_048,
            "every length-1 skip is absorbed, so the whole row group is read \
             and the refinement drops the odd rows"
        );
    }
}
