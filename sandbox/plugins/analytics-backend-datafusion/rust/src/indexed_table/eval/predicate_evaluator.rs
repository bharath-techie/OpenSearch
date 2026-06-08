/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Predicate-only evaluator — no collector, pure parquet-native filtering.
//!
//! Used for `FilterClass::None` with `emit_row_ids=true`: the query has no
//! `index_filter(...)` call (no Lucene collector), only DataFusion predicates.
//! Candidates default to the page-pruned universe; `on_batch_mask` evaluates
//! only the residual predicate.

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use roaring::RoaringBitmap;

use super::eval_helpers::{compute_page_ranges, CachedResidual, universe_bitmap_from_page_ranges};
use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner};
use crate::indexed_table::row_selection::PositionMap;
use crate::indexed_table::stream::RowGroupInfo;

/// Evaluator for predicate-only queries (no Collector).
///
/// Candidates = page-pruned universe. Residual predicate applied in `on_batch_mask`.
pub struct PredicateOnlyEvaluator {
    page_pruner: Arc<PagePruner>,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Residual predicate, remapped to the batch schema once and reused across
    /// batches. `None` when there is no residual (no page pruning either, so the
    /// candidate universe is gap-free and no per-batch filtering is needed).
    residual: Option<CachedResidual>,
    page_prune_metrics: Option<PagePruneMetrics>,
}

impl PredicateOnlyEvaluator {
    pub fn new(
        page_pruner: Arc<PagePruner>,
        pruning_predicate: Option<Arc<PruningPredicate>>,
        residual_expr: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        page_prune_metrics: Option<PagePruneMetrics>,
    ) -> Self {
        Self {
            page_pruner,
            pruning_predicate,
            residual: residual_expr.map(CachedResidual::new),
            page_prune_metrics,
        }
    }
}

impl RowGroupBitsetSource for PredicateOnlyEvaluator {
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        min_doc: i32,
        _max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let t = Instant::now();

        let page_ranges = compute_page_ranges(
            self.pruning_predicate.as_ref(),
            &self.page_pruner,
            rg,
            min_doc,
            self.page_prune_metrics.as_ref(),
        );

        let candidates = match universe_bitmap_from_page_ranges(&page_ranges, rg) {
            Some(bm) if bm.is_empty() => return Ok(None),
            Some(bm) => bm,
            None => return Ok(None),
        };

        // Fast-path select runs straight from the page ranges (RG-relative
        // `(start, len)`), so `IndexedStream` can build the parquet
        // `RowSelection` without re-walking the full candidate bitmap bit-by-bit
        // in `build_row_selection_with_min_skip_run` (the dominant cost on
        // non-selective full scans). Mirrors `universe_bitmap_from_page_ranges`'
        // RG-relative offset math. `None` page_ranges = whole RG = one run.
        let selection_runs: Vec<(usize, usize)> = match &page_ranges {
            Some(ranges) => ranges
                .iter()
                .map(|(r_min, r_max)| {
                    let lo = (*r_min as i64 - rg.first_row) as usize;
                    let len = (*r_max - *r_min) as usize;
                    (lo, len)
                })
                .collect(),
            None => vec![(0, rg.num_rows as usize)],
        };

        // No `mask_buffer`: with `needs_row_mask() == false` the stream never
        // builds `current_mask`, so the packed-bits buffer this evaluator used
        // to pre-materialize would be dead work. The residual in `on_batch_mask`
        // (or parquet pushdown when row-granular) does the filtering. Skipping
        // `bitmap_to_packed_bits` here removes a full per-RG bit-iteration.
        Ok(Some(PrefetchedRg {
            candidates,
            eval_nanos: t.elapsed().as_nanos() as u64,
            context: Box::new(()),
            mask_buffer: None,
            selection_runs: Some(selection_runs),
        }))
    }

    fn on_batch_mask(
        &self,
        _rg_state: &dyn std::any::Any,
        _rg_first_row: i64,
        _position_map: &PositionMap,
        _batch_offset: usize,
        batch_len: usize,
        batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        let Some(ref residual) = self.residual else {
            return Ok(None);
        };
        Ok(Some(residual.eval(batch, batch_len)?))
    }

    /// The candidate-stage `current_mask` is never consumed for this evaluator:
    /// when `residual` is `Some`, `on_batch_mask` returns the exact residual
    /// mask and `finalize_batch` applies it EXCLUSIVELY (ignoring `current_mask`);
    /// when it's `None`, there is no page pruning, so the candidate universe has
    /// no gaps and no mask is needed. Returning `false` skips the per-RG
    /// `build_mask` over the full row group (block-granular regime) — pure waste
    /// here, since the residual in `on_batch_mask` already does the filtering.
    fn needs_row_mask(&self) -> bool {
        false
    }
}
