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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use datafusion::physical_optimizer::pruning::PruningPredicate;

use super::eval_helpers::evaluate_residual;
use super::RowPositions;
use super::{PrefetchedRg, PrefetchedRgRows, RowGroupBitsetSource};
use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner, StatsPruneTree};
use crate::indexed_table::stream::RowGroupInfo;

/// Evaluator for predicate-only queries (no Collector).
///
/// Candidates = page-pruned universe. Residual predicate applied in `on_batch_mask`.
pub struct PredicateOnlyEvaluator {
    page_pruner: Arc<PagePruner>,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    residual_expr: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    page_prune_metrics: Option<PagePruneMetrics>,
    stats_prune_tree: Option<Arc<StatsPruneTree>>,
    /// Reverse map: absolute RG index → position in `rg_can_match` vectors.
    rg_index_to_pos: HashMap<usize, usize>,
}

impl PredicateOnlyEvaluator {
    pub fn new(
        page_pruner: Arc<PagePruner>,
        pruning_predicate: Option<Arc<PruningPredicate>>,
        residual_expr: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        page_prune_metrics: Option<PagePruneMetrics>,
        stats_prune_tree: Option<Arc<StatsPruneTree>>,
        rg_index_to_pos: HashMap<usize, usize>,
    ) -> Self {
        Self {
            page_pruner,
            pruning_predicate,
            residual_expr,
            page_prune_metrics,
            stats_prune_tree,
            rg_index_to_pos,
        }
    }
}

impl RowGroupBitsetSource for PredicateOnlyEvaluator {
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        _min_doc: i32,
        _max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let t = Instant::now();

        // RG-level early-exit: precomputed from column stats at construction.
        if let Some(ref spt) = self.stats_prune_tree {
            if let Some(&pos) = self.rg_index_to_pos.get(&rg.index) {
                if let Some(&false) = spt.rg_can_match.get(pos) {
                    native_bridge_common::log_debug!(
                        "PredicateOnly: skipping RG {} — pruned by RG-level stats",
                        rg.index
                    );
                    return Ok(None);
                }
            }
        }

        let selection = match self.pruning_predicate.as_ref() {
            Some(predicate) => self
                .page_pruner
                .prune_rg(predicate, rg.index, self.page_prune_metrics.as_ref())
                .unwrap_or_else(|| {
                    RowSelection::from(vec![RowSelector::select(rg.num_rows as usize)])
                }),
            None => RowSelection::from(vec![RowSelector::select(rg.num_rows as usize)]),
        };
        let selected_rows = selection
            .iter()
            .filter(|selector| selector.skip == false)
            .map(|selector| selector.row_count)
            .sum();
        if selected_rows == 0 {
            return Ok(None);
        }
        Ok(Some(PrefetchedRg {
            rows: PrefetchedRgRows::Selection {
                selection,
                selected_rows,
            },
            eval_nanos: t.elapsed().as_nanos() as u64,
            context: Box::new(()),
        }))
    }

    fn on_batch_mask(
        &self,
        _rg_state: &dyn std::any::Any,
        _rg_first_row: i64,
        row_positions: &RowPositions,
        batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        let Some(ref residual) = self.residual_expr else {
            return Ok(None);
        };
        // The residual is a plain expression over the batch's own columns, so
        // this evaluator never needs row positions — only the row count.
        Ok(Some(evaluate_residual(
            residual,
            batch,
            row_positions.len(),
        )?))
    }

    fn forbid_parquet_pushdown(&self) -> bool {
        // The page pruner already supplies a bounded direct RowSelection.
        // A controlled ClickBench A/B found that evaluating the same residual
        // as a parquet RowFilter regressed 14/15 tested query shapes, so keep
        // refinement on decoded batches for this evaluator.
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::page_pruner::PagePruner;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn minimal_page_pruner() -> Arc<PagePruner> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![0i32; 8]))],
        )
        .unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let mut writer = ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = tmp.reopen().unwrap();
        let options = ArrowReaderOptions::new().with_page_index(true);
        let meta = ArrowReaderMetadata::load(&file, options).unwrap();
        Arc::new(PagePruner::new(meta.schema(), meta.metadata().clone()))
    }

    #[test]
    fn stats_prune_tree_skips_rg_when_false() {
        let pruner = minimal_page_pruner();
        let spt = StatsPruneTree {
            rg_can_match: vec![false],
            children: vec![],
        };
        let eval = PredicateOnlyEvaluator::new(
            pruner,
            None,
            None,
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
        let pruner = minimal_page_pruner();
        let spt = StatsPruneTree {
            rg_can_match: vec![true],
            children: vec![],
        };
        let eval = PredicateOnlyEvaluator::new(
            pruner,
            None,
            None,
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
            .expect("should have candidates");
        assert_eq!(prefetched.rows.matched_rows(), 8);
        assert!(prefetched.rows.bitmap().is_none());
    }

    #[test]
    fn stats_prune_tree_none_does_not_prune() {
        let pruner = minimal_page_pruner();
        let eval = PredicateOnlyEvaluator::new(pruner, None, None, None, None, HashMap::new());
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 8,
        };
        let prefetched = eval
            .prefetch_rg(&rg, 0, 8)
            .unwrap()
            .expect("should have candidates");
        assert_eq!(prefetched.rows.matched_rows(), 8);
        assert!(prefetched.rows.bitmap().is_none());
    }
}
