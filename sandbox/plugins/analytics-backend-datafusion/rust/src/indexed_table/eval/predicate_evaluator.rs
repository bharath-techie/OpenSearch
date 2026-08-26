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
use datafusion::physical_optimizer::pruning::PruningPredicate;
use roaring::RoaringBitmap;

use super::eval_helpers::{
    compute_page_ranges, evaluate_residual, universe_bitmap_from_page_ranges,
};
use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::ffm_callbacks::get_live_docs;
use crate::indexed_table::page_pruner::{PagePruneMetrics, PagePruner, StatsPruneTree};
use crate::indexed_table::row_selection::{bitmap_to_packed_bits, PositionMap};
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
    /// Query context and segment generation used to fetch liveDocs before decode.
    live_docs: Option<(i64, i64)>,
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
            live_docs: None,
        }
    }

    pub fn with_live_docs(mut self, context_id: i64, writer_generation: i64) -> Self {
        self.live_docs = Some((context_id, writer_generation));
        self
    }
}

fn intersect_live_docs(candidates: &mut RoaringBitmap, words: &[u64], len: u32) {
    for (word_index, &word) in words.iter().enumerate() {
        let base = (word_index as u32) * 64;
        if base >= len {
            break;
        }
        let valid_bits = (len - base).min(64);
        let valid_mask = if valid_bits == 64 {
            u64::MAX
        } else {
            (1u64 << valid_bits) - 1
        };
        let mut deleted = (!word) & valid_mask;
        while deleted != 0 {
            let bit = deleted.trailing_zeros();
            candidates.remove(base + bit);
            deleted &= deleted - 1;
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

        let page_ranges = compute_page_ranges(
            self.pruning_predicate.as_ref(),
            &self.page_pruner,
            rg,
            min_doc,
            self.page_prune_metrics.as_ref(),
        );

        let mut candidates = match universe_bitmap_from_page_ranges(&page_ranges, rg) {
            Some(bm) if bm.is_empty() => return Ok(None),
            Some(bm) => bm,
            None => return Ok(None),
        };

        if let Some((context_id, writer_generation)) = self.live_docs {
            if let Some(words) = get_live_docs(
                context_id,
                writer_generation,
                min_doc,
                min_doc + rg.num_rows as i32,
            )? {
                intersect_live_docs(&mut candidates, &words, rg.num_rows as u32);
                if candidates.is_empty() {
                    return Ok(None);
                }
            }
        }

        let mask_len = rg.num_rows as usize;
        let packed_bits = bitmap_to_packed_bits(&candidates, mask_len as u32);
        let mask_buffer = datafusion::arrow::buffer::Buffer::from_vec(packed_bits);
        Ok(Some(PrefetchedRg {
            candidates,
            eval_nanos: t.elapsed().as_nanos() as u64,
            context: Box::new(()),
            mask_buffer: Some(mask_buffer),
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
        let Some(ref residual) = self.residual_expr else {
            return Ok(None);
        };
        Ok(Some(evaluate_residual(residual, batch, batch_len)?))
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
        Arc::new(PagePruner::new(
            meta.schema(),
            meta.metadata().clone(),
            meta.schema().clone(),
        ))
    }

    #[test]
    fn live_docs_intersection_removes_only_deleted_candidates() {
        let mut candidates = RoaringBitmap::from_iter([0u32, 1, 2, 3, 64, 65, 66, 69]);
        intersect_live_docs(&mut candidates, &[0b1101, 0b100101], 70);
        assert_eq!(
            candidates.iter().collect::<Vec<_>>(),
            vec![0, 2, 3, 64, 66, 69]
        );
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
        assert_eq!(prefetched.candidates.len(), 8);
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
        assert_eq!(prefetched.candidates.len(), 8);
    }
}
