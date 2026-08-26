/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Generic liveDocs filtering for indexed Parquet scans.
//!
//! The wrapper intersects the candidate bitmap before decode and, when the
//! wrapped evaluator produces an exact post-decode mask, intersects that mask
//! too. Applying both stages is required: tree and residual evaluators may
//! intentionally ignore the candidate-stage mask when producing their exact
//! result.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{BooleanArray, BooleanBuilder};
use datafusion::arrow::record_batch::RecordBatch;

use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::ffm_callbacks::get_live_docs;
use crate::indexed_table::row_selection::{bitmap_to_packed_bits, PositionMap};
use crate::indexed_table::stream::RowGroupInfo;

pub struct LiveDocsEvaluator {
    inner: Arc<dyn RowGroupBitsetSource>,
    context_id: i64,
    writer_generation: i64,
}

struct LiveDocsRgContext {
    inner: Box<dyn Any + Send + Sync>,
    words: Option<Vec<u64>>,
    row_count: usize,
}

impl LiveDocsEvaluator {
    pub fn new(
        inner: Arc<dyn RowGroupBitsetSource>,
        context_id: i64,
        writer_generation: i64,
    ) -> Self {
        Self {
            inner,
            context_id,
            writer_generation,
        }
    }
}

fn intersect_live_docs(candidates: &mut roaring::RoaringBitmap, words: &[u64], row_count: usize) {
    for (word_index, &word) in words.iter().enumerate() {
        let base = word_index * 64;
        if base >= row_count {
            break;
        }
        let valid_bits = (row_count - base).min(64);
        let valid_mask = if valid_bits == 64 {
            u64::MAX
        } else {
            (1u64 << valid_bits) - 1
        };
        let mut deleted = (!word) & valid_mask;
        while deleted != 0 {
            let bit = deleted.trailing_zeros() as usize;
            candidates.remove((base + bit) as u32);
            deleted &= deleted - 1;
        }
    }
}

fn is_live(words: &[u64], row: usize, row_count: usize) -> Result<bool, String> {
    if row >= row_count {
        return Err(format!(
            "liveDocs row {} is outside row-group length {}",
            row, row_count
        ));
    }
    let word = words.get(row >> 6).ok_or_else(|| {
        format!(
            "liveDocs bitmap is too short for row {} ({} words)",
            row,
            words.len()
        )
    })?;
    Ok((word & (1u64 << (row & 63))) != 0)
}

impl RowGroupBitsetSource for LiveDocsEvaluator {
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let Some(mut prefetched) = self.inner.prefetch_rg(rg, min_doc, max_doc)? else {
            return Ok(None);
        };
        let started = Instant::now();

        // Candidates and PositionMap use row-group-relative positions, so fetch
        // the complete row-group range even when dynamic pruning narrowed the
        // evaluator's min/max bounds.
        let rg_start = i32::try_from(rg.first_row)
            .map_err(|_| format!("row-group start {} exceeds i32", rg.first_row))?;
        let row_count = usize::try_from(rg.num_rows)
            .map_err(|_| format!("invalid row-group length {}", rg.num_rows))?;
        let rg_end = rg_start
            .checked_add(
                i32::try_from(rg.num_rows)
                    .map_err(|_| format!("row-group length {} exceeds i32", rg.num_rows))?,
            )
            .ok_or_else(|| "row-group doc range overflow".to_string())?;

        let words = get_live_docs(self.context_id, self.writer_generation, rg_start, rg_end)?;

        if let Some(ref words) = words {
            let expected_words = row_count.div_ceil(64);
            if words.len() != expected_words {
                return Err(format!(
                    "liveDocs callback returned {} words for {} rows; expected {}",
                    words.len(),
                    row_count,
                    expected_words
                ));
            }
            intersect_live_docs(&mut prefetched.candidates, words, row_count);
            if prefetched.candidates.is_empty() {
                return Ok(None);
            }
            prefetched.mask_buffer = Some(datafusion::arrow::buffer::Buffer::from_vec(
                bitmap_to_packed_bits(&prefetched.candidates, row_count as u32),
            ));
        }

        prefetched.eval_nanos = prefetched
            .eval_nanos
            .saturating_add(started.elapsed().as_nanos() as u64);
        prefetched.context = Box::new(LiveDocsRgContext {
            inner: prefetched.context,
            words,
            row_count,
        });
        Ok(Some(prefetched))
    }

    fn on_batch_mask(
        &self,
        rg_state: &dyn Any,
        rg_first_row: i64,
        position_map: &PositionMap,
        batch_offset: usize,
        batch_len: usize,
        batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        let state = rg_state
            .downcast_ref::<LiveDocsRgContext>()
            .ok_or_else(|| "LiveDocsEvaluator: unexpected row-group context".to_string())?;
        let inner_mask = self.inner.on_batch_mask(
            state.inner.as_ref(),
            rg_first_row,
            position_map,
            batch_offset,
            batch_len,
            batch,
        )?;

        let Some(words) = state.words.as_ref() else {
            return Ok(inner_mask);
        };
        let Some(inner_mask) = inner_mask else {
            return Ok(None);
        };
        if inner_mask.len() != batch_len {
            return Err(format!(
                "LiveDocsEvaluator: inner mask length {} != batch length {}",
                inner_mask.len(),
                batch_len
            ));
        }

        let mut live_builder = BooleanBuilder::with_capacity(batch_len);
        for index in 0..batch_len {
            let delivered = batch_offset + index;
            let row = position_map.rg_position(delivered).ok_or_else(|| {
                format!(
                    "LiveDocsEvaluator: delivered row {} is outside PositionMap",
                    delivered
                )
            })?;
            live_builder.append_value(is_live(words, row, state.row_count)?);
        }
        let live_mask = live_builder.finish();
        let combined =
            datafusion::arrow::compute::kernels::boolean::and_kleene(&inner_mask, &live_mask)
                .map_err(|error| {
                    format!("LiveDocsEvaluator: mask intersection failed: {}", error)
                })?;
        Ok(Some(combined))
    }

    fn needs_row_mask(&self) -> bool {
        self.inner.needs_row_mask()
    }

    fn forbid_parquet_pushdown(&self) -> bool {
        self.inner.forbid_parquet_pushdown()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;

    #[test]
    fn intersection_removes_only_deleted_candidates() {
        let mut candidates = RoaringBitmap::from_iter([0u32, 1, 2, 3, 64, 65, 66, 69]);
        intersect_live_docs(&mut candidates, &[0b1101, 0b100101], 70);
        assert_eq!(
            candidates.iter().collect::<Vec<_>>(),
            vec![0, 2, 3, 64, 66, 69]
        );
    }

    #[test]
    fn partial_final_word_ignores_bits_outside_row_group() {
        let mut candidates = RoaringBitmap::from_iter(0u32..70);
        intersect_live_docs(&mut candidates, &[u64::MAX, 0b11], 70);
        assert_eq!(
            candidates.iter().collect::<Vec<_>>(),
            (0u32..66).collect::<Vec<_>>()
        );
    }
}
