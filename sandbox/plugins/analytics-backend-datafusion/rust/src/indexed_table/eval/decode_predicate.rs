/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Refinement as a parquet [`ArrowPredicate`], evaluated during decode.
//!
//! An evaluator's refinement stage answers "of the rows the candidate stage
//! admitted, which ones actually match?". Running that on fully decoded batches
//! means every projected column is materialized for rows that are about to be
//! thrown away. Running it as an `ArrowPredicate` instead lets parquet decode
//! only the columns the refinement reads, apply the mask, and decode the
//! projection for survivors only.
//!
//! ```text
//!   post-decode (on_batch_mask)      during decode (this module)
//!   ───────────────────────────      ───────────────────────────
//!   decode a, b, c, __row_id__       decode __row_id__ (+ residual cols)
//!   mask                             mask
//!   drop non-matching rows           decode a, b, c for survivors only
//! ```
//!
//! # Identifying rows
//!
//! A predicate cannot use a row's position in the batch to identify it: earlier
//! predicates and the row group's `RowSelection` have already dropped rows, so
//! position *i* is not row *i* of the row group. Every predicate here therefore
//! projects `__row_id__` — a physical INT64 column holding the row's position
//! within the *file* — and derives everything from that value.
//!
//! Deriving the row group from the row id (rather than being told which row
//! group is being decoded) is what lets one predicate serve a whole file, which
//! is the granularity parquet builds `RowFilter`s at.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Int64Array, RecordBatch};
use datafusion::arrow::error::ArrowError;
use datafusion::parquet::arrow::arrow_reader::ArrowPredicate;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::schema::types::SchemaDescriptor;

use super::{RowGroupBitsetSource, RowPositions};
use crate::indexed_table::stream::RowGroupInfo;

/// Maps a file row position to the row group containing it.
///
/// Row groups are contiguous and ordered, so this is a binary search over their
/// start offsets.
#[derive(Debug, Clone)]
pub struct RowGroupLocator {
    /// `(first_row, num_rows, rg)` per row group, ascending by `first_row`.
    row_groups: Arc<Vec<RowGroupInfo>>,
}

impl RowGroupLocator {
    pub fn new(mut row_groups: Vec<RowGroupInfo>) -> Self {
        row_groups.sort_by_key(|rg| rg.first_row);
        Self {
            row_groups: Arc::new(row_groups),
        }
    }

    /// The row group containing file row `row_id`, or `None` if the row falls
    /// outside every row group in the chunk.
    pub fn locate(&self, row_id: i64) -> Option<&RowGroupInfo> {
        let idx = self
            .row_groups
            .partition_point(|rg| rg.first_row <= row_id)
            .checked_sub(1)?;
        let rg = self.row_groups.get(idx)?;
        (row_id < rg.first_row + rg.num_rows).then_some(rg)
    }
}

/// Build the projection a decode-time predicate needs: `__row_id__` plus every
/// column its refinement reads.
///
/// An `ArrowPredicate` is handed *only* the columns its mask names, so omitting
/// a column the refinement evaluates makes the expression fail against the
/// batch. `__row_id__` is always included — it is how the predicate identifies
/// rows.
///
/// Returns `None` when the file has no `__row_id__` column, which means the
/// predicate cannot identify rows and must not be installed.
pub fn refinement_projection(
    schema_descr: &SchemaDescriptor,
    refinement_columns: &[String],
) -> Option<ProjectionMask> {
    let fields = schema_descr.root_schema().get_fields();
    let position = |name: &str| fields.iter().position(|field| field.name() == name);

    let mut roots = vec![position(crate::ROW_ID_COLUMN_NAME)?];
    for column in refinement_columns {
        // A refinement column absent from this file is schema drift, which the
        // refinement itself handles (an absent column reads as SQL UNKNOWN), so
        // skip it rather than failing the scan.
        if let Some(root) = position(column) {
            if !roots.contains(&root) {
                roots.push(root);
            }
        }
    }
    Some(ProjectionMask::roots(schema_descr, roots))
}

/// Evaluates an evaluator's refinement during decode.
///
/// Holds the evaluator and the per-row-group state its refinement needs, and
/// routes each batch to the right row group by reading `__row_id__`.
pub(in crate::indexed_table) struct RefinementPredicate {
    evaluator: Arc<dyn RowGroupBitsetSource>,
    locator: RowGroupLocator,
    projection: ProjectionMask,
    /// Per-row-group state from `prefetch_rg`, keyed by row-group index.
    ///
    /// Populated by the access provider before the row group is decoded. A row
    /// group absent from this map was never prefetched, which is a bug rather
    /// than a recoverable state — refining without the candidate set would
    /// silently admit non-candidates.
    contexts: Arc<super::super::access_provider::RgContextStore>,
}

impl std::fmt::Debug for RefinementPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefinementPredicate")
            .finish_non_exhaustive()
    }
}

impl RefinementPredicate {
    pub(in crate::indexed_table) fn new(
        evaluator: Arc<dyn RowGroupBitsetSource>,
        locator: RowGroupLocator,
        projection: ProjectionMask,
        contexts: Arc<super::super::access_provider::RgContextStore>,
    ) -> Self {
        Self {
            evaluator,
            locator,
            projection,
            contexts,
        }
    }

    fn row_ids(batch: &RecordBatch) -> Result<Int64Array, ArrowError> {
        let column = batch
            .column_by_name(crate::ROW_ID_COLUMN_NAME)
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "{} must be projected for decode-time refinement",
                    crate::ROW_ID_COLUMN_NAME
                ))
            })?;
        column
            .as_any()
            .downcast_ref::<Int64Array>()
            .cloned()
            .ok_or_else(|| {
                ArrowError::CastError(format!(
                    "{} must be Int64, got {:?}",
                    crate::ROW_ID_COLUMN_NAME,
                    column.data_type()
                ))
            })
    }
}

impl ArrowPredicate for RefinementPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        if batch.num_rows() == 0 {
            return Ok(BooleanArray::new(
                datafusion::arrow::buffer::BooleanBuffer::new_unset(0),
                None,
            ));
        }

        let row_ids = Self::row_ids(&batch)?;

        // Parquet delivers a batch from exactly one row group, so the first row
        // id identifies the row group for the whole batch.
        let first = row_ids.value(0);
        let rg = self.locator.locate(first).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "file row {first} is not in any row group of this chunk"
            ))
        })?;
        let rg_index = rg.index;
        let rg_first_row = rg.first_row;

        let context = self.contexts.peek(rg_index).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "row group {rg_index} was decoded without being prefetched"
            ))
        })?;

        let row_positions = RowPositions::new(&row_ids, rg_first_row);
        let mask = self
            .evaluator
            .on_batch_mask(context.state(), rg_first_row, &row_positions, &batch)
            .map_err(|e| ArrowError::ComputeError(format!("refinement failed: {e}")))?;

        // `None` means the candidate stage was already exact for this row group,
        // so every delivered row survives.
        Ok(match mask {
            Some(mask) => mask,
            None => BooleanArray::new(
                datafusion::arrow::buffer::BooleanBuffer::new_set(batch.num_rows()),
                None,
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rgs(spec: &[(usize, i64, i64)]) -> RowGroupLocator {
        RowGroupLocator::new(
            spec.iter()
                .map(|&(index, first_row, num_rows)| RowGroupInfo {
                    index,
                    first_row,
                    num_rows,
                })
                .collect(),
        )
    }

    #[test]
    fn locates_row_in_first_row_group() {
        let locator = rgs(&[(0, 0, 10), (1, 10, 10)]);
        assert_eq!(locator.locate(0).unwrap().index, 0);
        assert_eq!(locator.locate(9).unwrap().index, 0);
    }

    #[test]
    fn locates_row_at_row_group_boundary() {
        let locator = rgs(&[(0, 0, 10), (1, 10, 10)]);
        // 10 is the first row of RG1, not the last of RG0.
        assert_eq!(locator.locate(10).unwrap().index, 1);
        assert_eq!(locator.locate(19).unwrap().index, 1);
    }

    #[test]
    fn rejects_row_past_the_end() {
        let locator = rgs(&[(0, 0, 10), (1, 10, 10)]);
        assert!(locator.locate(20).is_none());
    }

    #[test]
    fn rejects_negative_row() {
        let locator = rgs(&[(0, 0, 10)]);
        assert!(locator.locate(-1).is_none());
    }

    /// A chunk holds a contiguous slice of a file's row groups, so the first
    /// row group need not start at row 0.
    #[test]
    fn locates_within_a_chunk_that_does_not_start_at_zero() {
        let locator = rgs(&[(3, 300, 100), (4, 400, 100)]);
        assert!(locator.locate(299).is_none());
        assert_eq!(locator.locate(300).unwrap().index, 3);
        assert_eq!(locator.locate(399).unwrap().index, 3);
        assert_eq!(locator.locate(400).unwrap().index, 4);
        assert!(locator.locate(500).is_none());
    }

    /// Row groups are sorted on construction, so an out-of-order chunk plan
    /// still locates correctly.
    #[test]
    fn sorts_row_groups_on_construction() {
        let locator = rgs(&[(1, 10, 10), (0, 0, 10)]);
        assert_eq!(locator.locate(5).unwrap().index, 0);
        assert_eq!(locator.locate(15).unwrap().index, 1);
    }

    /// A gap between row groups — possible when the access plan skipped one —
    /// must not be attributed to the row group before it.
    #[test]
    fn rejects_row_in_a_skipped_row_group() {
        let locator = rgs(&[(0, 0, 10), (2, 20, 10)]);
        assert!(locator.locate(15).is_none());
        assert_eq!(locator.locate(25).unwrap().index, 2);
    }
}
