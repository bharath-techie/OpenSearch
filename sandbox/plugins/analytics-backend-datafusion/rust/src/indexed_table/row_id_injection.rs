/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shard-global `__row_id__` for the fetch phase (QTF).
//!
//! `__row_id__` is a physical `INT64` column in every OpenSearch parquet file
//! holding the row's position *within that file*. QTF needs values unique across
//! the shard, so the stored value is offset by the segment's `global_base` (the
//! cumulative row count of all preceding segments).
//!
//! This used to *compute* the value from delivery position via a `PositionMap`,
//! which meant keeping that map in lockstep with whatever the decoder skipped.
//! Reading the stored column makes the value correct by construction: it travels
//! with the row, so a `RowSelection` or `RowFilter` dropping rows cannot shift it.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion_common::DataFusionError;

/// Rebase the batch's `__row_id__` to shard-global values and project to
/// `output_schema`.
///
/// `row_id_idx` is the column's position in `output_schema`. Remaining columns
/// are reordered to match that schema, since the parquet reader delivers them in
/// the file's physical order which need not agree.
pub fn rebase_row_ids(
    batch: &RecordBatch,
    global_base: u64,
    row_id_idx: usize,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let stored = batch
        .column_by_name(crate::ROW_ID_COLUMN_NAME)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "row-id output requested but {} is not in the delivered batch",
                crate::ROW_ID_COLUMN_NAME
            ))
        })?;
    let stored_ids = stored
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{} must be Int64, got {:?}",
                crate::ROW_ID_COLUMN_NAME,
                stored.data_type()
            ))
        })?;

    let base = i64::try_from(global_base).map_err(|_| {
        DataFusionError::Internal(format!("segment global_base {global_base} exceeds i64"))
    })?;
    // `__row_id__` is REQUIRED in the file schema, so there is no null mask to
    // carry over and a plain values map suffices.
    let rebased: ArrayRef = Arc::new(Int64Array::from_iter_values(
        stored_ids.values().iter().map(|&v| v + base),
    ));

    let columns: Vec<ArrayRef> = output_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            if i == row_id_idx {
                return Ok(Arc::clone(&rebased));
            }
            let idx = batch.schema().index_of(field.name()).map_err(|_| {
                DataFusionError::Internal(format!(
                    "output column {} missing from the delivered batch",
                    field.name()
                ))
            })?;
            Ok(Arc::clone(batch.column(idx)))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_new_with_options(
        Arc::clone(output_schema),
        columns,
        &datafusion::arrow::record_batch::RecordBatchOptions::new()
            .with_row_count(Some(batch.num_rows())),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn batch(row_ids: Vec<i64>, vals: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            Field::new(crate::ROW_ID_COLUMN_NAME, DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vals)),
                Arc::new(Int64Array::from(row_ids)),
            ],
        )
        .unwrap()
    }

    fn out_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            Field::new(crate::ROW_ID_COLUMN_NAME, DataType::Int64, false),
        ]))
    }

    fn ids_of(batch: &RecordBatch, idx: usize) -> Vec<i64> {
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn adds_global_base_to_stored_ids() {
        let b = batch(vec![0, 1, 2], vec![10, 11, 12]);
        let out = rebase_row_ids(&b, 1_000, 1, &out_schema()).unwrap();
        assert_eq!(ids_of(&out, 1), vec![1000, 1001, 1002]);
    }

    /// Gapped delivery is what the old position-derived implementation had to
    /// work to get right: with rows skipped, delivered row `i` is not row `i` of
    /// the row group. Reading the stored value makes it fall out for free.
    #[test]
    fn preserves_gaps_from_a_row_selection() {
        let b = batch(vec![5, 9, 40], vec![1, 2, 3]);
        let out = rebase_row_ids(&b, 100, 1, &out_schema()).unwrap();
        assert_eq!(ids_of(&out, 1), vec![105, 109, 140]);
    }

    #[test]
    fn zero_base_is_identity() {
        let b = batch(vec![7, 8], vec![1, 2]);
        let out = rebase_row_ids(&b, 0, 1, &out_schema()).unwrap();
        assert_eq!(ids_of(&out, 1), vec![7, 8]);
    }

    #[test]
    fn empty_batch_yields_empty_ids() {
        let b = batch(vec![], vec![]);
        let out = rebase_row_ids(&b, 42, 1, &out_schema()).unwrap();
        assert_eq!(out.num_rows(), 0);
        assert!(ids_of(&out, 1).is_empty());
    }

    #[test]
    fn reorders_columns_to_output_schema() {
        let b = batch(vec![0, 1], vec![10, 11]);
        // Output wants row_id first — the reverse of the delivered order.
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new(crate::ROW_ID_COLUMN_NAME, DataType::Int64, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let out = rebase_row_ids(&b, 5, 0, &schema).unwrap();
        assert_eq!(ids_of(&out, 0), vec![5, 6]);
        assert_eq!(out.schema().field(1).name(), "v");
    }

    #[test]
    fn missing_column_errors_rather_than_guessing() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
        assert!(rebase_row_ids(&b, 0, 1, &out_schema()).is_err());
    }
}
