/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared helpers for evaluators (SingleCollector, PredicateOnly, Tree).

use std::sync::Arc;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNode;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;

/// Evaluate a residual predicate against a batch, returning a BooleanArray mask.
pub fn evaluate_residual(
    residual: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
    batch_len: usize,
) -> Result<BooleanArray, String> {
    let remapped = remap_expr_to_batch(residual, batch)?;
    let value = remapped
        .evaluate(batch)
        .map_err(|e| format!("evaluate_residual: {}", e))?;
    let array = value
        .into_array(batch_len)
        .map_err(|e| format!("evaluate_residual into_array: {}", e))?;
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "evaluate_residual: did not produce BooleanArray".to_string())
        .cloned()
}

/// Remap column references in a PhysicalExpr to match the batch schema.
/// The expression may reference columns by index in the full table schema,
/// but the batch only contains projected columns. This rewrites Column
/// expressions to use the batch's field positions by name lookup.
pub fn remap_expr_to_batch(
    expr: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> Result<Arc<dyn PhysicalExpr>, String> {
    let batch_schema = batch.schema();
    expr.clone()
        .transform(|node| {
            use datafusion::common::tree_node::Transformed;
            if let Some(col) = node.downcast_ref::<Column>() {
                if let Ok(idx) = batch_schema.index_of(col.name()) {
                    let new_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(col.name(), idx));
                    return Ok(Transformed::yes(new_col));
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .map_err(|e| format!("remap_expr_to_batch: {}", e))
}
