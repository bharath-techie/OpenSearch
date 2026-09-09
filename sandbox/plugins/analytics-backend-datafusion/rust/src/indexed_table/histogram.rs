/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! PR3 Stage C2 — histogram exec (Fix 5).
//!
//! For a `span()` histogram (`SELECT span(ts, N) AS b, count(*) FROM t GROUP BY
//! b`) the per-RG classifier ([`super::row_group_plan::RowGroupPlan::HistogramBucket`])
//! proves that an interior row group's whole `[min, max]` maps to a SINGLE
//! bucket `b`. Every row of such an RG contributes `(b, num_matching_rows)` — no
//! decode needed. This module wires that up:
//!
//! * A [`HistogramSink`] (one map per leaf output partition) accumulates the
//!   `bucket -> count` contributions the streaming loop gathers for interior
//!   RGs (via the `count_rg` docFreq short-circuit — see
//!   `stream::IndexedStream::poll_inner`).
//! * [`install_histogram_rewrite`] runs once, right after the aggregate-mode
//!   transform, when the planner declared a `Histogram` shape. It finds the
//!   partial `count(*)` aggregate over exactly one indexed leaf, installs a
//!   fresh sink on that leaf, and wraps the aggregate in
//!   [`HistogramCountsExec`]. Boundary/non-fast-path RGs still decode and feed
//!   the partial aggregate normally; the interior counts arrive as one extra
//!   partial batch appended after the aggregate drains, so the FINAL aggregate
//!   sums the two sources per bucket exactly.
//!
//! Fail-closed everywhere: any plan shape that doesn't match the exact
//! `Aggregate(Partial, 1 group, 1 count, [group, Int64] schema) → …passthrough…
//! → one QueryShardExec` template leaves the plan unchanged, so the RGs decode
//! fully (correct, just not accelerated) and the sink is never consulted.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::StreamExt;

use crate::fast_path_hints::FastPathHints;

/// Per-partition `bucket_tick -> count` accumulator. One [`Mutex<HashMap>`] per
/// leaf output partition; the leaf's streaming loop adds interior-RG counts
/// into its own partition's map, and [`HistogramCountsExec`] drains them after
/// the wrapped aggregate finishes for that partition.
pub type HistogramSink = Arc<Vec<Mutex<HashMap<i64, u64>>>>;

/// Build an empty sink sized to `partitions` leaf output partitions.
pub fn new_sink(partitions: usize) -> HistogramSink {
    Arc::new(
        (0..partitions)
            .map(|_| Mutex::new(HashMap::new()))
            .collect(),
    )
}

// ── HistogramCountsExec ──────────────────────────────────────────────

/// Wraps the partial `count(*)` aggregate. Forwards every partial batch from
/// its input unchanged, then — once the input for that partition ends — appends
/// ONE partial batch built from the sink's `(bucket, count)` entries for that
/// partition (skipped when the partition's map is empty). Output schema is the
/// input's, so the downstream FINAL aggregate consumes both sources uniformly.
pub struct HistogramCountsExec {
    /// The partial `AggregateExec` (its subtree holds the sink-carrying leaf).
    input: Arc<dyn ExecutionPlan>,
    /// Shared per-partition bucket accumulator, filled by the leaf's stream.
    sink: HistogramSink,
    /// Arrow type of the group (bucket) column — the input schema's field 0.
    /// Interior bucket ticks are physical-unit `i64`s cast into this type.
    group_type: DataType,
    /// Mirrors the input's properties (we neither repartition nor reorder).
    props: Arc<PlanProperties>,
}

impl HistogramCountsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, sink: HistogramSink, group_type: DataType) -> Self {
        let props = input.properties().clone();
        Self {
            input,
            sink,
            group_type,
            props,
        }
    }

    /// Build the single trailing partial batch from `sink[partition]`, or `None`
    /// when the partition contributed no interior buckets. Column 0 = bucket
    /// values (physical-unit ticks cast to `group_type`); column 1 = counts as
    /// `Int64` (the shape of a `count(*)` partial state).
    fn build_tail_batch(
        schema: &SchemaRef,
        sink: &HistogramSink,
        partition: usize,
        group_type: &DataType,
    ) -> Result<Option<RecordBatch>> {
        let map = match sink.get(partition) {
            Some(m) => m.lock().unwrap(),
            None => return Ok(None),
        };
        if map.is_empty() {
            return Ok(None);
        }
        let mut buckets: Vec<i64> = Vec::with_capacity(map.len());
        let mut counts: Vec<i64> = Vec::with_capacity(map.len());
        for (&b, &c) in map.iter() {
            buckets.push(b);
            counts.push(c as i64);
        }
        // Group ticks are physical-unit i64s; cast into the declared group type
        // (Int64 stays identity; Timestamp(unit, tz) reinterprets the same tick).
        let group_i64: ArrayRef = Arc::new(Int64Array::from(buckets));
        let group_col: ArrayRef = match group_type {
            DataType::Int64 => group_i64,
            DataType::Timestamp(_, _) => cast(&group_i64, group_type)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            other => {
                return Err(DataFusionError::Internal(format!(
                    "HistogramCountsExec: unsupported group type {other:?}"
                )))
            }
        };
        let count_col: ArrayRef = Arc::new(Int64Array::from(counts));
        let batch = RecordBatch::try_new(schema.clone(), vec![group_col, count_col])
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok(Some(batch))
    }
}

impl fmt::Debug for HistogramCountsExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HistogramCountsExec")
    }
}

impl DisplayAs for HistogramCountsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "HistogramCountsExec: group_type={:?}", self.group_type)
    }
}

impl ExecutionPlan for HistogramCountsExec {
    fn name(&self) -> &str {
        "HistogramCountsExec"
    }
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    /// This exec owns no physical expressions of its own (the wrapped aggregate
    /// carries them, and DataFusion recurses into it as a child). This runs
    /// post-optimization, so no further expression pushdown depends on it.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("HistogramCountsExec needs one child".into())
        })?;
        Ok(Arc::new(HistogramCountsExec::new(
            input,
            Arc::clone(&self.sink),
            self.group_type.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let input_stream = self.input.execute(partition, context)?;
        let sink = Arc::clone(&self.sink);
        let group_type = self.group_type.clone();
        let tail_schema = schema.clone();
        // `stream::once` is lazy: its future runs only after `chain` has drained
        // `input_stream`, i.e. after every interior-RG count for this partition
        // has landed in the sink. `filter_map` drops the `None` (empty map).
        let tail = futures::stream::once(async move {
            Self::build_tail_batch(&tail_schema, &sink, partition, &group_type).transpose()
        })
        .filter_map(|opt| async move { opt });
        let combined = input_stream.chain(tail);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, combined)))
    }
}

// ── install_histogram_rewrite ────────────────────────────────────────

/// Whether `plan` is a partial `count(*)` aggregate we can intercept: mode
/// `Partial`/`Single`, one group key, one aggregate, and a two-column
/// `[group, Int64]` output whose group column is `Int64` or `Timestamp`.
fn matches_partial_count(agg: &AggregateExec) -> Option<DataType> {
    if !matches!(agg.mode(), AggregateMode::Partial | AggregateMode::Single) {
        return None;
    }
    if agg.group_expr().expr().len() != 1 || agg.aggr_expr().len() != 1 {
        return None;
    }
    let schema = agg.schema();
    if schema.fields().len() != 2 {
        return None;
    }
    // count(*) partial state is a single Int64 column.
    if schema.field(1).data_type() != &DataType::Int64 {
        return None;
    }
    let group_type = schema.field(0).data_type().clone();
    match group_type {
        DataType::Int64 | DataType::Timestamp(_, _) => Some(group_type),
        _ => None,
    }
}

/// Walk single-child passthroughs from `plan` to the one indexed leaf
/// (`QueryShardExec`), installing a fresh [`HistogramSink`] on it. Returns the
/// rewritten subtree and the sink, or `None` if the chain branches or reaches a
/// non-leaf terminal (fail closed).
fn install_sink_on_leaf(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Option<(Arc<dyn ExecutionPlan>, HistogramSink)>> {
    if let Some(qse) = plan.downcast_ref::<super::table_provider::QueryShardExec>() {
        let partitions = plan.output_partitioning().partition_count().max(1);
        let sink = new_sink(partitions);
        let new_leaf =
            Arc::new(qse.clone_with_histogram_sink(Arc::clone(&sink))) as Arc<dyn ExecutionPlan>;
        return Ok(Some((new_leaf, sink)));
    }
    let children = plan.children();
    if children.len() != 1 {
        return Ok(None);
    }
    match install_sink_on_leaf(Arc::clone(children[0]))? {
        Some((new_child, sink)) => {
            let rebuilt = Arc::clone(&plan).with_new_children(vec![new_child])?;
            Ok(Some((rebuilt, sink)))
        }
        None => Ok(None),
    }
}

/// Install the histogram fast path (Fix 5) on `plan`, top-down.
///
/// Finds the first partial `count(*)` aggregate matching [`matches_partial_count`]
/// whose subtree reaches exactly one `QueryShardExec` through single-child
/// passthroughs; installs a sink on that leaf and wraps the aggregate in
/// [`HistogramCountsExec`]. Any mismatch returns the plan unchanged (fail
/// closed) — the RGs then decode fully. Called only when the planner declared a
/// `Histogram` shape (`hints.shape == Histogram`).
pub fn install_histogram_rewrite(
    plan: Arc<dyn ExecutionPlan>,
    hints: &FastPathHints,
) -> Result<Arc<dyn ExecutionPlan>> {
    debug_assert_eq!(
        hints.shape,
        crate::fast_path_hints::FastPathShape::Histogram
    );

    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        if let Some(group_type) = matches_partial_count(agg) {
            match install_sink_on_leaf(Arc::clone(agg.input()))? {
                Some((new_input, sink)) => {
                    let new_agg = Arc::clone(&plan).with_new_children(vec![new_input])?;
                    return Ok(Arc::new(HistogramCountsExec::new(
                        new_agg, sink, group_type,
                    )));
                }
                None => {
                    native_bridge_common::log_debug!(
                        "[fast-path-histogram] partial count matched but no single QueryShardExec leaf; leaving plan unchanged"
                    );
                    // Fall through: no rewrite at this node.
                }
            }
        }
    }

    // Recurse into a single child (the Final aggregate sits above the Partial).
    let children = plan.children();
    if children.len() == 1 {
        let child = Arc::clone(children[0]);
        let new_child = install_histogram_rewrite(child, hints)?;
        if !Arc::ptr_eq(&new_child, children[0]) {
            return plan.with_new_children(vec![new_child]);
        }
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::TimestampMillisecondArray;
    use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::Partitioning;
    use futures::StreamExt as _;

    /// Minimal single-partition leaf that replays one prebuilt batch — a
    /// self-contained stand-in for the wrapped partial aggregate's output (avoids
    /// depending on a specific DF `MemoryExec` API surface).
    #[derive(Debug)]
    struct VecExec {
        batch: RecordBatch,
        props: Arc<PlanProperties>,
    }

    impl VecExec {
        fn new(batch: RecordBatch) -> Self {
            let schema = batch.schema();
            let props = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self { batch, props }
        }
    }

    impl DisplayAs for VecExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "VecExec")
        }
    }

    impl ExecutionPlan for VecExec {
        fn name(&self) -> &str {
            "VecExec"
        }
        fn schema(&self) -> SchemaRef {
            self.batch.schema()
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.props
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let schema = self.batch.schema();
            let batch = self.batch.clone();
            let s = futures::stream::once(async move { Ok(batch) });
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
        }
    }

    /// Build a [`HistogramCountsExec`] over a `VecExec` input carrying
    /// `input_rows` (already in the partial `[group, count]` shape) plus a
    /// pre-filled single-partition sink, then collect the emitted rows.
    async fn run_exec(
        group_type: DataType,
        input_rows: Vec<(i64, i64)>,
        sink_entries: Vec<(i64, u64)>,
    ) -> Vec<(i64, i64)> {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("b", group_type.clone(), true),
            Field::new("cnt", DataType::Int64, true),
        ]));
        let g_i64: ArrayRef = Arc::new(Int64Array::from(
            input_rows.iter().map(|(g, _)| *g).collect::<Vec<_>>(),
        ));
        let g_col: ArrayRef = match group_type {
            DataType::Int64 => g_i64,
            DataType::Timestamp(_, _) => cast(&g_i64, &group_type).unwrap(),
            _ => unreachable!(),
        };
        let c_col: ArrayRef = Arc::new(Int64Array::from(
            input_rows.iter().map(|(_, c)| *c).collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![g_col, c_col]).unwrap();
        let input = Arc::new(VecExec::new(batch)) as Arc<dyn ExecutionPlan>;

        let sink = new_sink(1);
        {
            let mut m = sink[0].lock().unwrap();
            for (b, c) in sink_entries {
                *m.entry(b).or_insert(0) += c;
            }
        }
        let exec = HistogramCountsExec::new(input, sink, group_type.clone());

        let ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, ctx).unwrap();
        let mut out: Vec<(i64, i64)> = Vec::new();
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            let counts = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            let groups_i64: Vec<i64> = match group_type {
                DataType::Int64 => {
                    let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    (0..b.num_rows()).map(|i| a.value(i)).collect()
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    let a = b
                        .column(0)
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    (0..b.num_rows()).map(|i| a.value(i)).collect()
                }
                _ => unreachable!(),
            };
            for i in 0..b.num_rows() {
                out.push((groups_i64[i], counts.value(i)));
            }
        }
        out.sort_unstable();
        out
    }

    #[tokio::test]
    async fn forwards_input_then_appends_sink_int64() {
        // Boundary partial rows: (100, 3). Interior sink: bucket 0 -> 8, 200 -> 4.
        let out = run_exec(DataType::Int64, vec![(100, 3)], vec![(0, 8), (200, 4)]).await;
        assert_eq!(out, vec![(0, 8), (100, 3), (200, 4)]);
    }

    #[tokio::test]
    async fn empty_sink_emits_only_input() {
        let out = run_exec(DataType::Int64, vec![(100, 3)], vec![]).await;
        assert_eq!(
            out,
            vec![(100, 3)],
            "no trailing batch when the sink is empty"
        );
    }

    #[tokio::test]
    async fn timestamp_group_type_roundtrips_ticks() {
        let ts = DataType::Timestamp(TimeUnit::Millisecond, None);
        let out = run_exec(ts, vec![], vec![(1_000, 5), (2_000, 7)]).await;
        assert_eq!(out, vec![(1_000, 5), (2_000, 7)]);
    }
}
