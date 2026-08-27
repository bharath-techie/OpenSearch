/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Histogram fast path — answer `count() by span(_timestamp, N)` from footer
//! statistics + Lucene cardinalities instead of decoding every timestamp.
//!
//! Plan shape (from PPL `stats count() by span(ts, N)`):
//!
//! ```text
//! AggregateExec(Final…)                       ← untouched
//!   RepartitionExec(hash on bucket)           ← untouched
//!     AggregateExec(mode=Partial, gby=[b@0], aggr=[count(1)])
//!       ProjectionExec(expr=[ts/α/β*β as b])  ┐ replaced by
//!         QueryShardExec                      ┘ UnionExec[boundary, counts]
//! ```
//!
//! The rewrite keeps the original subtree for BOUNDARY row groups (those the
//! footer stats cannot prove single-bucket + WITHIN) and adds a
//! [`HistogramCountsExec`] emitting partial-aggregate rows `(bucket, count)`
//! for INTERIOR row groups — no parquet IO, counts via the evaluator's
//! `count_docs_range` (row counts when predicate-only; one forward Lucene
//! cursor per chunk when a Lucene leaf filters).
//!
//! Fail-closed gates:
//! - `sort_range_within_rgs` present (range residual proven tautological and
//!   the filter tree shape supports single-leaf counting);
//! - bucket expression is monotonic integer arithmetic referencing ONLY the
//!   leading sort column (`+ - * / %`… restricted to `Div/Mul/Add/Sub` with
//!   positive integer literals on the right);
//! - Partial aggregate is exactly one `count(1)` with a single group key;
//! - bucket values computed by evaluating DataFusion's OWN expression on the
//!   footer min/max (semantics identical by construction), interior iff both
//!   evaluate equal.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

use super::metrics::StreamMetrics;
use super::partitioning::SegmentChunk;
use super::table_provider::QueryShardExec;

/// One contiguous run of interior row groups sharing a bucket value.
#[derive(Debug, Clone)]
struct BucketRun {
    bucket: i64,
    doc_min: i32,
    doc_max: i32,
}

/// Interior work for one chunk: the runs plus the chunk identity needed to
/// build an evaluator at execute time.
#[derive(Debug, Clone)]
struct ChunkCounts {
    chunk: SegmentChunk,
    runs: Vec<BucketRun>,
}

/// True when `expr` is monotonic (non-decreasing) integer arithmetic over
/// exactly the column named `ts_name`: `Column`, or `BinaryExpr` of
/// `Div/Mul/Add/Sub` whose right side is a positive integer literal and whose
/// left side recurses. This covers PPL `span()` (`ts / a / w * w`).
fn bucket_expr_is_monotonic_int_arith(expr: &Arc<dyn PhysicalExpr>, ts_name: &str) -> bool {
    if let Some(col) = expr.as_ref().downcast_ref::<Column>() {
        return col.name() == ts_name;
    }
    if let Some(bin) = expr.as_ref().downcast_ref::<BinaryExpr>() {
        let rhs_pos_int = bin
            .right()
            .downcast_ref::<Literal>()
            .and_then(|l| match l.value() {
                ScalarValue::Int64(Some(v)) => Some(*v > 0),
                ScalarValue::Int32(Some(v)) => Some(*v > 0),
                _ => None,
            })
            .unwrap_or(false);
        if !rhs_pos_int {
            return false;
        }
        return matches!(
            bin.op(),
            Operator::Divide | Operator::Multiply | Operator::Plus | Operator::Minus
        ) && bucket_expr_is_monotonic_int_arith(bin.left(), ts_name);
    }
    false
}

/// Evaluate `expr` (over `scan_schema`) with the sort column set to `ts_value`
/// and every other column null. Whitelisted expressions reference only the
/// sort column, so the nulls are never consumed. Returns the resulting i64.
fn eval_bucket_at(
    expr: &Arc<dyn PhysicalExpr>,
    scan_schema: &SchemaRef,
    ts_index: usize,
    ts_value: i64,
) -> Option<i64> {
    let columns: Vec<ArrayRef> = scan_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| -> ArrayRef {
            if i == ts_index {
                match field.data_type() {
                    DataType::Int64 => Arc::new(Int64Array::from(vec![ts_value])),
                    _ => datafusion::arrow::array::new_null_array(field.data_type(), 1),
                }
            } else {
                datafusion::arrow::array::new_null_array(field.data_type(), 1)
            }
        })
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(scan_schema), columns).ok()?;
    let value = expr.evaluate(&batch).ok()?.into_array(1).ok()?;
    match ScalarValue::try_from_array(&*value, 0).ok()? {
        ScalarValue::Int64(Some(v)) => Some(v),
        ScalarValue::Int32(Some(v)) => Some(v as i64),
        _ => None,
    }
}

/// Try to rewrite `plan` (the full physical plan) for the histogram fast
/// path. Returns the rewritten plan, or the original when any gate fails.
pub(crate) fn try_rewrite_histogram(
    plan: Arc<dyn ExecutionPlan>,
    sort_field: &str,
) -> Arc<dyn ExecutionPlan> {
    match rewrite_node(&plan, sort_field) {
        Some(new_plan) => new_plan,
        None => plan,
    }
}

fn rewrite_node(node: &Arc<dyn ExecutionPlan>, sort_field: &str) -> Option<Arc<dyn ExecutionPlan>> {
    // Try to match THIS node as the Partial aggregate; otherwise recurse.
    if let Some(rewritten) = try_match_partial_agg(node, sort_field) {
        return Some(rewritten);
    }
    let mut changed = false;
    let mut children: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    for child in node.children() {
        match rewrite_node(child, sort_field) {
            Some(new_child) => {
                changed = true;
                children.push(new_child);
            }
            None => children.push(Arc::clone(child)),
        }
    }
    if !changed {
        return None;
    }
    Arc::clone(node).with_new_children(children).ok()
}

fn try_match_partial_agg(
    node: &Arc<dyn ExecutionPlan>,
    sort_field: &str,
) -> Option<Arc<dyn ExecutionPlan>> {
    let agg = node.as_ref().downcast_ref::<AggregateExec>()?;
    if *agg.mode() != AggregateMode::Partial {
        return None;
    }
    // Exactly one plain group expression, no grouping sets, one count(1)
    // aggregate without filters.
    let group = agg.group_expr();
    if group.expr().len() != 1 || !group.null_expr().is_empty() {
        return None;
    }
    if agg.aggr_expr().len() != 1 || agg.filter_expr().iter().any(|f| f.is_some()) {
        return None;
    }
    let aggr = &agg.aggr_expr()[0];
    // COUNT over a literal (count() / count(*) lowering). Fail closed on
    // anything else — e.g. count(col) has null semantics we must not fake.
    if aggr.fun().name() != "count" {
        return None;
    }
    let count_inputs = aggr.expressions();
    if !(count_inputs.len() == 1 && count_inputs[0].as_ref().downcast_ref::<Literal>().is_some()) {
        return None;
    }

    // Input chain: descend through pass-through nodes (Repartition/
    // CoalesceBatches) to at most ONE single-expr ProjectionExec and then the
    // QueryShardExec. The bucket expression over the scan schema is either
    // the projection's expr (group key must then be a plain col@0 reference)
    // or the group expr itself.
    let agg_input = agg.input();
    let mut cursor: &Arc<dyn ExecutionPlan> = agg_input;
    let mut projection_expr: Option<Arc<dyn PhysicalExpr>> = None;
    let shard: &QueryShardExec = loop {
        if let Some(shard) = cursor.as_ref().downcast_ref::<QueryShardExec>() {
            break shard;
        }
        if let Some(proj) = cursor.as_ref().downcast_ref::<ProjectionExec>() {
            if projection_expr.is_some() || proj.expr().len() != 1 {
                return None;
            }
            projection_expr = Some(Arc::clone(&proj.expr()[0].expr));
            cursor = proj.input();
            continue;
        }
        let passthrough = cursor
            .as_ref()
            .downcast_ref::<datafusion::physical_plan::repartition::RepartitionExec>()
            .is_some()
            || cursor
                .as_ref()
                .downcast_ref::<datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec>()
                .is_some();
        if !passthrough || cursor.children().len() != 1 {
            return None;
        }
        cursor = cursor.children()[0];
    };
    let bucket_expr: Arc<dyn PhysicalExpr> = match &projection_expr {
        Some(expr) => {
            // Group key must be a plain reference to the projection output.
            group.expr()[0]
                .0
                .as_ref()
                .downcast_ref::<Column>()
                .filter(|c| c.index() == 0)?;
            Arc::clone(expr)
        }
        None => Arc::clone(&group.expr()[0].0),
    };

    if !bucket_expr_is_monotonic_int_arith(&bucket_expr, sort_field) {
        return None;
    }

    let config = shard.indexed_config();
    let within = config.sort_range_within_rgs.as_ref()?;
    if config.emit_row_ids {
        return None;
    }
    let scan_schema = shard.schema();
    let ts_index = scan_schema.index_of(sort_field).ok()?;

    // Classify each segment's row groups: interior = WITHIN (residual
    // tautology) AND footer min/max land in the SAME bucket under
    // DataFusion's own expression. Everything else stays on the decode path.
    let mut interior_buckets: HashMap<usize, HashMap<usize, i64>> = HashMap::new();
    for (seg_idx, segment) in config.segments.iter().enumerate() {
        let Some(within_set) = within.get(&seg_idx) else {
            continue;
        };
        let converter =
            datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter::try_new(
                sort_field,
                &segment.arrow_schema,
                segment.metadata.file_metadata().schema_descr(),
            )
            .ok()?;
        let row_groups = segment.metadata.row_groups();
        let mins = converter.row_group_mins(row_groups.iter()).ok()?;
        let maxes = converter.row_group_maxes(row_groups.iter()).ok()?;
        let mut per_seg = HashMap::new();
        for rg in 0..row_groups.len() {
            if !within_set.contains(&rg) {
                continue;
            }
            let lo = ScalarValue::try_from_array(&*mins, rg)
                .ok()
                .as_ref()
                .and_then(crate::indexed_executor::scalar_as_i64);
            let hi = ScalarValue::try_from_array(&*maxes, rg)
                .ok()
                .as_ref()
                .and_then(crate::indexed_executor::scalar_as_i64);
            let (Some(lo), Some(hi)) = (lo, hi) else {
                continue;
            };
            let b_lo = eval_bucket_at(&bucket_expr, &scan_schema, ts_index, lo)?;
            let b_hi = eval_bucket_at(&bucket_expr, &scan_schema, ts_index, hi)?;
            if b_lo == b_hi {
                per_seg.insert(rg, b_lo);
            }
        }
        if !per_seg.is_empty() {
            interior_buckets.insert(seg_idx, per_seg);
        }
    }
    if interior_buckets.is_empty() {
        return None;
    }

    // Partial-agg output: [group_col, count_state]; both must be Int64 for
    // our synthesized batches to be type-correct.
    let partial_schema = agg.schema();
    if partial_schema.fields().len() != 2
        || partial_schema.field(0).data_type() != &DataType::Int64
        || partial_schema.field(1).data_type() != &DataType::Int64
    {
        return None;
    }

    // Build per-partition interior workloads mirroring the shard's
    // assignments, and the boundary shard exec with interior RGs removed.
    let assignments = shard.assignments();
    let mut partitions: Vec<Vec<ChunkCounts>> = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let mut chunk_counts = Vec::new();
        for chunk in &assignment.chunks {
            let Some(per_seg) = interior_buckets.get(&chunk.segment_idx) else {
                continue;
            };
            let segment = &config.segments[chunk.segment_idx];
            // Coalesce consecutive interior RGs sharing a bucket into runs.
            let mut runs: Vec<BucketRun> = Vec::new();
            let mut interior_rg_indices: Vec<usize> = Vec::new();
            for &rg_idx in &chunk.row_group_indices {
                let Some(&bucket) = per_seg.get(&rg_idx) else {
                    continue;
                };
                let info = segment
                    .row_groups
                    .iter()
                    .find(|info| info.index == rg_idx)?;
                let doc_min = (info.first_row as i32).max(chunk.doc_min);
                let doc_max = ((info.first_row + info.num_rows) as i32).min(chunk.doc_max);
                if doc_min >= doc_max {
                    continue;
                }
                interior_rg_indices.push(rg_idx);
                match runs.last_mut() {
                    Some(last) if last.bucket == bucket && last.doc_max == doc_min => {
                        last.doc_max = doc_max;
                    }
                    _ => runs.push(BucketRun {
                        bucket,
                        doc_min,
                        doc_max,
                    }),
                }
            }
            if runs.is_empty() {
                continue;
            }
            chunk_counts.push(ChunkCounts {
                chunk: SegmentChunk {
                    segment_idx: chunk.segment_idx,
                    doc_min: chunk.doc_min,
                    doc_max: chunk.doc_max,
                    row_group_indices: interior_rg_indices,
                },
                runs,
            });
        }
        partitions.push(chunk_counts);
    }

    // Boundary side: original subtree, rebuilt with the shard replaced by a
    // clone with interior RGs stripped.
    let interior_sets: HashMap<usize, HashSet<usize>> = interior_buckets
        .iter()
        .map(|(seg, m)| (*seg, m.keys().copied().collect()))
        .collect();
    fn rebuild_with_filtered_shard(
        node: &Arc<dyn ExecutionPlan>,
        remove: &HashMap<usize, HashSet<usize>>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        if let Some(shard) = node.as_ref().downcast_ref::<QueryShardExec>() {
            return Some(Arc::new(shard.with_row_groups_removed(remove)));
        }
        let children: Option<Vec<Arc<dyn ExecutionPlan>>> = node
            .children()
            .iter()
            .map(|c| rebuild_with_filtered_shard(c, remove))
            .collect();
        Arc::clone(node).with_new_children(children?).ok()
    }
    let boundary_partial: Arc<dyn ExecutionPlan> = Arc::clone(node)
        .with_new_children(vec![rebuild_with_filtered_shard(
            agg_input,
            &interior_sets,
        )?])
        .ok()?;

    let counts_exec: Arc<dyn ExecutionPlan> = Arc::new(HistogramCountsExec::new(
        partial_schema,
        Arc::clone(config),
        partitions,
    ));

    native_bridge_common::log_debug!(
        "histogram fast path: {} interior segment(s); boundary subtree retained",
        interior_buckets.len()
    );
    Some(Arc::new(UnionExec::new(vec![
        boundary_partial,
        counts_exec,
    ])))
}

/// Emits partial-aggregate `(bucket, count)` rows for interior row groups.
pub struct HistogramCountsExec {
    schema: SchemaRef,
    config: Arc<super::table_provider::IndexedTableConfig>,
    partitions: Vec<Vec<ChunkCounts>>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl HistogramCountsExec {
    fn new(
        schema: SchemaRef,
        config: Arc<super::table_provider::IndexedTableConfig>,
        partitions: Vec<Vec<ChunkCounts>>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partitions.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            config,
            partitions,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for HistogramCountsExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistogramCountsExec")
            .field("partitions", &self.partitions.len())
            .finish()
    }
}

impl DisplayAs for HistogramCountsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let runs: usize = self
            .partitions
            .iter()
            .flat_map(|p| p.iter().map(|c| c.runs.len()))
            .sum();
        write!(
            f,
            "HistogramCountsExec: partitions={}, bucket_runs={}",
            self.partitions.len(),
            runs
        )
    }
}

impl ExecutionPlan for HistogramCountsExec {
    fn name(&self) -> &str {
        "HistogramCountsExec"
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let chunks = self.partitions.get(partition).cloned().unwrap_or_default();
        let config = Arc::clone(&self.config);
        let schema = Arc::clone(&self.schema);
        let runs_counted: Count =
            MetricBuilder::new(&self.metrics).counter("hist_runs_from_stats", partition);

        let out_schema = Arc::clone(&schema);
        let stream = futures::stream::once(async move {
            let result = tokio::task::spawn_blocking(move || -> Result<RecordBatch> {
                let mut buckets: Vec<i64> = Vec::new();
                let mut counts: Vec<i64> = Vec::new();
                for chunk_counts in &chunks {
                    let segment = &config.segments[chunk_counts.chunk.segment_idx];
                    let evaluator = (config.evaluator_factory)(
                        segment,
                        &chunk_counts.chunk,
                        &StreamMetrics::empty(),
                        None,
                    )
                    .map_err(|e| DataFusionError::External(e.into()))?;
                    for run in &chunk_counts.runs {
                        let count = match evaluator
                            .count_docs_range(run.doc_min, run.doc_max)
                            .map_err(|e| DataFusionError::External(e.into()))?
                        {
                            Some(count) => count,
                            None => {
                                // Fallback: bitset cardinality via prefetch —
                                // correct, just slower. Never silently wrong.
                                let rg = super::stream::RowGroupInfo {
                                    index: 0,
                                    first_row: run.doc_min as i64,
                                    num_rows: (run.doc_max - run.doc_min) as i64,
                                };
                                match evaluator
                                    .prefetch_rg(&rg, run.doc_min, run.doc_max)
                                    .map_err(|e| DataFusionError::External(e.into()))?
                                {
                                    Some(prefetched) => prefetched.candidates.len(),
                                    None => 0,
                                }
                            }
                        };
                        runs_counted.add(1);
                        if count > 0 {
                            buckets.push(run.bucket);
                            counts.push(count as i64);
                        }
                    }
                }
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(buckets)) as ArrayRef,
                        Arc::new(Int64Array::from(counts)) as ArrayRef,
                    ],
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("histogram count task: {e}")))?;
            result
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}
