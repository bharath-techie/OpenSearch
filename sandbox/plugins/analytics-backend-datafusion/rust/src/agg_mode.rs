/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Aggregate mode stripping for distributed partial/final execution.

use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::optimizer::{PhysicalOptimizer, PhysicalOptimizerRule};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::Result;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Mode {
    Default,
    Partial,
    Final,
}

/// Returns the default physical optimizer rules with `CombinePartialFinalAggregate` removed.
pub(crate) fn physical_optimizer_rules_without_combine(
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let combine_name = CombinePartialFinalAggregate::new().name().to_string();
    PhysicalOptimizer::new()
        .rules
        .into_iter()
        .filter(|r| r.name() != combine_name)
        .collect()
}

/// Applies aggregate mode stripping to a physical plan.
/// `has_topk`: when true and stripping to Partial, replaces Final/FinalPartitioned with
/// PartialReduce so CSS partitions are merged by group key before the TopK sort truncates.
pub(crate) fn apply_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    mode: Mode,
    has_topk: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    match mode {
        Mode::Default => Ok(plan),
        Mode::Partial => force_aggregate_mode(plan, AggregateMode::Partial, has_topk),
        Mode::Final => force_aggregate_mode(plan, AggregateMode::Final, false),
    }
}

/// Returns the output schema of the Partial aggregate without rebuilding the plan tree.
/// Used by `derive_schema_from_partial_plan` where we only need types, not an executable plan.
pub(crate) fn partial_aggregate_schema(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<arrow::datatypes::SchemaRef> {
    find_partial_input(Arc::clone(plan)).map(|p| p.schema())
}

/// The DataFusion version whose accumulator state layouts this build writes and folds.
/// Stamped into every view's spec; folding across versions is refused (state-schema
/// versioning in the MV architecture doc).
pub(crate) const DATAFUSION_VERSION: &str = datafusion::DATAFUSION_VERSION;

/// Manual escape hatch: bump when OUR conventions change incompatibly (state column
/// naming, sanitization, positional contract) independent of the DataFusion version.
pub(crate) const STATE_LAYOUT_VERSION: u32 = 1;

/// Converts a reduce plan's FINAL aggregate into a state-emitting `PartialReduce` and
/// renames its outputs to the materialized-view storage convention.
///
/// Used by MV refresh: instead of finalizing, the coordinator folds the shards' partial
/// states by group key and emits the folded states, which the materialize sink writes to
/// the view index. Output columns: group keys keep their names; aggregate call `i`'s
/// accumulator state fields become `{call_alias}__st_0..n-1` — the same convention the
/// parquet PartialReduce merge and the finalize-on-read path consume. Call aliases come
/// from the plan itself (Calcite pre-reduces avg/stddev/var to sum/count primitives, so
/// aliases may be generated names like `$f2`); the read path replans the same definition
/// with the same rules, so the names are deterministic. No aggregate is ever finalized
/// on disk, so view segments stay foldable for any DataFusion aggregate with zero
/// function-specific handling.
///
/// The FINAL aggregate is located through single-input wrappers; whatever sits above it
/// (finalize projections such as avg's `sum/count` DIVIDE, relabels) is dropped — those
/// compute or rename *final* values, which do not exist in state emission. The read
/// path reapplies them over the finalized states.
pub(crate) fn to_state_emitting_plan(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Strip the planner-inserted Partial half first — the fragment's input already
    // carries partial states (from shards at refresh time, from the view scan at read
    // time). Leaving the Partial in place would run `update_batch` over states as if
    // they were raw values: associative folds (sum/min/max) survive by luck, but
    // sketch states would be hashed as opaque blobs. Same strip `prepare_final_plan`
    // applies.
    let plan = force_aggregate_mode(plan, AggregateMode::Final, false)?;
    let agg_node = find_final_agg(&plan).ok_or_else(|| {
        datafusion_common::DataFusionError::Plan(
            "state-emitting reduce: no Final/FinalPartitioned aggregate in plan".to_string(),
        )
    })?;
    let agg = agg_node
        .downcast_ref::<AggregateExec>()
        .expect("find_final_agg returns AggregateExec nodes");
    let reduce = Arc::new(AggregateExec::try_new(
        AggregateMode::PartialReduce,
        agg.group_expr().clone(),
        agg.aggr_expr().to_vec(),
        agg.filter_expr().to_vec(),
        Arc::clone(agg.input()),
        agg.input_schema(),
    )?);

    // Rename projection: positional over the PartialReduce output (groups first, then
    // each aggregate's state fields in declaration order — the same layout the
    // aggregation itself consumes and produces).
    let schema = reduce.schema();
    let group_len = agg.group_expr().expr().len();
    let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(schema.fields().len());
    for idx in 0..group_len {
        let name = schema.field(idx).name();
        exprs.push((Arc::new(Column::new(name, idx)), name.clone()));
    }
    let mut idx = group_len;
    for call in agg.aggr_expr() {
        let state_count = call.state_fields()?.len();
        for state_idx in 0..state_count {
            let name = schema.field(idx).name();
            exprs.push((Arc::new(Column::new(name, idx)), state_column_name(call.name(), state_idx)));
            idx += 1;
        }
    }
    if idx != schema.fields().len() {
        return Err(datafusion_common::DataFusionError::Plan(format!(
            "state-emitting reduce: consumed {} of {} reduce output columns",
            idx,
            schema.fields().len()
        )));
    }
    Ok(Arc::new(ProjectionExec::try_new(exprs, reduce)?))
}

/// Name of the i-th state column for aggregate output `output` — the shared storage
/// convention between state emission, the parquet PartialReduce merge, and reads.
/// The output name is sanitized to mapping-safe form: physical call names such as
/// `sum(input-0.cnt)` carry dots (object-path separators in index mappings) and
/// parentheses, so every character outside `[A-Za-z0-9_]` becomes `_`.
pub(crate) fn state_column_name(output: &str, state_idx: usize) -> String {
    format!("{}__st_{}", sanitize_output_name(output), state_idx)
}

/// Mapping-safe form of an aggregate call name (also used as the spec's `output`).
pub(crate) fn sanitize_output_name(output: &str) -> String {
    output
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

/// Describes the state-emitting form of a reduce plan as JSON — the single source of
/// truth from which the view index is provisioned:
///
/// ```json
/// {
///   "engine": {"datafusion": "54.0.0", "layout": 1},
///   "key_columns": ["k", "p"],
///   "aggs": [{"output": "$f2", "fn": "sum", "input_types": ["Float64"]}],
///   "state_columns": [{"name": "k", "type": "Utf8"}, {"name": "$f2__st_0", "type": "Float64"}]
/// }
/// ```
///
/// `key_columns`/`aggs` is exactly the parquet merge spec (`index.parquet.mv.spec`);
/// `state_columns` is the sink schema for view-index mappings. `engine` pins the
/// accumulator state layout: state fields are an on-disk format, so folding a view
/// written under a different DataFusion version (or a bumped layout marker) must be
/// refused rather than risk mis-merging states. Derived from the same plan the refresh
/// executes, so it can never drift from what gets written.
pub(crate) fn describe_state_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<String> {
    let agg_node = find_final_agg(plan).ok_or_else(|| {
        datafusion_common::DataFusionError::Plan(
            "describe_state_plan: no Final/FinalPartitioned aggregate in plan".to_string(),
        )
    })?;
    let agg = agg_node
        .downcast_ref::<AggregateExec>()
        .expect("find_final_agg returns AggregateExec nodes");
    let state_plan = to_state_emitting_plan(Arc::clone(&agg_node))?;
    let state_schema = state_plan.schema();
    let input_schema = agg.input_schema();

    let mut json = format!(
        "{{\"engine\":{{\"datafusion\":{:?},\"layout\":{}}},\"key_columns\":[",
        DATAFUSION_VERSION, STATE_LAYOUT_VERSION
    );
    let group_len = agg.group_expr().expr().len();
    for idx in 0..group_len {
        if idx > 0 {
            json.push(',');
        }
        json.push_str(&format!("{:?}", state_schema.field(idx).name()));
    }
    json.push_str("],\"aggs\":[");
    for (i, call) in agg.aggr_expr().iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        let input_types: Vec<String> = call
            .expressions()
            .iter()
            .map(|e| e.data_type(&input_schema).map(|t| format!("{}", t)))
            .collect::<Result<_>>()?;
        json.push_str(&format!(
            "{{\"output\":{:?},\"fn\":{:?},\"input_types\":[{}]}}",
            sanitize_output_name(call.name()),
            call.fun().name(),
            input_types.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join(",")
        ));
    }
    json.push_str("],\"state_columns\":[");
    for (idx, field) in state_schema.fields().iter().enumerate() {
        if idx > 0 {
            json.push(',');
        }
        json.push_str(&format!(
            "{{\"name\":{:?},\"type\":\"{}\"}}",
            field.name(),
            field.data_type()
        ));
    }
    json.push_str("]}");
    Ok(json)
}

/// Walks through single-input wrappers to the first Final/FinalPartitioned aggregate.
fn find_final_agg(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        if matches!(*agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
            return Some(Arc::clone(plan));
        }
        return None;
    }
    let children = plan.children();
    if children.len() == 1 {
        return find_final_agg(children[0]);
    }
    None
}

/// Walks the plan tree and strips the half that doesn't match `target`.
fn force_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    target: AggregateMode,
    has_topk: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        // Treat `FinalPartitioned` as `Final`: DataFusion picks `FinalPartitioned` for
        // grouped aggregates that consume hash-partitioned input and `Final` for scalar /
        // un-partitioned ones. Both are the FINAL half of the Partial/Final pair we strip.
        let agg_is_target = *agg.mode() == target
            || (target == AggregateMode::Final && *agg.mode() == AggregateMode::FinalPartitioned);
        if agg_is_target {
            // Keep this node, recurse into children
            let new_children: Vec<Arc<dyn ExecutionPlan>> = agg
                .children()
                .into_iter()
                .map(|c| force_aggregate_mode(Arc::clone(c), target, has_topk))
                .collect::<Result<_>>()?;
            return plan.with_new_children(new_children);
        }
        // Mode mismatch — strip this node
        match target {
            AggregateMode::Partial => {
                // Current node is Final/FinalPartitioned.
                // When TopK is active and the input has multiple partitions (CSS), replace
                // with PartialReduce instead of stripping. PartialReduce keeps agg.input()
                // (RepartitionExec(Hash) → Partial(×N)) so CSS partitions are merged by
                // group key before TopK truncation. Skip when input_partitions=1 — PartialReduce
                // over a single partition is redundant and adds unnecessary overhead.
                if has_topk && agg.input().output_partitioning().partition_count() > 1 {
                    return Ok(Arc::new(AggregateExec::try_new(
                        AggregateMode::PartialReduce,
                        agg.group_expr().clone(),
                        agg.aggr_expr().to_vec(),
                        agg.filter_expr().to_vec(),
                        Arc::clone(agg.input()),
                        agg.input_schema(),
                    )?));
                }
                // Normal path: strip Final, return Partial subtree
                if let Some(partial_subtree) = find_partial_input(Arc::clone(agg.input())) {
                    return Ok(partial_subtree);
                }
                Ok(Arc::clone(agg.input()))
            }
            AggregateMode::Final => {
                // Current node is Partial; skip it, return its child
                // (the Final above will keep itself)
                let child = agg.children()[0];
                force_aggregate_mode(Arc::clone(child), target, false)
            }
            _ => Ok(plan),
        }
    } else if plan.children().len() == 1 {
        // Single-input wrapper — recurse transparently.
        let old_child = Arc::clone(plan.children()[0]);
        let new_child = force_aggregate_mode(old_child.clone(), target, has_topk)?;

        // DataFusion's ProjectionMapping::try_new asserts col.name() == input_schema.field(i).name();
        // with_new_children triggers it. Remap columns to the post-strip schema so it passes.
        if let Some(proj) = plan.downcast_ref::<ProjectionExec>() {
            if old_child.schema() != new_child.schema() {
                let new_schema = &new_child.schema();
                let remapped: Vec<(Arc<dyn PhysicalExpr>, String)> = proj
                    .expr()
                    .iter()
                    .map(|pe| (remap_column(pe.expr.clone(), new_schema), pe.alias.clone()))
                    .collect();
                return Ok(Arc::new(ProjectionExec::try_new(remapped, new_child)?));
            }
        }

        plan.with_new_children(vec![new_child])
    } else {
        // Leaf or multi-input node — return as-is
        Ok(plan)
    }
}

/// Walks down through any single-input wrapper (RelabelExec / RepartitionExec /
/// CoalescePartitionsExec / ProjectionExec / etc.) to find an
/// AggregateExec(Partial) and returns the entire Partial subtree (the
/// AggregateExec node itself, not just its input).
fn find_partial_input(plan: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        if *agg.mode() == AggregateMode::Partial {
            return Some(plan);
        }
        // Non-Partial aggregate (Final/FinalPartitioned) — look into its input for Partial
        return find_partial_input(Arc::clone(agg.input()));
    }
    let children = plan.children();
    if children.len() == 1 {
        return find_partial_input(Arc::clone(children[0]));
    }
    None
}

/// Updates Column expression names to match the given schema (by index). Recurses into children.
fn remap_column(
    expr: Arc<dyn PhysicalExpr>,
    schema: &arrow::datatypes::SchemaRef,
) -> Arc<dyn PhysicalExpr> {
    if let Some(col) = expr.downcast_ref::<Column>() {
        return Arc::new(Column::new(schema.field(col.index()).name(), col.index()));
    }
    let children = expr.children();
    if children.is_empty() {
        return expr;
    }
    let new_children: Vec<_> = children
        .into_iter()
        .map(|c| remap_column(c.clone(), schema))
        .collect();
    let fallback = expr.clone();
    expr.with_new_children(new_children).unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::*;

    /// Helper: create a SessionContext with CombinePartialFinalAggregate disabled,
    /// register a memtable, and produce a physical plan for `SELECT SUM(x) FROM t`.
    async fn make_agg_plan() -> Arc<dyn ExecutionPlan> {
        let ctx = SessionContext::new_with_state(
            datafusion::execution::SessionStateBuilder::new()
                .with_config(SessionConfig::new())
                .with_default_features()
                .with_physical_optimizer_rules(physical_optimizer_rules_without_combine())
                .build(),
        );
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.sql("SELECT SUM(x) FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    /// Helper: create a plan with Repartition between Final and Partial.
    async fn make_agg_plan_with_repartition() -> Arc<dyn ExecutionPlan> {
        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = 4;
        let ctx = SessionContext::new_with_state(
            datafusion::execution::SessionStateBuilder::new()
                .with_config(config)
                .with_default_features()
                .with_physical_optimizer_rules(physical_optimizer_rules_without_combine())
                .build(),
        );
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        // GROUP BY forces repartition with multiple target partitions
        let df = ctx.sql("SELECT x, SUM(x) FROM t GROUP BY x").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    fn plan_string(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    fn contains_node(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
        if plan.name().contains(name) {
            return true;
        }
        plan.children().iter().any(|c| contains_node(c, name))
    }

    fn find_agg_modes(plan: &Arc<dyn ExecutionPlan>) -> Vec<AggregateMode> {
        let mut modes = Vec::new();
        if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
            modes.push(*agg.mode());
        }
        for child in plan.children() {
            modes.extend(find_agg_modes(child));
        }
        modes
    }

    #[tokio::test]
    async fn test_strip_partial_over_scan() {
        // Final(Partial(memtable)) → strip to Partial only
        let plan = make_agg_plan().await;
        let modes = find_agg_modes(&plan);
        assert!(
            modes.contains(&AggregateMode::Final) || modes.contains(&AggregateMode::Partial),
            "Plan should have aggregate nodes: {}",
            plan_string(&plan)
        );

        let result = apply_aggregate_mode(plan, Mode::Partial, false).unwrap();
        let result_modes = find_agg_modes(&result);
        assert!(
            result_modes.contains(&AggregateMode::Partial),
            "Should contain Partial: {}",
            plan_string(&result)
        );
        assert!(
            !result_modes.contains(&AggregateMode::Final),
            "Should NOT contain Final: {}",
            plan_string(&result)
        );
    }

    #[tokio::test]
    async fn test_strip_final_over_scan() {
        // Final(Partial(memtable)) → strip to Final only (Partial removed)
        let plan = make_agg_plan().await;
        let result = apply_aggregate_mode(plan, Mode::Final, false).unwrap();
        let result_modes = find_agg_modes(&result);
        assert!(
            result_modes.contains(&AggregateMode::Final),
            "Should contain Final: {}",
            plan_string(&result)
        );
        assert!(
            !result_modes.contains(&AggregateMode::Partial),
            "Should NOT contain Partial: {}",
            plan_string(&result)
        );
    }

    #[tokio::test]
    async fn test_strip_partial_past_repartition() {
        // Final → Repartition/Coalesce → Partial → scan; strip to Partial
        let plan = make_agg_plan_with_repartition().await;
        let plan_str = plan_string(&plan);
        // Verify the plan has the expected structure
        let modes = find_agg_modes(&plan);
        if modes.len() < 2 {
            // If optimizer collapsed it, just verify Mode::Partial works
            let result = apply_aggregate_mode(plan, Mode::Partial, false).unwrap();
            let result_modes = find_agg_modes(&result);
            assert!(!result_modes.contains(&AggregateMode::Final));
            return;
        }

        let result = apply_aggregate_mode(plan, Mode::Partial, false).unwrap();
        let result_modes = find_agg_modes(&result);
        assert!(
            !result_modes.contains(&AggregateMode::Final),
            "Should NOT contain Final after strip: {}\nOriginal: {}",
            plan_string(&result),
            plan_str
        );
    }

    #[tokio::test]
    async fn test_strip_final_past_coalesce() {
        // Final → CoalescePartitions → Partial → scan; strip to Final
        let plan = make_agg_plan().await;
        // The simple plan has CoalescePartitions between Final and Partial
        let result = apply_aggregate_mode(plan, Mode::Final, false).unwrap();
        let result_modes = find_agg_modes(&result);
        assert!(
            !result_modes.contains(&AggregateMode::Partial),
            "Should NOT contain Partial after strip: {}",
            plan_string(&result)
        );
        assert!(
            result_modes.contains(&AggregateMode::Final),
            "Should contain Final: {}",
            plan_string(&result)
        );
    }

    /// A state-emitting reduce must fold partial states by group key and emit
    /// accumulator state columns under the `{output}__st_i` convention — never
    /// finalized values. Mirrors production: the reduce plan's input carries partial
    /// STATES (from shards at refresh, from a view scan at read); any planner-inserted
    /// Partial half is stripped so the aggregation merges rather than re-accumulates.
    /// avg proves the general case: two state fields per call.
    #[tokio::test]
    async fn test_to_state_emitting_plan_emits_folded_states() {
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::FunctionRegistry;
        use datafusion::physical_expr::aggregate::AggregateExprBuilder;
        use datafusion::physical_expr::expressions::col;
        use datafusion::physical_plan::aggregates::{AggregateExec, PhysicalGroupBy};

        let raw_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Float64, false),
        ]));
        let batch = |ks: Vec<&str>, xs: Vec<f64>| {
            arrow_array::RecordBatch::try_new(
                Arc::clone(&raw_schema),
                vec![
                    Arc::new(arrow_array::StringArray::from(ks)),
                    Arc::new(arrow_array::Float64Array::from(xs)),
                ],
            )
            .unwrap()
        };
        let ctx = SessionContext::new();
        let avg = ctx.state().udaf("avg").unwrap();
        let sum = ctx.state().udaf("sum").unwrap();
        let aggs: Vec<Arc<datafusion::physical_expr::aggregate::AggregateFunctionExpr>> = vec![
            Arc::new(
                AggregateExprBuilder::new(avg, vec![col("x", &raw_schema).unwrap()])
                    .schema(Arc::clone(&raw_schema))
                    .alias("a")
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                AggregateExprBuilder::new(sum, vec![col("x", &raw_schema).unwrap()])
                    .schema(Arc::clone(&raw_schema))
                    .alias("s")
                    .build()
                    .unwrap(),
            ),
        ];
        let groups = PhysicalGroupBy::new_single(vec![(col("k", &raw_schema).unwrap(), "k".to_string())]);

        // Two independent Partial runs → two state partitions (the "shards" / "view segments").
        let mut state_partitions: Vec<Vec<arrow_array::RecordBatch>> = Vec::new();
        for b in [batch(vec!["a", "a", "b"], vec![10.0, 20.0, 5.0]), batch(vec!["a"], vec![30.0])] {
            let input = MemorySourceConfig::try_new_exec(&[vec![b]], Arc::clone(&raw_schema), None).unwrap();
            let partial = Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Partial,
                    groups.clone(),
                    aggs.clone(),
                    vec![None; 2],
                    input,
                    Arc::clone(&raw_schema),
                )
                .unwrap(),
            );
            let states = datafusion::physical_plan::collect(partial, SessionContext::new().task_ctx())
                .await
                .unwrap();
            state_partitions.push(states);
        }
        let states2 = state_partitions.pop().unwrap();
        let states1 = state_partitions.pop().unwrap();
        let state_schema = states1[0].schema();

        // The reduce plan as production produces it: FINAL over the state-typed input.
        let input =
            MemorySourceConfig::try_new_exec(&[states1, states2], Arc::clone(&state_schema), None).unwrap();
        let coalesced = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input),
        );
        let final_plan = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                groups.clone(),
                aggs.clone(),
                vec![None; 2],
                coalesced,
                Arc::clone(&state_schema),
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let describe = describe_state_plan(&final_plan).unwrap();
        let state_plan = to_state_emitting_plan(final_plan).unwrap();

        // Schema: group key + avg's two state fields + sum's one, named {alias}__st_i.
        let schema = state_plan.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["k", "a__st_0", "a__st_1", "s__st_0"]);
        assert!(describe.contains("\"fn\":\"avg\""), "describe: {describe}");
        assert!(describe.contains("\"key_columns\":[\"k\"]"), "describe: {describe}");
        assert!(
            describe.contains(&format!("\"datafusion\":\"{}\"", DATAFUSION_VERSION)),
            "engine version stamped: {describe}"
        );

        // Executes and folds ACROSS the state partitions: group "a" must be
        // count=3, sum=60 — merged states, never finalized, never re-accumulated.
        let batches = datafusion::physical_plan::collect(state_plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        let all = arrow::compute::concat_batches(&schema, &batches).unwrap();
        assert_eq!(all.num_rows(), 2, "two groups");
        let k = all
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let count = all
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        let sum_col = all
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        let row_a = (0..all.num_rows()).find(|&i| k.value(i) == "a").unwrap();
        assert_eq!(count.value(row_a), 3, "avg state count folded across partitions");
        assert_eq!(sum_col.value(row_a), 60.0, "avg state sum folded, not finalized");
    }

    /// A plan with no FINAL aggregate (plain scan) must be rejected loudly.
    #[tokio::test]
    async fn test_to_state_emitting_plan_rejects_non_aggregate() {
        let ctx = SessionContext::new_with_state(
            datafusion::execution::SessionStateBuilder::new()
                .with_config(SessionConfig::new())
                .with_default_features()
                .with_physical_optimizer_rules(physical_optimizer_rules_without_combine())
                .build(),
        );
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.sql("SELECT x FROM t").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let err = to_state_emitting_plan(plan).err().expect("non-aggregate must fail");
        assert!(err.to_string().contains("no Final"), "got: {err}");
    }

    #[test]
    fn test_combine_rule_absent() {
        let rules = physical_optimizer_rules_without_combine();
        let combine_name = CombinePartialFinalAggregate::new().name().to_string();
        assert!(
            !rules.iter().any(|r| r.name() == combine_name),
            "CombinePartialFinalAggregate should be filtered out"
        );
        assert!(!rules.is_empty(), "Should have other optimizer rules");
    }

    /// Verifies apply_aggregate_mode(Partial) strips the Final aggregate and keeps
    /// only the Partial subtree — the core behavior the indexed executor relies on
    /// for engine-native-merge (dc/HLL) queries.
    #[tokio::test]
    async fn test_apply_partial_strips_final() {
        let plan = make_agg_plan().await;
        let display_before = plan_string(&plan);
        assert!(
            display_before.contains("AggregateExec: mode=Final"),
            "expected Final in plan"
        );
        assert!(
            display_before.contains("AggregateExec: mode=Partial"),
            "expected Partial in plan"
        );

        let stripped = apply_aggregate_mode(plan, Mode::Partial, false).unwrap();
        let display_after = plan_string(&stripped);
        assert!(
            !display_after.contains("mode=Final"),
            "Final should be stripped"
        );
        assert!(
            display_after.contains("mode=Partial"),
            "Partial should remain"
        );
    }

    /// When has_topk=true and the input has multiple partitions (CSS), Final/FinalPartitioned
    /// must be replaced with PartialReduce rather than stripped, so the coordinator receives
    /// correctly merged partial state instead of per-partition-truncated results.
    #[tokio::test]
    async fn test_apply_partial_with_topk_produces_partial_reduce() {
        let plan = make_agg_plan_with_repartition().await;
        let display_before = plan_string(&plan);
        // With target_partitions=4 and GROUP BY, DF produces FinalPartitioned.
        assert!(
            display_before.contains("mode=FinalPartitioned")
                || display_before.contains("mode=Final"),
            "expected Final/FinalPartitioned in multi-partition plan, got:\n{display_before}"
        );

        let result = apply_aggregate_mode(plan, Mode::Partial, true).unwrap();
        let modes = find_agg_modes(&result);
        assert!(
            modes.contains(&AggregateMode::PartialReduce),
            "has_topk=true with multi-partition input must produce PartialReduce, got modes: {modes:?}"
        );
        assert!(
            !modes.contains(&AggregateMode::Final)
                && !modes.contains(&AggregateMode::FinalPartitioned),
            "Final/FinalPartitioned must not remain after stripping"
        );
    }
}
