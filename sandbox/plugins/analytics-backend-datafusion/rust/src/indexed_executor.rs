/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Indexed query executor — decodes substrait, classifies the filter tree,
//! builds providers per leaf, runs the query.
//!
//! Per-leaf lifecycle at query time (one compiled-query + per-segment matcher
//! per Collector leaf):
//!   1. `createProvider(annotation_id)` FFM upcall → `provider_key`  (once per
//!      Collector leaf, once per query).
//!   2. `createCollector(provider_key, seg, min, max)` FFM upcall → collector
//!      (once per SegmentChunk × Collector leaf).
//!   3. `collectDocs(collector, min, max, out)` FFM upcall (once per row group).
//!   4. `releaseCollector(collector)` when RG scan completes.
//!   5. `releaseProvider(provider_key)` when the tree is dropped.

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{DataType, SchemaRef, TimeUnit},
    catalog::Session,
    common::tree_node::{TreeNode, TreeNodeRecursion},
    common::{DataFusionError, ScalarValue},
    datasource::{TableProvider, TableType},
    execution::memory_pool::MemoryPool,
    execution::object_store::ObjectStoreUrl,
    logical_expr::{Expr, LogicalPlan, Operator},
    parquet::arrow::arrow_reader::statistics::StatisticsConverter,
    physical_expr::expressions::Column,
    physical_expr::PhysicalExpr,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::displayable,
    physical_plan::execute_stream,
    physical_plan::stream::RecordBatchStreamAdapter,
    physical_plan::ExecutionPlan,
};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use native_bridge_common::log_debug;
use prost::Message;
use substrait::proto::Plan;

use crate::api::DataFusionRuntime;
use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::helper::{
    build_query_runtime_env_with_store, build_query_session_context, register_listing_table,
};
use crate::indexed_table::bool_tree::BoolNode;
use crate::indexed_table::eval::bitmap_tree::{BitmapTreeEvaluator, CollectorLeafBitmaps};
use crate::indexed_table::eval::single_collector::SingleCollectorEvaluator;
use crate::indexed_table::eval::{CollectorCallStrategy, RowGroupBitsetSource, TreeBitsetSource};
use crate::indexed_table::ffm_callbacks::{create_provider, FfmSegmentCollector, ProviderHandle};
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::PagePruner;
use crate::indexed_table::segment_info::build_segments;
use crate::indexed_table::substrait_to_tree::{
    classify_filter, expr_to_bool_tree, extract_filter_expr, ExtractionResult, FilterClass,
};
use crate::indexed_table::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
    WithinClassifierReasons,
};

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::api::ShardView;
use crate::cache::page_index;
use crate::datafusion_query_config::DatafusionQueryConfig;
use crate::indexed_table::bool_tree::residual_bool_to_physical_expr;
use crate::indexed_table::metrics::StreamMetrics;
use crate::indexed_table::page_pruner::{
    build_pruning_predicate, PagePruneMetrics, StatsPruneTree,
};
use crate::parquet_page_cache::{
    load_scoped_page_index_cols, resolve_predicate_parquet_columns_pair,
};

/// Execute an indexed query.
///
/// `shard_view` carries the segment's parquet paths (populated when the reader
/// was built from a catalog snapshot). `query_memory_pool` is the per-query
/// tracker (same as vanilla path) — `None` disables tracking and uses the
/// global pool.
// TODO: remove this function once all callers migrate to the instruction-based path
// TODO: remove once api.rs migrates to instruction-based path directly.
// Kept as thin wrapper to make existing tests exercise execute_indexed_with_context
// with minimal changes.
pub async fn execute_indexed_query(
    substrait_bytes: Vec<u8>,
    table_name: String,
    shard_view: &ShardView,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
    query_memory_pool: Option<Arc<dyn MemoryPool>>,
    query_config: Arc<DatafusionQueryConfig>,
    context_id: i64,
) -> Result<i64, DataFusionError> {
    let num_partitions = query_config.target_partitions.max(1);
    // Build the per-query RuntimeEnv (list-files cache pre-populated, optional
    // per-query pool overlay) and register the shard object store — shared with
    // the vanilla path. File-metadata and file-statistics caches are inherited
    // from the global runtime for cross-query reuse.
    let runtime_env = build_query_runtime_env_with_store(
        runtime,
        &shard_view.table_path,
        shard_view.object_metas.as_ref(),
        Arc::clone(&shard_view.store),
        query_memory_pool,
    )?;

    // Build a fresh session context per query. The indexed path fans out via
    // IndexedExec partitions (derived from num_partitions), not DataFusion's, but
    // DF still wants a sane value for any post-scan operators it may add. The
    // `indexed_path` flag also drops the combine-partial-final optimizer pass and
    // registers the indexed-only index_filter / delegation_possible UDFs.
    let ctx = build_query_session_context(&query_config, runtime_env, num_partitions, true);

    // Register default ListingTable so substrait consumer can resolve the table.
    // No sort-order declaration on the indexed path (empty slices) — the indexed
    // executor drives ordering itself via IndexedExec.
    register_listing_table(&ctx, &table_name, shard_view.table_path.clone(), &[], &[]).await?;

    // Build SessionContextHandle and delegate to execute_indexed_with_context
    let handle = crate::session_context::SessionContextHandle {
        ctx,
        table_path: shard_view.table_path.clone(),
        object_metas: shard_view.object_metas.clone(),
        writer_generations: shard_view.writer_generations.clone(),
        sort_fields: shard_view.sort_fields.clone(),
        sort_orders: shard_view.sort_orders.clone(),
        query_context: crate::query_tracker::QueryTrackingContext::new(
            context_id,
            runtime.runtime_env.memory_pool.clone(),
            crate::query_tracker::QueryType::Shard,
        ),
        table_name: table_name.clone(),
        indexed_config: None, // derive classification from tree
        query_config: Arc::unwrap_or_clone(query_config),
        io_handle: tokio::runtime::Handle::current(),
        aggregate_mode: crate::agg_mode::Mode::Default,
        has_topk: false,
        prepared_plan: None,
        phantom_reservation: None,
    };
    let ptr = Box::into_raw(Box::new(handle)) as i64;

    // NOTE: gate acquired on CPU here — acceptable for this deprecated benchmark-only path.
    // Production uses df_execute_with_context which acquires the gate on IO for backpressure.
    let partition_weight = num_partitions.max(1) as u32;
    let gate = cpu_executor.concurrency_gate().clone();
    let max_p = gate.max_permits();
    let permit = gate.acquire_many(partition_weight.min(max_p)).await;

    unsafe { execute_indexed_with_context(ptr, substrait_bytes, cpu_executor, permit).await }
}

// ── Helpers ───────────────────────────────────────────────────────────

/// Inclusive i64 range extracted from top-level conjunctive comparisons on the leading
/// `index.sort.field`. This is intentionally narrow: OR/NOT, casts, non-i64 literals, and
/// non-comparison predicates are left untouched (fail closed to the normal decode path).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SortRange {
    column: String,
    lower: Option<i64>,
    upper: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeBound {
    Lower(i64),
    Upper(i64),
}

pub(crate) fn scalar_as_i64(value: &ScalarValue) -> Option<i64> {
    scalar_ticks_with_unit(value).map(|(ticks, _unit)| ticks)
}

/// Like [`scalar_as_i64`] but also returns the scalar's temporal unit (`None`
/// for a plain integer). Used to normalize a derived sort-range bound into the
/// sort column's footer unit — see [`comparison_bound`].
fn scalar_ticks_with_unit(value: &ScalarValue) -> Option<(i64, Option<TimeUnit>)> {
    match value {
        ScalarValue::Int64(Some(v)) => Some((*v, None)),
        ScalarValue::TimestampSecond(Some(v), _) => Some((*v, Some(TimeUnit::Second))),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some((*v, Some(TimeUnit::Millisecond))),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some((*v, Some(TimeUnit::Microsecond))),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some((*v, Some(TimeUnit::Nanosecond))),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok().map(|v| (v, None)),
        _ => None,
    }
}

fn time_unit_ticks_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// Convert a bound tick count from `src` unit to `tgt` unit.
///
/// Widening to a finer unit (e.g. ms→ns) is exact (multiply). Coarsening to a
/// wider unit (e.g. ns→ms) loses sub-tick precision, so it rounds in the
/// direction that keeps the derived range CONSERVATIVE (fail-closed WITHIN):
/// a lower bound rounds toward +∞ (ceil), an upper bound toward −∞ (floor).
/// These are the exact integer equivalences for `col * factor <op> literal`
/// (the sort column is stored in `tgt`, the folded literal arrives in `src`),
/// so no genuinely-WITHIN row group is ever excluded and none is wrongly
/// included. Returns `None` on i64 overflow.
fn convert_bound_ticks(value: i64, src: TimeUnit, tgt: TimeUnit, is_lower: bool) -> Option<i64> {
    let s = time_unit_ticks_per_second(src);
    let t = time_unit_ticks_per_second(tgt);
    if t == s {
        Some(value)
    } else if t > s {
        value.checked_mul(t / s)
    } else {
        let d = s / t;
        let q = value.div_euclid(d);
        let r = value.rem_euclid(d);
        Some(if is_lower && r != 0 { q + 1 } else { q })
    }
}

fn convert_range_bound(bound: RangeBound, src: TimeUnit, tgt: TimeUnit) -> Option<RangeBound> {
    match bound {
        RangeBound::Lower(v) => convert_bound_ticks(v, src, tgt, true).map(RangeBound::Lower),
        RangeBound::Upper(v) => convert_bound_ticks(v, src, tgt, false).map(RangeBound::Upper),
    }
}

/// Extract a range bound `column <op> literal` (either operand order) from a
/// comparison, normalized into `target_unit` (the sort column's arrow
/// `TimeUnit`, `None` when the column is a plain integer).
///
/// Unit normalization is load-bearing: PPL lowers `timestamp('...')` bounds to a
/// `Timestamp(Nanosecond)` literal (DataFusion's `to_timestamp` default), but an
/// OpenSearch `date` column's parquet footer statistics are `Timestamp(ms)`
/// (`CalciteToArrowSchema`). Without converting the ns bound (~1.77e18) into ms
/// (~1.77e12) it never contains the ms footer, so `segment_within_rgs` returns
/// an empty WITHIN set and the count-shortcut / relaxed-strip / top-K-truncation
/// fast paths silently never activate (the single-shard Q4 gap).
fn comparison_bound(
    expr: &Expr,
    column: &str,
    target_unit: Option<TimeUnit>,
) -> Option<RangeBound> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };

    let normalize = |op: Operator, value: i64, column_on_left: bool| -> Option<RangeBound> {
        let op = if column_on_left {
            op
        } else {
            match op {
                Operator::Gt => Operator::Lt,
                Operator::GtEq => Operator::LtEq,
                Operator::Lt => Operator::Gt,
                Operator::LtEq => Operator::GtEq,
                _ => return None,
            }
        };
        match op {
            Operator::Gt => value.checked_add(1).map(RangeBound::Lower),
            Operator::GtEq => Some(RangeBound::Lower(value)),
            Operator::Lt => value.checked_sub(1).map(RangeBound::Upper),
            Operator::LtEq => Some(RangeBound::Upper(value)),
            _ => None,
        }
    };

    let (value, src_unit, column_on_left) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v, _)) if c.name == column => {
            let (ticks, unit) = scalar_ticks_with_unit(v)?;
            (ticks, unit, true)
        }
        (Expr::Literal(v, _), Expr::Column(c)) if c.name == column => {
            let (ticks, unit) = scalar_ticks_with_unit(v)?;
            (ticks, unit, false)
        }
        _ => return None,
    };

    // Apply strictness (±1) in the literal's native unit, then normalize the
    // resulting bound into the sort column's footer unit.
    let bound = normalize(binary.op, value, column_on_left)?;
    match (src_unit, target_unit) {
        (Some(src), Some(tgt)) => convert_range_bound(bound, src, tgt),
        // Plain-integer column or literal: no temporal unit to reconcile —
        // preserve the raw i64 (unchanged behavior).
        _ => Some(bound),
    }
}

#[cfg(test)]
fn extract_sort_range(plan: &LogicalPlan, column: &str) -> Option<SortRange> {
    let mut lower: Option<i64> = None;
    let mut upper: Option<i64> = None;
    let mut found = false;
    let result = plan.apply(|node| {
        if let LogicalPlan::Filter(filter) = node {
            for conjunct in datafusion_expr::utils::split_conjunction(&filter.predicate) {
                match comparison_bound(conjunct, column, None) {
                    Some(RangeBound::Lower(value)) => {
                        lower = Some(lower.map_or(value, |current| current.max(value)));
                        found = true;
                    }
                    Some(RangeBound::Upper(value)) => {
                        upper = Some(upper.map_or(value, |current| current.min(value)));
                        found = true;
                    }
                    None => {}
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if result.is_err() || !found || lower.zip(upper).is_some_and(|(lo, hi)| lo > hi) {
        return None;
    }
    Some(SortRange {
        column: column.to_string(),
        lower,
        upper,
    })
}

/// Const-fold constant operands of range/equality comparisons to literals.
///
/// PPL lowers timestamp bounds as `timestamp('...')` scalar-function calls
/// (e.g. `logdate >= timestamp('2026-02-08 00:00:00.000')`). The executor
/// consumes the RAW, unoptimized `from_substrait_plan` output, so these bounds
/// are never const-folded — they stay as `ScalarFunction`/`Cast` expressions.
/// Both `comparison_bound` (logical) and `physical_expr_is_sort_range_only`
/// (physical) require a *literal* bound, so an unfolded function bound silently
/// disables the WITHIN / count-shortcut / top-K-truncation fast paths.
///
/// This walks the predicate and, for each comparison BinaryExpr, folds any
/// operand that is fully constant (no column references) and not already a
/// literal into a literal via `ExprSimplifier`. It never touches an operand
/// that references a column, and only ever rewrites comparison operands — so
/// Lucene marker UDFs (`delegated_predicate`, `delegation_possible`), which are
/// boolean conjuncts rather than comparison operands, are left untouched (and
/// are never invoked/evaluated). Column-side casts are deliberately preserved:
/// `comparison_bound` still requires a *bare* sort column, so a cast column
/// operand fails closed (keeping the extracted i64 bound in the sort column's
/// own arrow unit, matching `segment_within_rgs`' footer statistics).
fn const_fold_comparison_bounds(expr: Expr, schema: &datafusion::common::DFSchemaRef) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::simplify::SimplifyContext;
    use datafusion::logical_expr::BinaryExpr as LogicalBinaryExpr;
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;

    let simplifier = ExprSimplifier::new(
        SimplifyContext::builder()
            .with_schema(schema.clone())
            .build(),
    );

    // Fold a fully-constant, non-literal operand to a literal. Returns `None`
    // if the operand references a column or does not fold to a literal.
    let fold_operand = |operand: &Expr| -> Option<Expr> {
        if matches!(operand, Expr::Literal(..)) {
            return None;
        }
        if !operand.column_refs().is_empty() {
            return None;
        }
        match simplifier.simplify(operand.clone()) {
            Ok(folded @ Expr::Literal(..)) => Some(folded),
            _ => None,
        }
    };

    let original = expr.clone();
    let rewritten = expr.transform(|node| {
        if let Expr::BinaryExpr(be) = &node {
            if matches!(
                be.op,
                Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq | Operator::Eq
            ) {
                let new_left = fold_operand(&be.left);
                let new_right = fold_operand(&be.right);
                if new_left.is_some() || new_right.is_some() {
                    let op = be.op;
                    let left = new_left.unwrap_or_else(|| (*be.left).clone());
                    let right = new_right.unwrap_or_else(|| (*be.right).clone());
                    return Ok(Transformed::yes(Expr::BinaryExpr(LogicalBinaryExpr::new(
                        Box::new(left),
                        op,
                        Box::new(right),
                    ))));
                }
            }
        }
        Ok(Transformed::no(node))
    });
    match rewritten {
        Ok(t) => t.data,
        Err(_) => original,
    }
}

/// Extract a conjunctive integer/timestamp range on `column` from the top-level
/// AND conjuncts of a single (already const-folded) predicate expression.
///
/// Mirrors `extract_sort_range` but operates on a predicate `Expr` rather than
/// walking a `LogicalPlan`, so it can consume the const-folded filter predicate.
/// `target_unit` is the sort column's arrow `TimeUnit` (`None` for a plain
/// integer column); extracted bounds are normalized into it — see
/// [`comparison_bound`].
fn sort_range_from_predicate(
    predicate: &Expr,
    column: &str,
    target_unit: Option<TimeUnit>,
) -> Option<SortRange> {
    let mut lower: Option<i64> = None;
    let mut upper: Option<i64> = None;
    let mut found = false;
    for conjunct in datafusion_expr::utils::split_conjunction(predicate) {
        match comparison_bound(conjunct, column, target_unit) {
            Some(RangeBound::Lower(value)) => {
                lower = Some(lower.map_or(value, |current| current.max(value)));
                found = true;
            }
            Some(RangeBound::Upper(value)) => {
                upper = Some(upper.map_or(value, |current| current.min(value)));
                found = true;
            }
            None => {}
        }
    }
    if !found || lower.zip(upper).is_some_and(|(lo, hi)| lo > hi) {
        return None;
    }
    Some(SortRange {
        column: column.to_string(),
        lower,
        upper,
    })
}

/// Row-group indices in `segment` whose sort-column FOOTER statistics prove the
/// row group lies fully WITHIN `range` with zero nulls. Uses only footer
/// `ColumnChunkMetaData` statistics (always fetched with the metadata — no page
/// index IO). Fail-closed per row group: missing stats, unsupported types, or
/// any nulls exclude that row group from the set.
fn segment_within_rgs(range: &SortRange, segment: &SegmentFileInfo) -> HashSet<usize> {
    let mut reasons = WithinClassifierReasons::default();
    segment_within_rgs_with_reasons(range, segment, &mut reasons)
}

/// Like [`segment_within_rgs`] but records, into `reasons`, a semantics-neutral
/// tally of every fail-closed segment exit and per-row-group rejection so a
/// `profile:true` run can explain WHY the WITHIN set is empty. The returned
/// set is byte-for-byte identical to [`segment_within_rgs`] — the counters are
/// pure diagnostics and never change the acceptance decision.
fn segment_within_rgs_with_reasons(
    range: &SortRange,
    segment: &SegmentFileInfo,
    reasons: &mut WithinClassifierReasons,
) -> HashSet<usize> {
    let mut within = HashSet::new();
    let converter = match StatisticsConverter::try_new(
        &range.column,
        &segment.arrow_schema,
        segment.metadata.file_metadata().schema_descr(),
    ) {
        Ok(converter) => converter,
        Err(_) => {
            reasons.converter_creation_failure += 1;
            return within;
        }
    };
    if converter.parquet_column_index().is_none() {
        reasons.parquet_column_index_missing += 1;
        return within;
    }
    let row_groups = segment.metadata.row_groups();
    let mins = match converter.row_group_mins(row_groups.iter()) {
        Ok(values) => values,
        Err(_) => {
            reasons.row_group_mins_error += 1;
            return within;
        }
    };
    let maxes = match converter.row_group_maxes(row_groups.iter()) {
        Ok(values) => values,
        Err(_) => {
            reasons.row_group_maxes_error += 1;
            return within;
        }
    };
    let null_counts = match converter.row_group_null_counts(row_groups.iter()) {
        Ok(values) => values,
        Err(_) => {
            reasons.row_group_null_counts_error += 1;
            return within;
        }
    };
    if mins.len() != row_groups.len()
        || maxes.len() != row_groups.len()
        || null_counts.len() != row_groups.len()
    {
        reasons.vector_length_mismatch += 1;
        return within;
    }
    for rg in 0..row_groups.len() {
        let minimum = ScalarValue::try_from_array(&*mins, rg)
            .ok()
            .as_ref()
            .and_then(scalar_as_i64);
        let maximum = ScalarValue::try_from_array(&*maxes, rg)
            .ok()
            .as_ref()
            .and_then(scalar_as_i64);
        let null_is_null = datafusion::arrow::array::Array::is_null(&null_counts, rg);
        let null_free = !null_is_null && null_counts.value(rg) == 0;

        // Reason tallies (independent; a single RG may hit several). These do
        // not gate acceptance — the `if let` below reproduces the original
        // accept condition exactly.
        if minimum.is_none() {
            reasons.min_scalar_unsupported += 1;
        }
        if maximum.is_none() {
            reasons.max_scalar_unsupported += 1;
        }
        if null_is_null {
            reasons.null_count_unavailable += 1;
        } else if null_counts.value(rg) != 0 {
            reasons.null_count_nonzero += 1;
        }
        if let (Some(minimum), Some(maximum)) = (minimum, maximum) {
            let lower_ok = range.lower.is_none_or(|lower| lower <= minimum);
            let upper_ok = range.upper.is_none_or(|upper| maximum <= upper);
            if !lower_ok {
                reasons.lower_bound_rejection += 1;
            }
            if !upper_ok {
                reasons.upper_bound_rejection += 1;
            }
            if null_free && lower_ok && upper_ok {
                within.insert(rg);
                reasons.within_accepted += 1;
            }
        }
    }
    within
}

/// Count-fast-path tree shape check for `FilterClass::SingleCollector`.
///
/// The top-level conjunction may contain only Lucene-evaluated leaves
/// (`Collector`, `DelegationPossible`) and range comparisons on the sort
/// column. Combined with a WITHIN row group, the sort-range conjuncts are
/// tautologies and the count is exactly the Lucene leaf's cardinality (or
/// the row count when there is no Lucene leaf).
///
/// The evaluator can count through at most ONE Lucene leaf (no bitset
/// intersection on the count path), so the shape is restricted to:
/// - one `Collector` and zero `DelegationPossible`, or
/// - zero `Collector` and at most one `DelegationPossible`.
/// OR/NOT anywhere fails closed.
fn count_tree_shape_supported(tree: &BoolNode, column: &str) -> bool {
    fn conjuncts_ok(node: &BoolNode, column: &str) -> bool {
        match node {
            BoolNode::And(children) => children.iter().all(|c| conjuncts_ok(c, column)),
            BoolNode::Collector { .. } | BoolNode::DelegationPossible { .. } => true,
            BoolNode::Predicate(expr) => physical_expr_is_sort_range_only(expr, column),
            BoolNode::Or(_) | BoolNode::Not(_) => false,
        }
    }
    if !conjuncts_ok(tree, column) {
        return false;
    }
    let collectors = tree.collector_leaf_count();
    let delegations = tree.delegation_possible_leaf_count();
    (collectors == 1 && delegations == 0) || (collectors == 0 && delegations <= 1)
}

/// True when `expr` consists SOLELY of conjunctive range comparisons
/// (`>`, `>=`, `<`, `<=`) between `column` and integer/timestamp literals.
/// Only then is the residual a tautology on a WITHIN row group, making the
/// count shortcut safe. OR/NOT, casts, other columns, and non-comparison
/// operators all return false (fail closed).
fn physical_expr_is_sort_range_only(expr: &Arc<dyn PhysicalExpr>, column: &str) -> bool {
    use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
    let Some(binary) = expr.as_ref().downcast_ref::<BinaryExpr>() else {
        return false;
    };
    match binary.op() {
        Operator::And => {
            physical_expr_is_sort_range_only(binary.left(), column)
                && physical_expr_is_sort_range_only(binary.right(), column)
        }
        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
            let column_vs_literal = |a: &Arc<dyn PhysicalExpr>, b: &Arc<dyn PhysicalExpr>| {
                a.downcast_ref::<Column>()
                    .is_some_and(|c| c.name() == column)
                    && b.downcast_ref::<Literal>()
                        .is_some_and(|l| scalar_as_i64(l.value()).is_some())
            };
            column_vs_literal(binary.left(), binary.right())
                || column_vs_literal(binary.right(), binary.left())
        }
        _ => false,
    }
}

/// Split a residual `PhysicalExpr` into its top-level AND conjuncts and drop
/// every conjunct that is `physical_expr_is_sort_range_only` on `column`.
///
/// Returns `(remaining, stripped_any)`:
/// - `remaining`: the AND of the surviving conjuncts, or `None` when every
///   conjunct was a sort-range conjunct (fully stripped → empty residual).
/// - `stripped_any`: `true` iff at least one sort-range conjunct was removed.
///
/// Fail-closed: only top-level `AND` nodes are split. Any other node
/// (a bare comparison, `OR`, `NOT`, a cast, a non-literal bound, …) is treated
/// as one opaque conjunct — a sort-range one is dropped, anything else is
/// kept verbatim. A residual that is not a clean AND of
/// strippable-plus-other conjuncts therefore yields `stripped_any = false`
/// (relaxed path disabled) or keeps the unstrippable part intact.
fn strip_sort_range_conjuncts(
    expr: &Arc<dyn PhysicalExpr>,
    column: &str,
) -> (Option<Arc<dyn PhysicalExpr>>, bool) {
    use datafusion::physical_expr::expressions::BinaryExpr;
    fn collect_conjuncts(e: &Arc<dyn PhysicalExpr>, out: &mut Vec<Arc<dyn PhysicalExpr>>) {
        if let Some(b) = e.as_ref().downcast_ref::<BinaryExpr>() {
            if matches!(b.op(), Operator::And) {
                collect_conjuncts(b.left(), out);
                collect_conjuncts(b.right(), out);
                return;
            }
        }
        out.push(Arc::clone(e));
    }
    let mut conjuncts = Vec::new();
    collect_conjuncts(expr, &mut conjuncts);

    let mut kept: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
    let mut stripped_any = false;
    for c in conjuncts {
        if physical_expr_is_sort_range_only(&c, column) {
            stripped_any = true;
        } else {
            kept.push(c);
        }
    }
    let remaining = match kept.len() {
        0 => None,
        _ => {
            let mut it = kept.into_iter();
            let first = it.next().unwrap();
            Some(it.fold(first, |acc, e| {
                Arc::new(BinaryExpr::new(acc, Operator::And, e)) as Arc<dyn PhysicalExpr>
            }))
        }
    };
    (remaining, stripped_any)
}

/// Result of walking a logical plan looking for the leading top-of-plan ORDER BY.
///
/// `column` is the bare column name (no qualifier — we compare against `index.sort.field`
/// which is also unqualified). `descending` is `true` for `ORDER BY x DESC`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopSort {
    pub column: String,
    pub descending: bool,
    /// Total row budget of the enclosing top-K: `Sort.fetch` when the
    /// optimizer pushed the limit into the sort, else fetch+skip of the
    /// Limit above the Sort. `None` when unbounded.
    pub fetch: Option<usize>,
    /// True when the Sort has exactly one sort key. Per-RG candidate
    /// truncation is only valid for a single-key sort (secondary keys can
    /// demand more than `fetch` rows from one RG on ties).
    pub single_key: bool,
}

/// Walk through the top of a logical plan to find the leading sort expression.
///
/// Descends through `Projection`, `Limit`, `SubqueryAlias`, `Distinct`, and `Filter` —
/// nodes that don't reorder rows or rewrite sort key columns. On reaching
/// `LogicalPlan::Sort`, returns the leading sort key's bare column name and direction.
/// Returns `None` if the plan has no top-level Sort, or if the first Sort key is not
/// a plain `Expr::Column` (e.g. `ORDER BY lower(x)` — we can't claim catalog monotonicity
/// after a function).
pub(crate) fn analyze_top_sort(plan: &datafusion::logical_expr::LogicalPlan) -> Option<TopSort> {
    use datafusion::logical_expr::{Expr, LogicalPlan};
    let mut current = plan;
    let mut limit_budget: Option<usize> = None;
    fn expr_as_usize(e: Option<&Expr>) -> Option<usize> {
        match e {
            Some(Expr::Literal(v, _)) => scalar_as_i64(v).and_then(|n| usize::try_from(n).ok()),
            _ => None,
        }
    }
    loop {
        match current {
            LogicalPlan::Sort(s) => {
                let leading = s.expr.first()?;
                let col = match &leading.expr {
                    Expr::Column(c) => c.name.clone(),
                    _ => return None,
                };
                return Some(TopSort {
                    column: col,
                    descending: !leading.asc,
                    fetch: s.fetch.or(limit_budget),
                    single_key: s.expr.len() == 1,
                });
            }
            LogicalPlan::Projection(p) => current = p.input.as_ref(),
            LogicalPlan::Limit(l) => {
                // Budget = fetch + skip (rows the top-K must fully produce).
                // A Limit with no literal fetch leaves the budget unbounded.
                limit_budget = expr_as_usize(l.fetch.as_deref()).map(|fetch| {
                    fetch.saturating_add(expr_as_usize(l.skip.as_deref()).unwrap_or(0))
                });
                current = l.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(a) => current = a.input.as_ref(),
            LogicalPlan::Distinct(d) => match d {
                datafusion::logical_expr::Distinct::All(input) => current = input.as_ref(),
                datafusion::logical_expr::Distinct::On(on) => current = on.input.as_ref(),
            },
            LogicalPlan::Filter(f) => current = f.input.as_ref(),
            _ => return None,
        }
    }
}

/// Decide the per-RG top-K candidate truncation `(keep_last, budget)` for a
/// rows-shape `sort <key> | head N` query, or `None` when truncation does not
/// apply.
///
/// Requires: at least one WITHIN row group (`within_present`), a single-key
/// top-level sort with a bounded fetch, and that the sort key matches the
/// catalog's leading `index.sort.field`. `keep_last` is relative to storage
/// order: `true` keeps the LAST `budget` candidates of a WITHIN RG (query
/// direction opposite the catalog's stored direction), `false` keeps the FIRST.
///
/// Multi-key sorts fail closed: a secondary key can demand more than `budget`
/// rows from a single RG on ties, so per-RG truncation would be unsafe.
fn compute_sort_topk_truncate(
    within_present: bool,
    top: Option<&TopSort>,
    sort_fields: &[String],
    sort_orders: &[String],
) -> Option<(bool, usize)> {
    if !within_present {
        return None;
    }
    let top = top?;
    if !top.single_key {
        return None;
    }
    let budget = top.fetch?;
    let catalog_field = sort_fields.first()?;
    if top.column != *catalog_field {
        return None;
    }
    let catalog_descending = sort_orders
        .first()
        .is_some_and(|o| o.eq_ignore_ascii_case("desc"));
    Some((top.descending != catalog_descending, budget))
}

/// Does `proj` carry the top-K sort key through as an **identity** column?
///
/// Scan-level candidate truncation targets the catalog's physical leading sort
/// column. A projection that produces the sort-key NAME from a *different*
/// column (`other AS logdate`) or from a computed expression (`foo + 1 AS
/// logdate`) would break that mapping, so the truncation would keep/drop the
/// wrong rows. This returns `true` only when the output field named `sort_key`
/// is produced by a plain `Expr::Column` referencing that same `sort_key`
/// (an alias whose inner expression is that identity column is also accepted).
/// If the projection does not output `sort_key` at all, it cannot legitimately
/// feed a downstream Sort on that key through an identity mapping — fail closed.
fn projection_preserves_sort_key(
    proj: &datafusion::logical_expr::Projection,
    sort_key: &str,
) -> bool {
    use datafusion::logical_expr::Expr;
    for (expr, field) in proj.expr.iter().zip(proj.schema.fields().iter()) {
        if field.name() == sort_key {
            let inner = match expr {
                Expr::Alias(a) => a.expr.as_ref(),
                other => other,
            };
            return matches!(inner, Expr::Column(c) if c.name == *sort_key);
        }
    }
    false
}

/// Is `proj` a safe projection to sit **ABOVE** the bounded top-K Sort?
///
/// Above-sort projections run *after* the Sort/truncation has already chosen
/// the surviving rows, so simply **dropping** the ordering column (the common
/// `SELECT id … ORDER BY ts` shape, where the final SELECT list omits `ts`) is
/// safe and stays eligible. A pure identity pass-through of the sort key is
/// likewise safe. The one shape we fail closed on is a **remap**: a projection
/// that manufactures a field with the sort key's NAME from a *different* or
/// computed source (`v AS ts`, `foo + 1 AS ts`). That would alias a foreign
/// column onto the name the catalog-order scan truncation keys on, so we reject
/// it. Returns `true` when the projection either does not output `sort_key` at
/// all, or outputs it as an identity `Expr::Column(sort_key)`.
fn above_sort_projection_ok(proj: &datafusion::logical_expr::Projection, sort_key: &str) -> bool {
    use datafusion::logical_expr::Expr;
    for (expr, field) in proj.expr.iter().zip(proj.schema.fields().iter()) {
        if field.name() == sort_key {
            let inner = match expr {
                Expr::Alias(a) => a.expr.as_ref(),
                other => other,
            };
            // Outputs the sort-key name → allowed ONLY as an identity ref.
            return matches!(inner, Expr::Column(c) if c.name == *sort_key);
        }
    }
    // Sort key not present in the output → dropped after sorting → safe.
    true
}

/// Strict, fail-closed eligibility for **scan-level** per-RG top-K candidate
/// truncation.
///
/// Returns the bounded top-K [`TopSort`] **only** when a leading bounded Sort
/// exists AND the path from that Sort down to the indexed `TableScan` passes
/// exclusively through the row/sort-key-preserving allowlist below. Any barrier
/// returns `None`.
///
/// Why this exists (separate from [`analyze_top_sort`]): truncation keeps only
/// the first/last `budget` candidates of each Top-K-WITHIN row group. That is
/// correct only when every row it drops provably cannot re-enter the answer.
/// [`analyze_top_sort`] returns as soon as it reaches the top Sort and never
/// inspects the operators *beneath* it — so the unsafe candidate `7225d84e`
/// armed truncation below a `ROW_NUMBER`/dedup window (which needs every row of
/// each partition), silently corrupting Q5. [`analyze_top_sort`] stays
/// permissive because it only drives the segment-iteration reversal, a
/// read-only reorder that is safe regardless of what sits below the Sort. Only
/// truncation eligibility uses this stricter walk.
///
/// The walk is split into two phases with DIFFERENT allowlists, because an
/// operator that is safe above the Sort is not necessarily safe below it (and
/// vice-versa):
///
/// Phase A — root → bounded Sort (operators that run AFTER truncation):
///   * `Limit`                           — captures the fetch/skip budget; only
///                                          removes rows, never adds
///   * `SubqueryAlias`                   — pure rename of the relation
///   * `Projection`                      — recorded, then validated once the
///                                          sort key is known: it must either
///                                          drop the sort key or pass it through
///                                          as identity (`above_sort_projection_ok`);
///                                          a remap (`v AS ts`) fails closed
///   BARRIERS here: `Filter` (drops rows the budget assumed present),
///   `Distinct`, and any unknown node.
///
/// Phase B — bounded Sort → indexed `TableScan` (operators the scan feeds):
///   * `TableScan`                       — terminal (reached the indexed scan)
///   * `Filter`                          — its predicate is executed IN the scan
///                                          path; truncation runs on the surviving
///                                          (post-filter) candidate bitmap
///   * `SubqueryAlias`                   — pure rename of the relation
///   * `Projection` (identity sort key)  — only if it carries the sort key
///                                          through unchanged (`projection_preserves_sort_key`)
///   BARRIERS here (fail closed → `None`): `Limit` (an inner limit truncates
///   rows the top-K still needs), an inner `Sort`, `Repartition`,
///   `Window`/`BoundedWindowAgg`/`ROW_NUMBER`, `Aggregate`, `Join`/`CrossJoin`,
///   `Distinct`, `Union`/`Intersect`/`Except`, `Unnest`, `RecursiveQuery`,
///   `Extension`, `Values`/`EmptyRelation`, and any other unknown/
///   cardinality-changing/multi-input node.
pub(crate) fn analyze_scan_topk_truncation_path(
    plan: &datafusion::logical_expr::LogicalPlan,
) -> Option<TopSort> {
    use datafusion::logical_expr::{Expr, LogicalPlan};

    fn expr_as_usize(e: Option<&Expr>) -> Option<usize> {
        match e {
            Some(Expr::Literal(v, _)) => scalar_as_i64(v).and_then(|n| usize::try_from(n).ok()),
            _ => None,
        }
    }

    // ── Phase A: locate the leading bounded Sort ────────────────────────────
    // Descend ONLY the wrappers that may legitimately sit ABOVE a top-K Sort:
    // `Limit` (fetch/skip budget), `SubqueryAlias`, and `Projection`. The sort
    // key is not known until the Sort is found, so above-sort projections are
    // recorded here and validated (identity-or-dropped) once we have the key.
    // `Filter`, `Distinct`, and any unknown node above the Sort are BARRIERS: a
    // Filter above the Sort removes rows the per-RG truncation budget assumed
    // were present, and `Distinct` changes cardinality.
    let mut current = plan;
    let mut limit_budget: Option<usize> = None;
    let mut above_sort_projections: Vec<&datafusion::logical_expr::Projection> = Vec::new();
    let (top, sort_input): (TopSort, &LogicalPlan) = loop {
        match current {
            LogicalPlan::Sort(s) => {
                let leading = s.expr.first()?;
                let col = match &leading.expr {
                    Expr::Column(c) => c.name.clone(),
                    _ => return None,
                };
                let top = TopSort {
                    column: col,
                    descending: !leading.asc,
                    fetch: s.fetch.or(limit_budget),
                    single_key: s.expr.len() == 1,
                };
                break (top, s.input.as_ref());
            }
            LogicalPlan::Projection(p) => {
                above_sort_projections.push(p);
                current = p.input.as_ref();
            }
            LogicalPlan::Limit(l) => {
                limit_budget = expr_as_usize(l.fetch.as_deref()).map(|fetch| {
                    fetch.saturating_add(expr_as_usize(l.skip.as_deref()).unwrap_or(0))
                });
                current = l.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(a) => current = a.input.as_ref(),
            // `Filter`, `Distinct`, and everything else above the Sort fail closed.
            _ => return None,
        }
    };

    // Now that the sort key is known, validate every recorded above-sort
    // projection: each must either drop the sort key or preserve it as an
    // identity column. A projection that remaps the sort-key name onto a
    // different/computed source fails closed.
    for proj in &above_sort_projections {
        if !above_sort_projection_ok(proj, &top.column) {
            return None;
        }
    }

    // ── Phase B: verify Sort → TableScan is all-safe (fail closed) ──────────
    let sort_key = top.column.clone();
    let mut node = sort_input;
    loop {
        match node {
            LogicalPlan::TableScan(_) => return Some(top),
            LogicalPlan::Filter(f) => node = f.input.as_ref(),
            LogicalPlan::SubqueryAlias(a) => node = a.input.as_ref(),
            LogicalPlan::Projection(p) => {
                if !projection_preserves_sort_key(p, &sort_key) {
                    return None;
                }
                node = p.input.as_ref();
            }
            // Any operator not explicitly allowed above is a barrier: `Limit`
            // (truncates rows the top-K still needs), an inner `Sort`,
            // `Repartition`, `Window`/`ROW_NUMBER`, `Aggregate`, `Join`,
            // `Distinct`, set ops, and any unknown/future node all fail closed.
            _ => return None,
        }
    }
}

/// Decide whether to flip segment iteration order for the indexed scan.
///
/// Returns `true` iff the catalog has a sort declaration, the query has a top-level
/// ORDER BY whose leading key matches the catalog's leading sort field by name, and
/// the query's direction is the **opposite** of the catalog's. In that case the
/// segments — laid down newest-last by the writer — are in reverse order from what
/// the query wants, so iterating them tail-first feeds the largest values to a
/// `TopK` first and parquet page stats prune the rest.
///
/// All comparisons are case-sensitive on the field name (matching PR #22041's
/// `Column::from_name`). Direction comparison is case-insensitive on `"asc"`/`"desc"`.
pub(crate) fn should_reverse_segments(
    top_sort: Option<&TopSort>,
    sort_fields: &[String],
    sort_orders: &[String],
) -> bool {
    let Some(top) = top_sort else { return false };
    let Some(catalog_field) = sort_fields.first() else {
        return false;
    };
    let Some(catalog_order) = sort_orders.first() else {
        return false;
    };
    if top.column != *catalog_field {
        return false;
    }
    let catalog_descending = catalog_order.eq_ignore_ascii_case("desc");
    top.descending != catalog_descending
}

/// Reverse the iteration order of `segments` in place. Per-segment `global_base`
/// values are deliberately **left untouched** so each segment keeps its
/// catalog-order shard-global row ID space.
///
/// Why not recompute global_base after reversing?
///
/// `global_base` is the additive offset used to compute the QTF `__row_id__`:
/// `id = segment.global_base + position`. The fetch phase (`api::fetch_by_row_ids`)
/// rebuilds segments via `build_segments` against `ShardView.object_metas` (always
/// in catalog order) and reverses the mapping back via `partition_point` on
/// `global_base`. For the round trip to hold, both phases must agree on each
/// segment's `global_base` — and the only way to guarantee that without changing
/// the fetch path is to keep query-phase `global_base` at its catalog-order value.
///
/// Reversing only the iteration order — i.e. the order in which chunks/RGs are
/// scheduled by `compute_assignments` — is the *whole* point of this optimization
/// (newest-segment-first feeds TopK earlier and lets page stats prune older
/// segments). The `global_base` values are unrelated to that pruning win.
pub(crate) fn reverse_segment_iteration_order(segments: &mut [SegmentFileInfo]) {
    segments.reverse();
}

/// Collect all `Predicate(expr)` leaves in DFS order. Used by the
/// dispatcher to build a per-leaf `PruningPredicate` cache keyed by
/// `Arc::as_ptr` identity.
fn collect_predicate_exprs(tree: &BoolNode, out: &mut Vec<Arc<dyn PhysicalExpr>>) {
    match tree {
        BoolNode::And(c) | BoolNode::Or(c) => {
            c.iter().for_each(|ch| collect_predicate_exprs(ch, out))
        }
        BoolNode::Not(inner) => collect_predicate_exprs(inner, out),
        BoolNode::Collector { .. } => {}
        // Performance-delegated leaves contribute their original expression to the
        // PruningPredicate cache exactly like a plain Predicate — DF prunes pages
        // using the original expr; only the per-RG decision differs.
        BoolNode::DelegationPossible { original_expr, .. } => out.push(Arc::clone(original_expr)),
        BoolNode::Predicate(expr) => out.push(Arc::clone(expr)),
    }
}

/// Collect leaf predicate exprs from the extraction tree in a single traversal.
fn collect_leaf_exprs(extraction: Option<&ExtractionResult>) -> Vec<Arc<dyn PhysicalExpr>> {
    let Some(e) = extraction else { return vec![] };
    let mut exprs = Vec::new();
    collect_predicate_exprs(&e.tree, &mut exprs);
    exprs
}

fn collect_predicate_column_indices(extraction: Option<&ExtractionResult>) -> Vec<usize> {
    let Some(e) = extraction else { return vec![] };
    let mut exprs = Vec::new();
    collect_predicate_exprs(&e.tree, &mut exprs);
    collect_predicate_column_indices_from_exprs(&exprs)
}

fn collect_predicate_column_indices_from_exprs(exprs: &[Arc<dyn PhysicalExpr>]) -> Vec<usize> {
    let mut indices = HashSet::new();
    for expr in exprs {
        let _ = expr.apply(|node| {
            if let Some(col) = node.downcast_ref::<Column>() {
                indices.insert(col.index());
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    indices.into_iter().collect()
}

fn collect_predicate_column_names(
    extraction: Option<&ExtractionResult>,
    schema: &SchemaRef,
) -> Vec<String> {
    let Some(e) = extraction else { return vec![] };
    let mut exprs = Vec::new();
    collect_predicate_exprs(&e.tree, &mut exprs);
    let mut names = HashSet::new();
    for expr in &exprs {
        let _ = expr.apply(|node| {
            if let Some(col) = node.downcast_ref::<Column>() {
                if let Some(field) = schema.fields().get(col.index()) {
                    names.insert(field.name().to_string());
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    names.into_iter().collect()
}

fn collect_plan_column_names(plan: &datafusion::logical_expr::LogicalPlan) -> Vec<String> {
    let mut names = HashSet::new();
    let _ = plan.apply(|node| {
        // Output-schema columns of every node: this is what each node actually
        // emits / reads. Critically this captures `SELECT *` (and any projection
        // pushed into the scan), where no Projection expression lists the columns
        // but every column is still read. Expression-only collection misses them,
        // and a read column that gets only a placeholder OffsetIndex (instead of
        // its real multi-page one) corrupts the page read: arrow decodes the whole
        // column chunk as a single page → "output too small for decompressed data"
        // (or, upstream, the (0,0)-page byte-range subtract underflow).
        for field in node.schema().fields() {
            names.insert(field.name().to_string());
        }
        let _ = node.apply_expressions(|expr| {
            let _ = expr.apply(|e| {
                if let Expr::Column(col) = e {
                    names.insert(col.name().to_string());
                }
                Ok(TreeNodeRecursion::Continue)
            });
            Ok(TreeNodeRecursion::Continue)
        });
        Ok(TreeNodeRecursion::Continue)
    });
    names.into_iter().collect()
}

/// Build the `prune_tree_config` tuple from a BoolNode tree and schema.
/// Builds per-leaf PruningPredicates from pre-collected leaf exprs.
fn build_prune_tree_config(
    tree: &Arc<BoolNode>,
    schema: &SchemaRef,
    leaf_exprs: &[Arc<dyn PhysicalExpr>],
) -> Option<(
    Arc<BoolNode>,
    Arc<HashMap<usize, Arc<PruningPredicate>>>,
    SchemaRef,
)> {
    let leaf_predicates: HashMap<usize, Arc<PruningPredicate>> = leaf_exprs
        .iter()
        .filter_map(|expr| {
            build_pruning_predicate(expr, Arc::clone(schema))
                .map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
        })
        .collect();
    if leaf_predicates.is_empty() {
        return None;
    }
    Some((
        Arc::clone(tree),
        Arc::new(leaf_predicates),
        Arc::clone(schema),
    ))
}

/// For a tree classified as `SingleCollector`, walk it to find the single
/// Collector leaf and return its query bytes.
fn single_collector_id(tree: &BoolNode) -> Option<i32> {
    match tree {
        BoolNode::Collector { annotation_id } => Some(*annotation_id),
        BoolNode::And(children) => {
            for child in children {
                if let Some(id) = single_collector_id(child) {
                    return Some(id);
                }
            }
            None
        }
        _ => None,
    }
}

/// For a tree classified as `SingleCollector`, return the residual
/// (all non-Collector parts of the AND tree, re-assembled into a
/// single BoolNode). Recursively strips Collector leaves from nested
/// ANDs. Returns `None` if the tree is a bare Collector or the entire
/// tree is collectors-only (no residual predicates).
fn extract_single_collector_residual(tree: &BoolNode) -> Option<BoolNode> {
    fn strip_collectors(node: &BoolNode) -> Option<BoolNode> {
        match node {
            BoolNode::Collector { .. } => None,
            // Performance-delegated (dual-viable) leaves are handled separately as
            // `PerformanceLeaf`s with a per-RG DataFusion-XOR-Lucene choice — they must NOT be
            // baked into the always-applied residual (which would statically push their expr to
            // parquet and evaluate them even when the per-RG choice selects Lucene). Strip them
            // here; the evaluator applies a DataFusion-selected leaf's expr post-decode only.
            BoolNode::DelegationPossible { .. } => None,
            BoolNode::Predicate(_) => Some(node.clone()),
            BoolNode::And(children) => {
                let residuals: Vec<BoolNode> =
                    children.iter().filter_map(strip_collectors).collect();
                match residuals.len() {
                    0 => None,
                    1 => Some(residuals.into_iter().next().unwrap()),
                    _ => Some(BoolNode::And(residuals)),
                }
            }
            // OR/NOT with no collectors pass through unchanged (they're
            // pure-predicate subtrees in a SingleCollector-classified tree).
            other => Some(other.clone()),
        }
    }
    strip_collectors(tree)
}

// ── Placeholder provider used only for substrait consume pass ─────────

struct PlaceholderProvider {
    schema: SchemaRef,
}

impl fmt::Debug for PlaceholderProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceholderProvider").finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for PlaceholderProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Internal(
            "PlaceholderProvider should not be scanned".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::bool_tree::BoolNode;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
    use datafusion::physical_expr::PhysicalExpr;
    use object_store::path::Path as ObjectPath;
    use std::sync::Arc;

    fn collector(id: i32) -> BoolNode {
        BoolNode::Collector { annotation_id: id }
    }

    fn pred() -> BoolNode {
        let left: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new("price", 0));
        let right: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(0))));
        BoolNode::Predicate(Arc::new(BinaryExpr::new(left, Operator::Eq, right)))
    }

    fn is_predicate(node: &BoolNode) -> bool {
        matches!(node, BoolNode::Predicate(_))
    }

    // ── extract_single_collector_residual ─────────────────────────────

    #[test]
    fn residual_bare_collector_is_none() {
        assert!(extract_single_collector_residual(&collector(10)).is_none());
    }

    #[test]
    fn residual_and_collector_plus_predicate() {
        let tree = BoolNode::And(vec![collector(10), pred()]);
        let r = extract_single_collector_residual(&tree).unwrap();
        assert!(is_predicate(&r));
    }

    #[test]
    fn residual_and_only_collectors_is_none() {
        let tree = BoolNode::And(vec![collector(10), collector(11)]);
        assert!(extract_single_collector_residual(&tree).is_none());
    }

    #[test]
    fn residual_nested_and_strips_collectors() {
        // AND(C₁, AND(C₂, P)) → residual is P
        let tree = BoolNode::And(vec![
            collector(10),
            BoolNode::And(vec![collector(11), pred()]),
        ]);
        let r = extract_single_collector_residual(&tree).unwrap();
        assert!(is_predicate(&r));
    }

    #[test]
    fn residual_deeply_nested_and() {
        // AND(P₁, AND(C₁, AND(C₂, P₂))) → AND(P₁, P₂)
        let p1 = pred();
        let p2 = pred();
        let tree = BoolNode::And(vec![
            p1,
            BoolNode::And(vec![collector(0), BoolNode::And(vec![collector(1), p2])]),
        ]);
        let r = extract_single_collector_residual(&tree).unwrap();
        match r {
            BoolNode::And(children) => {
                assert_eq!(children.len(), 2);
                assert!(children.iter().all(is_predicate));
            }
            _ => panic!("expected AND, got {:?}", r),
        }
    }

    #[test]
    fn residual_nested_and_with_or_predicate() {
        // AND(C, AND(P, OR(P, P))) → AND(P, OR(P, P))
        let tree = BoolNode::And(vec![
            collector(10),
            BoolNode::And(vec![pred(), BoolNode::Or(vec![pred(), pred()])]),
        ]);
        let r = extract_single_collector_residual(&tree).unwrap();
        match r {
            BoolNode::And(children) => {
                assert_eq!(children.len(), 2);
                assert!(is_predicate(&children[0]));
                assert!(matches!(children[1], BoolNode::Or(_)));
            }
            _ => panic!("expected AND, got {:?}", r),
        }
    }

    #[test]
    fn residual_nested_and_all_collectors_is_none() {
        // AND(AND(C₁, C₂), AND(C₃, C₄)) → no residual
        let tree = BoolNode::And(vec![
            BoolNode::And(vec![collector(0), collector(1)]),
            BoolNode::And(vec![collector(2), collector(3)]),
        ]);
        assert!(extract_single_collector_residual(&tree).is_none());
    }

    // ── analyze_top_sort / should_reverse_segments ────────────────────

    fn build_logical_plan(sql: &str) -> datafusion::logical_expr::LogicalPlan {
        use datafusion::execution::context::SessionContext;
        use datafusion::execution::SessionStateBuilder;
        let state = SessionStateBuilder::new().with_default_features().build();
        let ctx = SessionContext::new_with_state(state);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int32, false),
        ]));
        ctx.register_batch(
            "t",
            datafusion::arrow::record_batch::RecordBatch::new_empty(schema),
        )
        .expect("register_batch");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let df = rt.block_on(ctx.sql(sql)).expect("sql");
        df.into_unoptimized_plan()
    }

    #[test]
    fn analyze_top_sort_finds_leading_desc() {
        let plan = build_logical_plan("SELECT id FROM t ORDER BY id DESC");
        let ts = analyze_top_sort(&plan).expect("expected Sort");
        assert_eq!(ts.column, "id");
        assert!(ts.descending);
    }

    #[test]
    fn analyze_top_sort_descends_through_limit() {
        let plan = build_logical_plan("SELECT id FROM t ORDER BY id ASC LIMIT 10");
        let ts = analyze_top_sort(&plan).expect("expected Sort");
        assert_eq!(ts.column, "id");
        assert!(!ts.descending);
    }

    #[test]
    fn analyze_top_sort_descends_through_projection() {
        let plan = build_logical_plan("SELECT v FROM t ORDER BY ts DESC");
        let ts = analyze_top_sort(&plan).expect("expected Sort");
        assert_eq!(ts.column, "ts");
        assert!(ts.descending);
    }

    #[test]
    fn analyze_top_sort_extracts_fetch_and_single_key() {
        let plan = build_logical_plan("SELECT id FROM t ORDER BY id DESC LIMIT 100");
        let top = analyze_top_sort(&plan).expect("sort");
        assert_eq!(top.column, "id");
        assert!(top.descending);
        assert_eq!(top.fetch, Some(100));
        assert!(top.single_key);

        let two = build_logical_plan("SELECT id, v FROM t ORDER BY id DESC, v ASC LIMIT 10");
        let top2 = analyze_top_sort(&two).expect("sort");
        assert!(!top2.single_key);

        let unbounded = build_logical_plan("SELECT id FROM t ORDER BY id DESC");
        let top3 = analyze_top_sort(&unbounded).expect("sort");
        assert_eq!(top3.fetch, None);
    }

    #[test]
    fn analyze_top_sort_returns_none_when_no_sort() {
        let plan = build_logical_plan("SELECT id FROM t");
        assert!(analyze_top_sort(&plan).is_none());
    }

    // ── analyze_scan_topk_truncation_path (fail-closed Sort→scan guard) ────
    //
    // Truncation eligibility must depend on the STRICT path guard, not the
    // permissive `analyze_top_sort`. These use real DataFusion logical plans
    // (`build_logical_plan`) so the operator variants (Window/Aggregate/…) are
    // exactly what the executor sees.

    #[test]
    fn scan_topk_path_q4_direct_filter_sort_head_eligible() {
        // Q4: `match(...) AND ts-range | sort ts | head N` — a Filter (whose
        // predicate the scan executes) between the bounded Sort and the scan.
        let plan = build_logical_plan(
            "SELECT id FROM t WHERE ts >= 1 AND ts < 100 ORDER BY ts DESC LIMIT 100",
        );
        let top = analyze_scan_topk_truncation_path(&plan).expect("Q4 path must be eligible");
        assert_eq!(top.column, "ts");
        assert!(top.descending);
        assert!(top.single_key);
        assert_eq!(top.fetch, Some(100));
        // And the shape gate arms on the matching catalog sort field.
        let fields = vec!["ts".to_string()];
        let orders = vec!["desc".to_string()];
        assert_eq!(
            compute_sort_topk_truncate(true, Some(&top), &fields, &orders),
            Some((false, 100))
        );
    }

    #[test]
    fn scan_topk_path_q1_q6_single_key_sort_eligible() {
        // Q1/Q6: single-key sort + head over a filtered scan (identity
        // projection of the output columns, incl. the sort key).
        let plan = build_logical_plan("SELECT id, ts FROM t WHERE v > 0 ORDER BY ts ASC LIMIT 50");
        let top = analyze_scan_topk_truncation_path(&plan).expect("Q1/Q6 path must be eligible");
        assert_eq!(top.column, "ts");
        assert!(!top.descending);
        assert!(top.single_key);
        assert_eq!(top.fetch, Some(50));
    }

    #[test]
    fn scan_topk_path_q3_multikey_path_safe_but_shape_declines() {
        // Q3: multi-key sort. The PATH is safe (Filter → scan), so the strict
        // guard returns a TopSort — but `single_key == false`, so the existing
        // shape gate (`compute_sort_topk_truncate`) declines. Truncation off.
        let plan =
            build_logical_plan("SELECT id FROM t WHERE ts < 100 ORDER BY ts DESC, v ASC LIMIT 100");
        let top = analyze_scan_topk_truncation_path(&plan).expect("path is safe");
        assert!(!top.single_key, "multi-key sort");
        let fields = vec!["ts".to_string()];
        let orders = vec!["desc".to_string()];
        assert_eq!(
            compute_sort_topk_truncate(true, Some(&top), &fields, &orders),
            None,
            "multi-key declines by the existing single_key gate"
        );
    }

    #[test]
    fn scan_topk_path_q5_window_row_number_barrier_declines() {
        // Q5: a ROW_NUMBER window (+ dedup Filter) between the bounded top-K
        // Sort and the scan. This is the unsafe candidate `7225d84e` shape.
        // The strict guard MUST fail closed at the Window barrier so
        // truncation never arms (activation_topk_range_within_rgs stays off).
        let plan = build_logical_plan(
            "SELECT id, ts FROM ( \
               SELECT id, ts, ROW_NUMBER() OVER (PARTITION BY v ORDER BY ts DESC) AS rn FROM t \
             ) WHERE rn = 1 ORDER BY ts DESC LIMIT 100",
        );
        // The permissive analyzer still finds the top Sort (drives only segment
        // reversal) — proving the two functions genuinely diverge here.
        assert!(
            analyze_top_sort(&plan).is_some(),
            "permissive analyze_top_sort still sees the top Sort"
        );
        // The strict guard rejects the path.
        assert!(
            analyze_scan_topk_truncation_path(&plan).is_none(),
            "Window/ROW_NUMBER between Sort and scan must fail closed"
        );
    }

    #[test]
    fn scan_topk_path_aggregate_barrier_declines() {
        // GROUP BY places an Aggregate between the Sort and the scan.
        let plan =
            build_logical_plan("SELECT ts, count(*) FROM t GROUP BY ts ORDER BY ts DESC LIMIT 100");
        assert!(
            analyze_scan_topk_truncation_path(&plan).is_none(),
            "Aggregate barrier must fail closed"
        );
    }

    #[test]
    fn scan_topk_path_distinct_barrier_declines() {
        // SELECT DISTINCT inserts a dedup (Aggregate/Distinct) below the Sort.
        let plan = build_logical_plan("SELECT DISTINCT id FROM t ORDER BY id DESC LIMIT 100");
        assert!(
            analyze_scan_topk_truncation_path(&plan).is_none(),
            "Distinct/dedup barrier must fail closed"
        );
    }

    #[test]
    fn scan_topk_path_join_barrier_declines() {
        // A self-join is a multi-input barrier between the Sort and the scan.
        let plan = build_logical_plan(
            "SELECT t1.id FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY t1.ts DESC LIMIT 100",
        );
        assert!(
            analyze_scan_topk_truncation_path(&plan).is_none(),
            "Join barrier must fail closed"
        );
    }

    #[test]
    fn scan_topk_path_set_op_barrier_declines() {
        // UNION ALL is a multi-input set operation — the catch-all `_` arm
        // (which also covers any unknown/future operator) must fail closed.
        let plan = build_logical_plan(
            "SELECT id, ts FROM t UNION ALL SELECT id, ts FROM t ORDER BY ts DESC LIMIT 100",
        );
        assert!(
            analyze_scan_topk_truncation_path(&plan).is_none(),
            "Union/set-op (and any unknown operator) must fail closed"
        );
    }

    #[test]
    fn scan_topk_path_projection_remapping_sort_key_declines() {
        // A projection between the Sort and the scan that produces the sort-key
        // NAME from a DIFFERENT column (`v AS ts`) remaps the mapping the
        // catalog-order scan truncation relies on — must fail closed even though
        // the only operator on the path is an (otherwise-allowed) Projection.
        let plan = build_logical_plan("SELECT id, v AS ts FROM t ORDER BY ts DESC LIMIT 100");
        assert!(
            analyze_scan_topk_truncation_path(&plan).is_none(),
            "non-identity projection of the sort key must fail closed"
        );
    }

    #[test]
    fn filter_above_sort_declines() {
        // A Filter that sits ABOVE the bounded Sort drops rows the per-RG
        // truncation budget assumed were present — the scan may have already
        // truncated candidates the above-sort Filter would have kept, so the
        // answer is corrupt. Phase A must fail closed at the Filter.
        let plan = build_logical_plan(
            "SELECT id, ts FROM \
               (SELECT id, ts FROM t WHERE ts < 100 ORDER BY ts DESC LIMIT 100) sub \
             WHERE ts < 50",
        );
        // The permissive analyzer still descends the Filter and finds the Sort
        // (it drives only the read-only segment reversal).
        assert!(
            analyze_top_sort(&plan).is_some(),
            "permissive analyze_top_sort still sees the top Sort"
        );
        assert!(
            analyze_scan_topk_truncation_path(&plan).is_none(),
            "Filter above the bounded Sort must fail closed"
        );
    }

    #[test]
    fn limit_below_sort_declines() {
        // An inner Limit BETWEEN the bounded Sort and the scan truncates rows
        // the top-K still needs (the outer Sort must see all rows the inner
        // Limit removed). Phase B must fail closed at the inner Limit.
        let plan = build_logical_plan(
            "SELECT id, ts FROM \
               (SELECT id, ts FROM t WHERE ts < 100 LIMIT 1000) sub \
             ORDER BY ts DESC LIMIT 100",
        );
        assert!(
            analyze_scan_topk_truncation_path(&plan).is_none(),
            "Limit below the bounded Sort must fail closed"
        );
    }

    #[test]
    fn remapping_projection_above_sort_declines() {
        // A projection ABOVE the Sort that manufactures the sort-key NAME (`ts`)
        // from a DIFFERENT column (`w`, i.e. `v`) aliases a foreign column onto
        // the name the catalog-order truncation keys on. Even though the only
        // above-sort operator is an otherwise-allowed Projection, this remap
        // must fail closed (validated after the sort key is discovered).
        let plan = build_logical_plan(
            "SELECT id, w AS ts FROM \
               (SELECT id, ts, v AS w FROM t WHERE ts < 100 ORDER BY ts DESC LIMIT 100) sub",
        );
        assert!(
            analyze_scan_topk_truncation_path(&plan).is_none(),
            "remapping projection above the Sort must fail closed"
        );
    }

    #[test]
    fn identity_projection_above_sort_eligible() {
        // A projection ABOVE the Sort that passes the sort key through as an
        // identity column (`ts` → `ts`) is safe: it neither drops rows nor
        // remaps the key. The path stays eligible and returns the bounded
        // TopSort with its fetch budget intact.
        let plan = build_logical_plan(
            "SELECT id, ts FROM \
               (SELECT id, ts FROM t WHERE ts < 100 ORDER BY ts DESC LIMIT 100) sub",
        );
        let top = analyze_scan_topk_truncation_path(&plan)
            .expect("identity projection above the Sort must stay eligible");
        assert_eq!(top.column, "ts");
        assert!(top.descending);
        assert!(top.single_key);
        assert_eq!(top.fetch, Some(100));
    }

    #[test]
    fn scan_topk_path_unbounded_sort_carries_no_fetch() {
        // No LIMIT: path is safe and a TopSort is returned, but `fetch == None`
        // so the shape gate declines (truncation needs a bounded budget).
        let plan = build_logical_plan("SELECT id FROM t WHERE ts < 100 ORDER BY ts DESC");
        let top = analyze_scan_topk_truncation_path(&plan).expect("path safe");
        assert_eq!(top.fetch, None);
        let fields = vec!["ts".to_string()];
        let orders = vec!["desc".to_string()];
        assert_eq!(
            compute_sort_topk_truncate(true, Some(&top), &fields, &orders),
            None,
            "unbounded sort has no budget → no truncation"
        );
    }

    #[test]
    fn scan_topk_path_no_sort_returns_none() {
        let plan = build_logical_plan("SELECT id FROM t");
        assert!(analyze_scan_topk_truncation_path(&plan).is_none());
    }

    // ── fully-covered sort-range simplification ───────────────────────

    #[test]
    fn extracts_inclusive_and_exclusive_sort_range() {
        let plan = build_logical_plan("SELECT count(*) FROM t WHERE ts > 9 AND ts < 101");
        let range = extract_sort_range(&plan, "ts").expect("range");
        assert_eq!(range.lower, Some(10));
        assert_eq!(range.upper, Some(100));
    }

    // ── const-fold of function/cast-wrapped sort-range bounds (Q4 root cause) ──

    #[test]
    fn unfolded_function_bound_hides_sort_range_until_const_folded() {
        // Mirrors the PPL Q4 shape: bounds arrive as non-literal scalar-function
        // expressions in the RAW substrait plan. `into_unoptimized_plan` keeps
        // them unfolded, exactly like `from_substrait_plan` — so `abs(..)` here
        // stands in for PPL's `timestamp('..')` lowering.
        let plan =
            build_logical_plan("SELECT count(*) FROM t WHERE ts >= abs(-10) AND ts < abs(-101)");
        let predicate = extract_filter_expr(&plan).expect("filter predicate");

        // Pre-fix behavior: the bound is a scalar function, not a literal →
        // `comparison_bound` returns None → no sort range recognized. This is
        // exactly why single-shard Q4 never populated `sort_range_within_rgs`
        // and `rg_topk_truncated` stayed 0.
        assert!(
            sort_range_from_predicate(&predicate, "ts", None).is_none(),
            "unfolded scalar-function bound must not be recognized as a sort range"
        );

        // Fix: const-fold the constant bound operands, then the range is recovered.
        let folded = const_fold_comparison_bounds(predicate, plan.schema());
        let range = sort_range_from_predicate(&folded, "ts", None).expect("range after const-fold");
        assert_eq!(range.lower, Some(10));
        assert_eq!(range.upper, Some(100));
    }

    #[test]
    fn const_fold_leaves_column_referencing_operands_untouched() {
        // A comparison whose non-column side references a column must never be
        // folded (it is not constant), so no bogus sort range is produced.
        let plan = build_logical_plan("SELECT ts FROM t WHERE ts >= id");
        let predicate = extract_filter_expr(&plan).expect("filter predicate");
        let folded = const_fold_comparison_bounds(predicate, plan.schema());
        assert!(sort_range_from_predicate(&folded, "ts", None).is_none());
    }

    /// Build a logical plan over a Timestamp(ns)-typed `logdate` column, whose
    /// range bounds are `to_timestamp('...')` scalar-function calls — the exact
    /// shape PPL's temporal lowering produces (`TIMESTAMP(string) -> to_timestamp`
    /// via `scalarFunctionAdapters`). The *unoptimized* plan is serialized to
    /// Substrait (Isthmus does no DataFusion optimization, so the constant
    /// `to_timestamp('...')` bound is NOT pre-folded on the wire) and then
    /// reconstructed with `from_substrait_plan` — byte-for-byte the executor's
    /// own consumer path (`execute_indexed_with_context_inner`). Returns the
    /// reconstructed plan.
    fn roundtrip_ppl_timestamp_plan(sql: &str) -> LogicalPlan {
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::execution::context::SessionContext;
        use datafusion::execution::SessionStateBuilder;
        use datafusion_substrait::logical_plan::producer::to_substrait_plan;

        let state = SessionStateBuilder::new().with_default_features().build();
        let ctx = SessionContext::new_with_state(state);
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "logdate",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("app_component", DataType::Utf8, false),
        ]));
        ctx.register_batch("t", RecordBatch::new_empty(schema))
            .expect("register_batch");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            // UNOPTIMIZED plan: mirror Isthmus, which emits the raw bound as a
            // scalar-function call rather than a const-folded literal.
            let unoptimized = ctx.sql(sql).await.expect("sql").into_unoptimized_plan();
            let substrait = to_substrait_plan(&unoptimized, &ctx.state())
                .expect("to_substrait_plan (producer) must support to_timestamp bounds");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode substrait");
            let decoded = Plan::decode(buf.as_slice()).expect("decode substrait");
            // Exact executor consumer path.
            from_substrait_plan(&ctx.state(), &decoded)
                .await
                .expect("from_substrait_plan (consumer)")
        })
    }

    /// End-to-end root-cause regression for single-shard Q4: a real
    /// PPL->Substrait->executor roundtrip carrying `to_timestamp('...')` range
    /// bounds. Proves (a) the reconstructed bound is NOT a literal, so the
    /// pre-fix `sort_range_from_predicate` finds no range (this is exactly why
    /// `sort_range_within_rgs` was empty and `rg_topk_truncated` stayed 0 on the
    /// benchmark node); and (b) after `const_fold_comparison_bounds` the range
    /// is recovered with the correct nanosecond bounds, re-enabling WITHIN
    /// classification and per-RG top-K truncation.
    #[test]
    fn ppl_substrait_timestamp_bounds_recovered_after_const_fold() {
        // 2026-02-08T00:00:00Z .. 2026-02-09T00:00:00Z (exclusive upper).
        let lower_ns: i64 = 1_770_508_800_000_000_000; // 2026-02-08T00:00:00Z
        let upper_ns: i64 = 1_770_595_200_000_000_000; // 2026-02-09T00:00:00Z
        let plan = roundtrip_ppl_timestamp_plan(
            "SELECT logdate FROM t \
             WHERE logdate >= to_timestamp('2026-02-08T00:00:00Z') \
               AND logdate < to_timestamp('2026-02-09T00:00:00Z') \
             ORDER BY logdate DESC LIMIT 100",
        );
        let predicate = extract_filter_expr(&plan).expect("filter predicate present");

        // (a) Pre-fix: the substrait roundtrip preserves the bound as a
        // non-literal scalar-function/cast expression, so no range is found.
        assert!(
            sort_range_from_predicate(&predicate, "logdate", None).is_none(),
            "unfolded to_timestamp bound must NOT yield a sort range pre-fold; \
             predicate was: {predicate:?}"
        );

        // (b) Post-fix: const-folding the constant bounds recovers the range.
        let folded = const_fold_comparison_bounds(predicate, plan.schema());
        let range = sort_range_from_predicate(&folded, "logdate", None)
            .expect("range must be recovered after const-fold");
        assert_eq!(range.lower, Some(lower_ns), "lower bound (ns) after fold");
        // `< upper` is exclusive → comparison_bound subtracts 1ns.
        assert_eq!(
            range.upper,
            Some(upper_ns - 1),
            "exclusive upper bound (ns) after fold"
        );
    }

    /// Reconstruct a Q4-shaped plan over a `Timestamp(Millisecond)` `logdate`
    /// column — the REAL live type per `CalciteToArrowSchema` (`date` →
    /// `Timestamp(ms)`) — carrying `to_timestamp('...')` (nanosecond) range
    /// bounds, exactly like PPL's `timestamp('...')` lowering. Byte-for-byte the
    /// executor's own consumer path (`from_substrait_plan`).
    fn roundtrip_ms_plan(sql: &str) -> LogicalPlan {
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::execution::context::SessionContext;
        use datafusion::execution::SessionStateBuilder;
        use datafusion_substrait::logical_plan::producer::to_substrait_plan;
        let state = SessionStateBuilder::new().with_default_features().build();
        let ctx = SessionContext::new_with_state(state);
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "logdate",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("app_component", DataType::Utf8, false),
        ]));
        ctx.register_batch("t", RecordBatch::new_empty(schema))
            .expect("register_batch");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let unoptimized = ctx.sql(sql).await.expect("sql").into_unoptimized_plan();
            let substrait =
                to_substrait_plan(&unoptimized, &ctx.state()).expect("to_substrait_plan");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            let decoded = Plan::decode(buf.as_slice()).expect("decode");
            from_substrait_plan(&ctx.state(), &decoded)
                .await
                .expect("from_substrait_plan")
        })
    }

    /// Disk-backed `logdate` fixture typed `Timestamp(Millisecond)` (matching an
    /// OpenSearch `date` column), two row groups of 4, values in millis inside
    /// 2026-02-08. Footer statistics therefore decode as `TimestampMillisecond`,
    /// exactly like the live Q4 parquet segments.
    fn write_ms_logdate_fixture() -> (tempfile::NamedTempFile, SchemaRef, i64, i64) {
        use datafusion::arrow::array::TimestampMillisecondArray;
        use datafusion::arrow::datatypes::TimeUnit;
        let day0: i64 = 1_770_508_800_000; // 2026-02-08T00:00:00Z in ms
        let vals: Vec<i64> = (0..8).map(|i| day0 + i * 3_600_000).collect(); // hourly
        let (lo, hi) = (vals[0], vals[7]);
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "logdate",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampMillisecondArray::from(vals)) as ArrayRef],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(4)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut w =
            ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        (tmp, schema, lo, hi)
    }

    fn ms_segment(path: &std::path::Path, schema: &SchemaRef) -> SegmentFileInfo {
        let size = std::fs::metadata(path).unwrap().len();
        let file = std::fs::File::open(path).unwrap();
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
                .unwrap();
        let parquet_meta = meta.metadata().clone();
        let mut rgs = Vec::new();
        let mut offset = 0i64;
        for i in 0..parquet_meta.num_row_groups() {
            let n = parquet_meta.row_group(i).num_rows();
            rgs.push(crate::indexed_table::stream::RowGroupInfo {
                index: i,
                first_row: offset,
                num_rows: n,
            });
            offset += n;
        }
        SegmentFileInfo {
            writer_generation: 0,
            max_doc: 8,
            object_path: ObjectPath::from(path.to_string_lossy().as_ref()),
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
            arrow_schema: schema.clone(),
            global_base: 0,
            sort_min: None,
            sort_max: None,
        }
    }

    /// Single-shard Q4 activation-gap regression: a `timestamp('...')` bound
    /// folds to a `Timestamp(Nanosecond)` literal, but `logdate`'s parquet footer
    /// is `Timestamp(Millisecond)`. WITHOUT unit normalization the ns range
    /// (~1.77e18) never contains the ms footer (~1.77e12), so `segment_within_rgs`
    /// is EMPTY — the exact reason `rg_topk_truncated` stayed 0 on the benchmark
    /// node even after const-fold was deployed. WITH normalization (the fix), the
    /// bound is recovered in ms and both row groups classify WITHIN.
    #[test]
    fn ns_folded_bound_matches_ms_footer_only_after_unit_normalization() {
        use datafusion::arrow::datatypes::TimeUnit;
        // Bounds straddle the whole fixture: 2026-02-07 .. 2026-02-09 (ns literals).
        let plan = roundtrip_ms_plan(
            "SELECT logdate FROM t \
             WHERE logdate >= to_timestamp('2026-02-07T00:00:00Z') \
               AND logdate < to_timestamp('2026-02-09T00:00:00Z') \
             ORDER BY logdate DESC LIMIT 100",
        );
        let folded = const_fold_comparison_bounds(
            extract_filter_expr(&plan).expect("filter predicate"),
            plan.schema(),
        );

        let (tmp, schema, lo_ms, hi_ms) = write_ms_logdate_fixture();
        let segment = ms_segment(tmp.path(), &schema);

        // (a) Pre-fix behavior — extract in the literal's native (ns) unit.
        let ns_range = sort_range_from_predicate(&folded, "logdate", None)
            .expect("a range IS derived (const-fold works) — but in nanoseconds");
        assert!(
            ns_range.lower.unwrap() > 1_000_000_000_000_000, // ~1e15+, i.e. ns magnitude
            "unnormalized bound is in nanoseconds: {ns_range:?}"
        );
        assert_eq!(
            segment_within_rgs(&ns_range, &segment),
            HashSet::new(),
            "ns bound vs ms footer → WITHIN set empty (this is the live Q4 gap: rg_topk_truncated=0)"
        );

        // (b) Post-fix behavior — normalize into the column's ms unit.
        let ms_range = sort_range_from_predicate(&folded, "logdate", Some(TimeUnit::Millisecond))
            .expect("range recovered in ms after normalization");
        assert!(
            ms_range.lower.unwrap() <= lo_ms && ms_range.upper.unwrap() >= hi_ms,
            "ms bound {ms_range:?} must contain the fixture footer [{lo_ms}, {hi_ms}]"
        );
        assert_eq!(
            segment_within_rgs(&ms_range, &segment),
            HashSet::from([0, 1]),
            "both row groups are footer-proven WITHIN once units match — truncation can now activate"
        );
    }

    #[test]
    fn compute_sort_topk_truncate_matches_q4_shape() {
        // Q4: single-key `sort - logdate | head 100` over catalog sort
        // [logdate desc, app_component asc]. Query DESC == catalog DESC → keep
        // the FIRST `budget` candidates of each WITHIN RG (keep_last = false).
        let top = TopSort {
            column: "logdate".to_string(),
            descending: true,
            fetch: Some(100),
            single_key: true,
        };
        let fields = vec!["logdate".to_string(), "app_component".to_string()];
        let orders = vec!["desc".to_string(), "asc".to_string()];
        assert_eq!(
            compute_sort_topk_truncate(true, Some(&top), &fields, &orders),
            Some((false, 100))
        );

        // No WITHIN row group → no truncation.
        assert_eq!(
            compute_sort_topk_truncate(false, Some(&top), &fields, &orders),
            None
        );

        // Multi-key sort (Q3 shape `sort - logdate, + app_component`) → fail closed.
        let multi = TopSort {
            single_key: false,
            ..top.clone()
        };
        assert_eq!(
            compute_sort_topk_truncate(true, Some(&multi), &fields, &orders),
            None
        );

        // Leading sort key is not the catalog leading field → no truncation.
        let other = TopSort {
            column: "app_component".to_string(),
            ..top.clone()
        };
        assert_eq!(
            compute_sort_topk_truncate(true, Some(&other), &fields, &orders),
            None
        );

        // Query ASC over catalog DESC storage → keep the LAST budget (keep_last = true).
        let asc = TopSort {
            descending: false,
            ..top.clone()
        };
        assert_eq!(
            compute_sort_topk_truncate(true, Some(&asc), &fields, &orders),
            Some((true, 100))
        );

        // Unbounded (no fetch) → no truncation.
        let unbounded = TopSort {
            fetch: None,
            ..top.clone()
        };
        assert_eq!(
            compute_sort_topk_truncate(true, Some(&unbounded), &fields, &orders),
            None
        );
    }

    // ── Q4 end-to-end: full PPL/substrait-style derivation → QTF top-K ────
    //
    // Regression for the single-shard Q4 gap. Drives the WHOLE derivation
    // chain that `execute_indexed_with_context_inner` runs on a real PPL plan,
    // WITHOUT hand-feeding `topk_range_within_rgs` or `sort_topk_truncate`:
    //
    //   raw unoptimized plan (function-wrapped ts bounds, à la PPL
    //   `timestamp('..')` lowering)
    //     → extract_filter_expr
    //     → const_fold_comparison_bounds         (the fix under test)
    //     → sort_range_from_predicate            → candidate_sort_range
    //     → segment_within_rgs (real footer)     → topk_range_within_rgs
    //     → analyze_top_sort + compute_sort_topk_truncate → (keep_last, budget)
    //
    // then feeds ONLY those DERIVED values into the indexed scan under QTF
    // (`emit_row_ids`) and asserts the observable counter + exact ordered row
    // IDs. Matches real Q4: single sort key `logdate DESC`, static
    // `timestamp()` bounds (fetch=3 stands in for 100), catalog index sort
    // `logdate DESC, app_component ASC`, QTF row-id emission.

    /// Disk-backed fixture in catalog sort order (`logdate DESC`): 16 rows,
    /// two row groups of 8. `logdate = 1000 - pos` (strictly descending,
    /// distinct); `__row_id__ = pos` so the position-derived QTF row ID equals
    /// storage position. RG0 = pos[0..8] (logdate 1000..993), RG1 = pos[8..16]
    /// (logdate 992..985).
    fn write_q4_fixture() -> (tempfile::NamedTempFile, SchemaRef) {
        let schema = q4_schema();
        let logdate: Vec<i64> = (0..16).map(|i| 1000 - i).collect();
        let app: Vec<String> = (0..16).map(|i| format!("svc-{}", i % 4)).collect();
        let row_id: Vec<i64> = (0..16).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(logdate)),
                Arc::new(datafusion::arrow::array::StringArray::from(app)),
                Arc::new(Int64Array::from(row_id)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut w =
            ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        (tmp, schema)
    }

    fn q4_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("logdate", DataType::Int64, false),
            Field::new("app_component", DataType::Utf8, false),
            Field::new(crate::ROW_ID_COLUMN_NAME, DataType::Int64, false),
        ]))
    }

    /// Full `SegmentFileInfo` over the on-disk Q4 fixture (row groups
    /// populated) — the same segment the executor builds, usable for BOTH
    /// `segment_within_rgs` derivation and the indexed scan.
    fn q4_segment(path: &std::path::Path, schema: &SchemaRef) -> SegmentFileInfo {
        let size = std::fs::metadata(path).unwrap().len();
        let file = std::fs::File::open(path).unwrap();
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
                .unwrap();
        let parquet_meta = meta.metadata().clone();
        let mut rgs = Vec::new();
        let mut offset = 0i64;
        for i in 0..parquet_meta.num_row_groups() {
            let n = parquet_meta.row_group(i).num_rows();
            rgs.push(crate::indexed_table::stream::RowGroupInfo {
                index: i,
                first_row: offset,
                num_rows: n,
            });
            offset += n;
        }
        SegmentFileInfo {
            writer_generation: 0,
            max_doc: 16,
            object_path: ObjectPath::from(path.to_string_lossy().as_ref()),
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
            arrow_schema: schema.clone(),
            global_base: 0,
            sort_min: None,
            sort_max: None,
        }
    }

    /// Build the RAW unoptimized logical plan for `sql` over the Q4 schema —
    /// mirrors `from_substrait_plan`'s unfolded output (bounds stay as
    /// scalar-function calls). Async so it composes inside the tokio test
    /// runtime (no nested `block_on`).
    async fn q4_unoptimized_plan(sql: &str) -> LogicalPlan {
        use datafusion::execution::context::SessionContext;
        use datafusion::execution::SessionStateBuilder;
        let state = SessionStateBuilder::new().with_default_features().build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_batch("t", RecordBatch::new_empty(q4_schema()))
            .unwrap();
        ctx.sql(sql).await.unwrap().into_unoptimized_plan()
    }

    fn sum_dev_counter(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
        use datafusion::physical_plan::metrics::{MetricType, MetricsSet};
        fn walk(plan: &Arc<dyn ExecutionPlan>, out: &mut MetricsSet) {
            if let Some(ms) = plan.metrics() {
                for m in ms.iter() {
                    out.push(m.clone());
                }
            }
            for child in plan.children() {
                walk(child, out);
            }
        }
        let mut set = MetricsSet::new();
        walk(plan, &mut set);
        set.sum(|m| m.value().name() == name && m.metric_type() == MetricType::Dev)
            .map(|v| v.as_usize())
            .unwrap_or(0)
    }

    /// Execute the QTF top-K scan with the DERIVED Top-K within-map (fed into
    /// `topk_range_within_rgs`, with the strict count set empty) + truncate.
    /// Returns the ordered emitted `__row_id__` values and the
    /// `rg_topk_truncated` counter.
    async fn run_q4_qtf(
        tmp_path: std::path::PathBuf,
        schema: SchemaRef,
        within: Option<Arc<HashMap<usize, HashSet<usize>>>>,
        truncate: Option<(bool, usize)>,
    ) -> (Vec<i64>, usize) {
        use datafusion::execution::context::SessionContext;
        use futures::StreamExt;

        let segment = q4_segment(&tmp_path, &schema);
        let factory: EvaluatorFactory = {
            let schema = schema.clone();
            Arc::new(
                move |segment: &SegmentFileInfo, _chunk, stream_metrics, _spt| {
                    let pruner = Arc::new(PagePruner::new(
                        &schema,
                        Arc::clone(&segment.metadata),
                        schema.clone(),
                    ));
                    let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(
                        crate::indexed_table::eval::predicate_evaluator::PredicateOnlyEvaluator::new(
                            pruner,
                            None,
                            None,
                            Some(PagePruneMetrics::from_stream_metrics(stream_metrics)),
                            None,
                            HashMap::new(),
                        ),
                    );
                    Ok(eval)
                },
            )
        };
        let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
            .target_partitions(1)
            .indexed_pushdown_filters(false)
            .build();
        let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
            schema: schema.clone(),
            segments: vec![segment],
            store: Arc::new(object_store::local::LocalFileSystem::new()),
            store_url: ObjectStoreUrl::local_filesystem(),
            evaluator_factory: factory,
            pushdown_predicate: None,
            query_config: std::sync::Arc::new(qc),
            predicate_columns: vec![],
            // QTF query phase: emit shard-global row IDs from position.
            emit_row_ids: true,
            prune_tree_config: None,
            // The catalog sort declaration is consumed by the DERIVATION above;
            // the single-segment scan needs no reversal (truncation already
            // resolved), so the execution harness mirrors the proven QTF e2e
            // pattern with empty catalog slices.
            sort_fields: vec![],
            sort_orders: vec![],
            // Faithful single-shard Q4: the strict count-shortcut set is EMPTY
            // (its `count_tree_shape_supported` gate fails on the real Q4
            // residual), and truncation is driven purely by the DEDICATED Top-K
            // WITHIN map. Proves truncation no longer depends on
            // `sort_range_within_rgs`.
            sort_range_within_rgs: None,
            topk_range_within_rgs: within,
            sort_topk_truncate: truncate,
            timestamp_within_rgs: None,
            pushdown_predicate_sans_sort_range: None,
            activation_diagnostics: Default::default(),
            cancellation_token: None,
        }));
        let ctx = SessionContext::new();
        ctx.register_table("t", provider).unwrap();
        // The range conjuncts are footer-proven tautologies on the WITHIN row
        // groups, so — exactly as the executor strips them — the scan SQL
        // carries only the sort+limit; the range lives in the derived
        // within-map.
        let df = ctx
            .sql("SELECT \"__row_id__\" FROM t ORDER BY logdate DESC LIMIT 3")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let mut stream =
            datafusion::physical_plan::execute_stream(Arc::clone(&plan), ctx.task_ctx()).unwrap();
        let mut ids = Vec::new();
        while let Some(batch) = stream.next().await {
            let b = batch.unwrap();
            let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..a.len() {
                ids.push(a.value(i));
            }
        }
        let truncated = sum_dev_counter(&plan, "rg_topk_truncated");
        (ids, truncated)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn q4_qtf_topk_derivation_end_to_end() {
        // Catalog index sort: logdate DESC, app_component ASC.
        let sort_fields = vec!["logdate".to_string(), "app_component".to_string()];
        let sort_orders = vec!["desc".to_string(), "asc".to_string()];

        // ── Stage 1: derive from the RAW unoptimized Q4 plan ──
        // Function-wrapped bounds (`abs(-N)` stands in for PPL `timestamp('..')`)
        // covering the whole corpus [985, 1000] so BOTH row groups are WITHIN.
        let plan = q4_unoptimized_plan(
            "SELECT \"__row_id__\" FROM t \
             WHERE logdate >= abs(-985) AND logdate <= abs(-1000) \
             ORDER BY logdate DESC LIMIT 3",
        )
        .await;

        // Load-bearing check: WITHOUT const-fold the function-wrapped bound is
        // not a literal, so no sort range is recognized — the original Q4 bug
        // that left `sort_range_within_rgs` empty and `rg_topk_truncated` at 0.
        let raw = extract_filter_expr(&plan).expect("filter predicate");
        assert!(
            sort_range_from_predicate(&raw, "logdate", None).is_none(),
            "unfolded function bound must hide the sort range (pre-fix behavior)"
        );

        // Executor derivation (mirrors execute_indexed_with_context_inner).
        let filter_expr = extract_filter_expr(&plan)
            .map(|expr| const_fold_comparison_bounds(expr, plan.schema()));
        let candidate_sort_range = sort_fields.first().and_then(|sort_field| {
            filter_expr
                .as_ref()
                .and_then(|expr| sort_range_from_predicate(expr, sort_field, None))
        });
        let range = candidate_sort_range.expect("sort range recovered after const-fold");
        assert_eq!(range.lower, Some(985));
        assert_eq!(range.upper, Some(1000));

        // Derive WITHIN row groups from the REAL parquet footer statistics.
        let (tmp, schema) = write_q4_fixture();
        let segment = q4_segment(tmp.path(), &schema);
        let within_set = segment_within_rgs(&range, &segment);
        assert_eq!(
            within_set,
            HashSet::from([0, 1]),
            "both row groups are footer-proven WITHIN [985, 1000]"
        );
        let within_map: Arc<HashMap<usize, HashSet<usize>>> =
            Arc::new(HashMap::from([(0usize, within_set)]));

        // Derive top-K truncation (single-key logdate DESC == catalog DESC →
        // keep the FIRST `budget` candidates of each WITHIN RG; budget=3).
        let top = analyze_top_sort(&plan);
        let truncate = compute_sort_topk_truncate(
            within_map.values().any(|s| !s.is_empty()),
            top.as_ref(),
            &sort_fields,
            &sort_orders,
        );
        assert_eq!(
            truncate,
            Some((false, 3)),
            "keep_last=false (query DESC over catalog DESC storage), budget=3"
        );

        // ── Stage 2: execute the QTF scan with ONLY the derived config ──
        let (ids, truncated) = run_q4_qtf(
            tmp.path().to_path_buf(),
            schema.clone(),
            Some(Arc::clone(&within_map)),
            truncate,
        )
        .await;
        // Top-3 by logdate DESC = storage positions 0,1,2 → row IDs 0,1,2,
        // returned in sort order (ordered + exact).
        assert_eq!(ids, vec![0, 1, 2], "exact ordered QTF row IDs");
        assert_eq!(
            truncated, 2,
            "both WITHIN row groups truncated to budget (observable counter > 0)"
        );

        // Equivalence: disabling truncation must yield identical row IDs.
        let (baseline, baseline_truncated) =
            run_q4_qtf(tmp.path().to_path_buf(), schema, Some(within_map), None).await;
        assert_eq!(baseline, ids, "row IDs identical with/without truncation");
        assert_eq!(baseline_truncated, 0, "no truncation when disabled");
    }

    fn segment_with_timestamps(values: Vec<Option<i64>>) -> SegmentFileInfo {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values.clone())) as ArrayRef],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_size(4)
            .set_data_page_row_count_limit(2)
            .set_write_batch_size(2)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, Arc::clone(&schema), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = bytes::Bytes::from(bytes);
        let metadata =
            ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::new().with_page_index(true))
                .unwrap()
                .metadata()
                .clone();
        let non_null: Vec<i64> = values.into_iter().flatten().collect();
        SegmentFileInfo {
            writer_generation: 1,
            max_doc: batch.num_rows() as i64,
            object_path: ObjectPath::from("test.parquet"),
            parquet_size: 0,
            row_groups: Vec::new(),
            metadata,
            arrow_schema: schema,
            global_base: 0,
            sort_min: non_null
                .iter()
                .min()
                .copied()
                .map(|v| ScalarValue::Int64(Some(v))),
            sort_max: non_null
                .iter()
                .max()
                .copied()
                .map(|v| ScalarValue::Int64(Some(v))),
        }
    }

    #[test]
    fn count_tree_shape_accepts_single_lucene_leaf_plus_range() {
        let ts_ge: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(PhysColumn::new("ts", 0)),
            Operator::GtEq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(5)))),
        ));
        let pod_eq: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(PhysColumn::new("pod", 1)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(3)))),
        ));
        // q3 shape: correctness collector + ts range.
        let q3 = BoolNode::And(vec![
            BoolNode::Collector { annotation_id: 1 },
            BoolNode::Predicate(Arc::clone(&ts_ge)),
        ]);
        assert!(count_tree_shape_supported(&q3, "ts"));
        // q7 shape: one delegation-possible equality + ts range.
        let q7 = BoolNode::And(vec![
            BoolNode::DelegationPossible {
                annotation_id: 2,
                original_expr: Arc::clone(&pod_eq),
            },
            BoolNode::Predicate(Arc::clone(&ts_ge)),
        ]);
        assert!(count_tree_shape_supported(&q7, "ts"));
        // Non-range plain predicate → reject.
        let bad = BoolNode::And(vec![
            BoolNode::Collector { annotation_id: 1 },
            BoolNode::Predicate(Arc::clone(&pod_eq)),
        ]);
        assert!(!count_tree_shape_supported(&bad, "ts"));
        // Two Lucene leaves → intersection needed → reject.
        let two = BoolNode::And(vec![
            BoolNode::Collector { annotation_id: 1 },
            BoolNode::DelegationPossible {
                annotation_id: 2,
                original_expr: Arc::clone(&pod_eq),
            },
            BoolNode::Predicate(Arc::clone(&ts_ge)),
        ]);
        assert!(!count_tree_shape_supported(&two, "ts"));
        // OR anywhere → reject.
        let or = BoolNode::And(vec![
            BoolNode::Or(vec![BoolNode::Collector { annotation_id: 1 }]),
            BoolNode::Predicate(Arc::clone(&ts_ge)),
        ]);
        assert!(!count_tree_shape_supported(&or, "ts"));
    }

    #[test]
    fn within_rgs_classified_from_footer_row_group_stats() {
        // 12 rows, RG size 4: RG0 = [0..3], RG1 = [4..7], RG2 = [8..11].
        let segment = segment_with_timestamps((0..12).map(Some).collect());
        let range = |lower: Option<i64>, upper: Option<i64>| SortRange {
            column: "ts".to_string(),
            lower,
            upper,
        };
        assert_eq!(
            segment_within_rgs(&range(Some(2), Some(9)), &segment),
            HashSet::from([1])
        );
        assert_eq!(
            segment_within_rgs(&range(Some(0), Some(11)), &segment),
            HashSet::from([0, 1, 2])
        );
        assert_eq!(
            segment_within_rgs(&range(None, Some(6)), &segment),
            HashSet::from([0])
        );
        assert_eq!(
            segment_within_rgs(&range(Some(20), Some(30)), &segment),
            HashSet::new()
        );
        // Column absent from the file → empty (fail closed).
        let missing = SortRange {
            column: "no_such_column".to_string(),
            lower: Some(0),
            upper: Some(11),
        };
        assert_eq!(segment_within_rgs(&missing, &segment), HashSet::new());
    }

    #[test]
    fn within_rgs_reject_null_bearing_row_groups() {
        let mut values: Vec<Option<i64>> = (0..12).map(Some).collect();
        values[5] = None; // RG1 carries a null
        let segment = segment_with_timestamps(values);
        let range = SortRange {
            column: "ts".to_string(),
            lower: Some(0),
            upper: Some(11),
        };
        assert_eq!(segment_within_rgs(&range, &segment), HashSet::from([0, 2]));
    }

    // ── Reason-level classifier diagnostics (`segment_within_rgs_with_reasons`) ──
    //
    // Each test asserts the WITHIN set is byte-for-byte identical to the plain
    // `segment_within_rgs` (semantics-neutral) AND that the reason tally
    // pinpoints exactly why each row group was (or was not) admitted. Reasons
    // reachable only through a `StatisticsConverter`/statistics failure
    // (`*_error`, `vector_length_mismatch`, `min/max_scalar_unsupported`,
    // `null_count_unavailable`) cannot be provoked by a healthy on-disk fixture,
    // so `parquet_column_index_missing` stands in for the segment-level
    // fail-closed family; a live capture is where those surface.

    #[test]
    fn within_reasons_healthy_all_within() {
        // Three RGs (0-3, 4-7, 8-11), range spans them all → every RG accepted,
        // no rejection reason fires.
        let segment = segment_with_timestamps((0..12).map(Some).collect());
        let range = SortRange {
            column: "ts".to_string(),
            lower: Some(0),
            upper: Some(11),
        };
        let mut reasons = WithinClassifierReasons::default();
        let within = segment_within_rgs_with_reasons(&range, &segment, &mut reasons);
        assert_eq!(within, segment_within_rgs(&range, &segment));
        assert_eq!(within, HashSet::from([0, 1, 2]));
        assert_eq!(reasons.within_accepted, 3);
        assert_eq!(reasons, {
            let mut r = WithinClassifierReasons::default();
            r.within_accepted = 3;
            r
        });
    }

    #[test]
    fn within_reasons_lower_bound_rejection() {
        // lower=5 excludes RG0 (max 3) on the lower bound; RG1 (4-7) also has
        // min 4 < 5 → both rejected on lower bound. RG2 (8-11) accepted.
        let segment = segment_with_timestamps((0..12).map(Some).collect());
        let range = SortRange {
            column: "ts".to_string(),
            lower: Some(5),
            upper: Some(11),
        };
        let mut reasons = WithinClassifierReasons::default();
        let within = segment_within_rgs_with_reasons(&range, &segment, &mut reasons);
        assert_eq!(within, segment_within_rgs(&range, &segment));
        assert_eq!(within, HashSet::from([2]));
        assert_eq!(reasons.lower_bound_rejection, 2);
        assert_eq!(reasons.within_accepted, 1);
        assert_eq!(reasons.upper_bound_rejection, 0);
    }

    #[test]
    fn within_reasons_upper_bound_rejection() {
        // upper=6 excludes RG1 (max 7) and RG2 (max 11) on the upper bound.
        let segment = segment_with_timestamps((0..12).map(Some).collect());
        let range = SortRange {
            column: "ts".to_string(),
            lower: Some(0),
            upper: Some(6),
        };
        let mut reasons = WithinClassifierReasons::default();
        let within = segment_within_rgs_with_reasons(&range, &segment, &mut reasons);
        assert_eq!(within, segment_within_rgs(&range, &segment));
        assert_eq!(within, HashSet::from([0]));
        assert_eq!(reasons.upper_bound_rejection, 2);
        assert_eq!(reasons.within_accepted, 1);
        assert_eq!(reasons.lower_bound_rejection, 0);
    }

    #[test]
    fn within_reasons_null_count_nonzero() {
        // RG1 carries a null → rejected as null_count_nonzero, not a bound
        // rejection. RG0 and RG2 accepted.
        let mut values: Vec<Option<i64>> = (0..12).map(Some).collect();
        values[5] = None;
        let segment = segment_with_timestamps(values);
        let range = SortRange {
            column: "ts".to_string(),
            lower: Some(0),
            upper: Some(11),
        };
        let mut reasons = WithinClassifierReasons::default();
        let within = segment_within_rgs_with_reasons(&range, &segment, &mut reasons);
        assert_eq!(within, segment_within_rgs(&range, &segment));
        assert_eq!(within, HashSet::from([0, 2]));
        assert_eq!(reasons.null_count_nonzero, 1);
        assert_eq!(reasons.within_accepted, 2);
        assert_eq!(reasons.lower_bound_rejection, 0);
        assert_eq!(reasons.upper_bound_rejection, 0);
    }

    #[test]
    fn within_reasons_converter_creation_failure_on_missing_column() {
        // A sort column absent from the file schema fails the segment-level
        // fail-closed exit exactly once (no per-RG counters), WITHIN empty.
        let segment = segment_with_timestamps((0..12).map(Some).collect());
        let missing = SortRange {
            column: "no_such_column".to_string(),
            lower: Some(0),
            upper: Some(11),
        };
        let mut reasons = WithinClassifierReasons::default();
        let within = segment_within_rgs_with_reasons(&missing, &segment, &mut reasons);
        assert_eq!(within, segment_within_rgs(&missing, &segment));
        assert!(within.is_empty());
        // Exactly one segment-level fail-closed exit fired; no per-RG counters.
        let segment_level =
            reasons.converter_creation_failure + reasons.parquet_column_index_missing;
        assert_eq!(
            segment_level, 1,
            "one segment-level fail-closed exit: {reasons:?}"
        );
        assert_eq!(reasons.within_accepted, 0);
        assert_eq!(reasons.lower_bound_rejection, 0);
        assert_eq!(reasons.upper_bound_rejection, 0);
        assert_eq!(reasons.null_count_nonzero, 0);
    }

    #[test]
    fn residual_sort_range_only_accepts_bounds_rejects_other_shapes() {
        let ts = || -> Arc<dyn PhysicalExpr> { Arc::new(PhysColumn::new("ts", 0)) };
        let lit = |v: i64| -> Arc<dyn PhysicalExpr> {
            Arc::new(Literal::new(ScalarValue::Int64(Some(v))))
        };
        let ge: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(ts(), Operator::GtEq, lit(5)));
        let lt: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(ts(), Operator::Lt, lit(10)));
        let both: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&ge),
            Operator::And,
            Arc::clone(&lt),
        ));
        assert!(physical_expr_is_sort_range_only(&ge, "ts"));
        assert!(physical_expr_is_sort_range_only(&both, "ts"));
        // Reversed operand order is also a plain bound.
        let rev: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(lit(5), Operator::Lt, ts()));
        assert!(physical_expr_is_sort_range_only(&rev, "ts"));
        // Wrong column name.
        assert!(!physical_expr_is_sort_range_only(&ge, "other"));
        // OR of bounds is not a conjunctive range.
        let or: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&ge),
            Operator::Or,
            Arc::clone(&lt),
        ));
        assert!(!physical_expr_is_sort_range_only(&or, "ts"));
        // Extra equality on another column poisons the conjunction.
        let other: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(PhysColumn::new("price", 1)),
            Operator::Eq,
            lit(3),
        ));
        let mixed: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(both, Operator::And, other));
        assert!(!physical_expr_is_sort_range_only(&mixed, "ts"));
    }

    // ── collect_plan_column_names ─────────────────────────────────────

    /// Regression: `SELECT *` (and any scan that reads columns no Projection
    /// expression names) must yield ALL output columns. The scoped page-index
    /// load gives only these columns a REAL multi-page OffsetIndex; columns left
    /// out get a single-page placeholder, which is fine for pruning but CORRUPTS
    /// a real read — arrow decodes the whole chunk as one page ("output too small
    /// for decompressed data"), or underflows the read byte range. The
    /// `match() | sort | head` shape (q23) hit this: it reads every column but no
    /// expression lists them. Collecting each plan node's OUTPUT SCHEMA fixes it.
    #[test]
    fn collect_plan_column_names_includes_select_star_columns() {
        // No projection expressions name id/ts/v, but `SELECT *` reads all three.
        let plan = build_logical_plan("SELECT * FROM t WHERE v = 0 ORDER BY ts");
        let mut names = collect_plan_column_names(&plan);
        names.sort();
        assert_eq!(
            names,
            vec!["id".to_string(), "ts".to_string(), "v".to_string()],
            "every read column (full output schema) must be collected, not just expression columns"
        );
    }

    /// A narrow projection still collects exactly the columns the query touches
    /// (projected `v` + filter `id` + sort `ts`) — the scoping benefit is retained.
    #[test]
    fn collect_plan_column_names_collects_projected_and_referenced() {
        let plan = build_logical_plan("SELECT v FROM t WHERE id = 1 ORDER BY ts");
        let names = collect_plan_column_names(&plan);
        for expected in ["v", "id", "ts"] {
            assert!(
                names.iter().any(|n| n == expected),
                "expected column `{expected}` in collected names {names:?}"
            );
        }
    }

    #[test]
    fn analyze_top_sort_returns_none_for_function_sort_key() {
        // `ORDER BY abs(v)` — leading sort key isn't a plain column. Catalog monotonicity
        // doesn't transfer through a function, so we conservatively decline to reverse.
        let plan = build_logical_plan("SELECT id FROM t ORDER BY abs(v)");
        assert!(analyze_top_sort(&plan).is_none());
    }

    #[test]
    fn should_reverse_segments_matches_leading_field_opposite_direction() {
        let top = TopSort {
            column: "id".to_string(),
            descending: true,
            fetch: None,
            single_key: true,
        };
        let fields = vec!["id".to_string()];
        let orders = vec!["asc".to_string()];
        assert!(should_reverse_segments(Some(&top), &fields, &orders));
    }

    #[test]
    fn should_reverse_segments_matches_leading_field_same_direction() {
        let top = TopSort {
            column: "id".to_string(),
            descending: false,
            fetch: None,
            single_key: true,
        };
        let fields = vec!["id".to_string()];
        let orders = vec!["asc".to_string()];
        assert!(!should_reverse_segments(Some(&top), &fields, &orders));
    }

    #[test]
    fn should_reverse_segments_catalog_desc_query_asc() {
        let top = TopSort {
            column: "id".to_string(),
            descending: false,
            fetch: None,
            single_key: true,
        };
        let fields = vec!["id".to_string()];
        let orders = vec!["desc".to_string()];
        assert!(should_reverse_segments(Some(&top), &fields, &orders));
    }

    #[test]
    fn should_reverse_segments_no_query_sort() {
        let fields = vec!["id".to_string()];
        let orders = vec!["asc".to_string()];
        assert!(!should_reverse_segments(None, &fields, &orders));
    }

    #[test]
    fn should_reverse_segments_no_catalog_sort() {
        let top = TopSort {
            column: "id".to_string(),
            descending: true,
            fetch: None,
            single_key: true,
        };
        assert!(!should_reverse_segments(Some(&top), &[], &[]));
    }

    #[test]
    fn should_reverse_segments_query_sort_on_non_leading_catalog_field() {
        // Catalog: [a ASC, b ASC]; query: ORDER BY b DESC. Segments are monotonic on `a`
        // (the leading key), not `b`. Reversing won't help — decline.
        let top = TopSort {
            column: "b".to_string(),
            descending: true,
            fetch: None,
            single_key: true,
        };
        let fields = vec!["a".to_string(), "b".to_string()];
        let orders = vec!["asc".to_string(), "asc".to_string()];
        assert!(!should_reverse_segments(Some(&top), &fields, &orders));
    }

    #[test]
    fn should_reverse_segments_field_name_case_sensitive() {
        // Match PR #22041 — `Column::from_name` is case-sensitive. If casing differs,
        // we don't claim the catalog ordering applies. Safe default: no reversal.
        let top = TopSort {
            column: "ID".to_string(),
            descending: true,
            fetch: None,
            single_key: true,
        };
        let fields = vec!["id".to_string()];
        let orders = vec!["asc".to_string()];
        assert!(!should_reverse_segments(Some(&top), &fields, &orders));
    }

    // ── reverse_segment_iteration_order ───────────────────────────────

    fn dummy_segment(max_doc: i64, global_base: u64) -> SegmentFileInfo {
        use datafusion::parquet::file::metadata::{FileMetaData, ParquetMetaData};
        // Build a minimal ParquetMetaData. We never read it back in these tests.
        let schema = std::sync::Arc::new(
            datafusion::parquet::schema::types::SchemaDescriptor::new(std::sync::Arc::new(
                datafusion::parquet::schema::types::Type::group_type_builder("schema")
                    .build()
                    .unwrap(),
            )),
        );
        let file_meta = FileMetaData::new(0, 0, None, None, schema, None);
        let pq_meta = ParquetMetaData::new(file_meta, vec![]);
        let metadata = std::sync::Arc::new(pq_meta);
        SegmentFileInfo {
            writer_generation: global_base as i64 + 1, // arbitrary, just to vary
            max_doc,
            object_path: object_store::path::Path::from(format!("seg-{}.parquet", global_base)),
            parquet_size: 0,
            row_groups: vec![],
            arrow_schema: std::sync::Arc::new(datafusion::arrow::datatypes::Schema::empty()),
            metadata,
            global_base,
            sort_min: None,
            sort_max: None,
        }
    }

    #[test]
    fn reverse_segments_preserves_global_base() {
        // Original: A(max_doc=10, base=0), B(max_doc=20, base=10), C(max_doc=30, base=30).
        // Reversal must keep each segment's original `global_base` intact so QTF row IDs
        // emitted in query phase remain interpretable by the fetch phase (which always
        // computes catalog-order bases).
        let mut segs = vec![
            dummy_segment(10, 0),
            dummy_segment(20, 10),
            dummy_segment(30, 30),
        ];
        reverse_segment_iteration_order(&mut segs);
        assert_eq!(segs.len(), 3);
        // New iteration order: C, B, A.
        assert_eq!(segs[0].max_doc, 30);
        assert_eq!(segs[0].global_base, 30); // C's catalog base, unchanged.
        assert_eq!(segs[1].max_doc, 20);
        assert_eq!(segs[1].global_base, 10); // B's catalog base, unchanged.
        assert_eq!(segs[2].max_doc, 10);
        assert_eq!(segs[2].global_base, 0); // A's catalog base, unchanged.
    }

    #[test]
    fn reverse_segments_empty_is_noop() {
        let mut segs: Vec<SegmentFileInfo> = vec![];
        reverse_segment_iteration_order(&mut segs);
        assert!(segs.is_empty());
    }

    #[test]
    fn reverse_segments_single_keeps_its_base() {
        let mut segs = vec![dummy_segment(42, 7)];
        reverse_segment_iteration_order(&mut segs);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].global_base, 7);
        assert_eq!(segs[0].max_doc, 42);
    }
}

/// Instruction-based indexed execution path. Consumes a pre-configured SessionContextHandle
/// (with UDF registered and IndexedExecutionConfig set) and routes to the appropriate
/// evaluator based on the Java-provided FilterTreeShape.
///
/// TODO: extract shared logic with `execute_indexed_query` to avoid duplication.
/// For now this delegates to the existing function by reconstructing the needed args
/// from the handle.
pub async unsafe fn execute_indexed_with_context(
    session_ctx_ptr: i64,
    substrait_bytes: Vec<u8>,
    cpu_executor: DedicatedExecutor,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<i64, DataFusionError> {
    let handle =
        *Box::from_raw(session_ctx_ptr as *mut crate::session_context::SessionContextHandle);
    let context_id = handle.query_context.context_id();
    let token = crate::query_tracker::get_cancellation_token(context_id);

    let query_future =
        execute_indexed_with_context_inner(handle, substrait_bytes, cpu_executor, permit);
    crate::cancellation::cancellable(token.as_ref(), context_id, query_future)
        .await
        .map_err(DataFusionError::Execution)
}

async unsafe fn execute_indexed_with_context_inner(
    handle: crate::session_context::SessionContextHandle,
    substrait_bytes: Vec<u8>,
    cpu_executor: DedicatedExecutor,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<i64, DataFusionError> {
    // Permit was acquired by the caller (ffm.rs) on the IO runtime before
    // spawning on the CPU runtime, so the Java search thread blocks at the
    // gate when it is full — creating backpressure at the Java threadpool level.

    // Empty shard: skip build_segments (errors on zero files) and emit an
    // empty stream. Mirrors the guard in query_executor::execute_with_context — including the
    // non-empty table_name gate, so hash-shuffle WORKER sessions (empty object_metas but scanning
    // registered StreamingTables, with an empty table_name) are NOT short-circuited to zero rows.
    if handle.object_metas.is_empty() && !handle.table_name.is_empty() {
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::ExecutionPlan;
        let context_id_early = handle.query_context.context_id();
        // engine-native-merge: borrow the partial-state schema from the prepared plan so the
        // empty stream matches the populated shards' wire shape (e.g. Binary HLL for dc()).
        let plan_schema: arrow::datatypes::SchemaRef =
            if let Some(prepared) = handle.prepared_plan.as_ref() {
                Arc::new(prepared.schema().as_ref().clone())
            } else {
                let plan = Plan::decode(substrait_bytes.as_slice())
                    .map_err(|e| DataFusionError::Execution(format!("decode substrait: {}", e)))?;
                let logical_plan = from_substrait_plan(&handle.ctx.state(), &plan).await?;
                Arc::new(logical_plan.schema().as_arrow().clone())
            };
        let plan_schema = crate::schema_coerce::coerce_inferred_schema(plan_schema);
        let empty_exec = EmptyExec::new(Arc::clone(&plan_schema));
        let df_stream = empty_exec.execute(0, handle.ctx.task_ctx())?;
        let (cross_rt_stream, abort_handle, _task_done) =
            CrossRtStream::new_with_df_error_stream_cancellable(
                df_stream,
                cpu_executor.clone(),
                None,
            );
        if let Some(h) = abort_handle {
            crate::query_tracker::set_abort_handle(context_id_early, h);
        }
        if let Some(rt) = cpu_executor.handle() {
            crate::query_tracker::set_cpu_runtime_handle(context_id_early, rt);
        }
        let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            cross_rt_stream.schema(),
            cross_rt_stream,
        );
        let stream_handle = crate::api::QueryStreamHandle::with_session_context(
            wrapped,
            handle.query_context,
            handle.ctx,
            Some(permit),
        );
        return Ok(Box::into_raw(Box::new(stream_handle)) as i64);
    }

    // Java-side QTF signal: scan must emit __row_id__. Captured before consuming indexed_config below.
    let requests_row_ids = handle
        .indexed_config
        .as_ref()
        .is_some_and(|c| c.requests_row_ids);
    let classification_override = handle.indexed_config.map(|config| {
        // FilterTreeShape: 1 = CONJUNCTIVE → SingleCollector, 2 = INTERLEAVED → Tree.
        match (config.tree_shape, config.delegated_predicate_count) {
            (1, _) => FilterClass::SingleCollector,
            (2, _) => FilterClass::Tree,
            _ => FilterClass::None,
        }
    });

    let query_config = Arc::new(handle.query_config);
    let num_partitions = query_config.target_partitions.max(1);
    let aggregate_mode = handle.aggregate_mode;
    let ctx = handle.ctx;
    let table_name = handle.table_name;
    let table_path = handle.table_path;
    let object_metas = handle.object_metas;
    let writer_generations = handle.writer_generations;
    let sort_fields = handle.sort_fields;
    let sort_orders = handle.sort_orders;
    let query_context = handle.query_context;
    let io_handle = handle.io_handle;
    // Extract context_id early so it can be captured by the per-segment closures
    // below. The closures pass it through every FFM upcall so Java can route each
    // callback to the correct per-query FilterDelegationHandle and DelegationThreadTracker.
    let context_id = query_context.context_id();

    // The substrait scan binds to the plan's NamedTable name (the alias/pattern like "tb-1,tb-2"
    // for multi-index queries, the concrete name otherwise), not the per-shard table_name.
    // create_session_context registered the provider under that name, so re-register under the
    // same name. Using table_name for a multi-index query leaves the scan unbound, so DataFusion
    // never consults supports_filters_pushdown and keeps the delegated_predicate FilterExec —
    // which then executes the marker UDF and errors.
    let register_name = crate::api::first_named_table_name(substrait_bytes.as_slice())
        .unwrap_or_else(|| table_name.clone());

    // SessionContext already has RuntimeEnv, caches, memory pool, UDF from create_session_context_indexed.
    // Deregister the default ListingTable (registered by create_session_context) — will be replaced
    // with IndexedTableProvider after plan decoding.
    ctx.deregister_table(&register_name)?;

    let store = ctx.state().runtime_env().object_store(&table_path)?;

    let state = ctx.state();
    let metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();

    let (mut segments, schema) = build_segments(
        &state,
        Arc::clone(&store),
        object_metas.as_ref(),
        writer_generations.as_ref(),
        metadata_cache,
        &sort_fields,
    )
    .await
    .map_err(DataFusionError::Execution)?;
    let schema = crate::schema_coerce::coerce_inferred_schema(schema);
    // Widen to the plan's base_schema so columns absent from this shard's parquet (cross-shard drift) are null-filled at read time.
    let schema = crate::session_context::widen_schema_from_plan(
        &ctx,
        &substrait_bytes,
        &register_name,
        &schema,
    );

    let placeholder: Arc<dyn TableProvider> = Arc::new(PlaceholderProvider {
        schema: schema.clone(),
    });
    ctx.register_table(&register_name, placeholder)?;

    let plan = Plan::decode(substrait_bytes.as_slice())
        .map_err(|e| DataFusionError::Execution(format!("decode substrait: {}", e)))?;
    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;

    // Shard filter predicate, with constant range/equality bounds folded to
    // literals. PPL lowers timestamp bounds as `timestamp('...')` scalar-function
    // calls; the raw (unoptimized) substrait plan leaves them unfolded, which
    // hides the sort-range from `sort_range_from_predicate` /
    // `physical_expr_is_sort_range_only` (both require a literal bound) and thus
    // silently disables the WITHIN / count-shortcut / top-K-truncation fast paths
    // for real PPL plans. Folding here re-enables them.
    let raw_filter_expr = extract_filter_expr(&logical_plan);
    let filter_expr =
        raw_filter_expr.map(|expr| const_fold_comparison_bounds(expr, logical_plan.schema()));

    // Candidate timestamp fast-path range: top-level conjunctive bounds on the
    // leading `index.sort.field`, read from the const-folded predicate. Whether
    // it activates anything is decided later per ROW GROUP from footer statistics
    // (`segment_within_rgs`) — the logical plan is never rewritten;
    // boundary/unknown row groups keep the full residual + page-pruning path.
    //
    // The bound is normalized into the sort column's arrow `TimeUnit` (the same
    // unit the footer statistics decode to) so a folded literal is compared
    // like-for-like against the sort column's footer — otherwise the WITHIN set
    // is silently empty and the top-K truncation / count-shortcut fast paths
    // never fire.
    let sort_column_unit = sort_fields.first().and_then(|sort_field| {
        schema
            .field_with_name(sort_field)
            .ok()
            .and_then(|field| match field.data_type() {
                DataType::Timestamp(unit, _) => Some(*unit),
                _ => None,
            })
    });
    let candidate_sort_range = sort_fields.first().and_then(|sort_field| {
        filter_expr
            .as_ref()
            .and_then(|expr| sort_range_from_predicate(expr, sort_field, sort_column_unit))
    });

    // ── Activation-chain diagnostics (captured for profile metrics only) ──
    // These locals mirror the exact planning-time conditions gating the
    // per-row-group top-K truncation / count-shortcut fast paths. They are
    // stashed on `IndexedTableConfig.activation_diagnostics` and surfaced as
    // `activation_*` metrics in `IndexedTableProvider::scan`. Capturing them
    // here (rather than recomputing in `scan`) keeps them faithful to the exact
    // values the fast-path logic saw. Never influences execution.
    let diag_filter_expr_present = filter_expr.is_some();
    let diag_sort_column_unit_code: u8 = match sort_column_unit {
        None => 0,
        Some(TimeUnit::Second) => 1,
        Some(TimeUnit::Millisecond) => 2,
        Some(TimeUnit::Microsecond) => 3,
        Some(TimeUnit::Nanosecond) => 4,
    };
    let diag_candidate_range_detected = candidate_sort_range.is_some();
    let diag_candidate_lower = candidate_sort_range.as_ref().and_then(|r| r.lower);
    let diag_candidate_upper = candidate_sort_range.as_ref().and_then(|r| r.upper);
    // Computed once and reused for both the segment-reverse decision and the
    // top-K truncation decision (was previously called twice).
    let top_sort = analyze_top_sort(&logical_plan);
    let diag_top_sort_present = top_sort.is_some();
    let diag_top_sort_single_key = top_sort.as_ref().is_some_and(|t| t.single_key);
    let diag_top_sort_fetch = top_sort.as_ref().and_then(|t| t.fetch);
    let diag_top_sort_key_matches_catalog = top_sort
        .as_ref()
        .zip(sort_fields.first())
        .is_some_and(|(t, catalog)| t.column == *catalog);

    // STRICT top-K truncation eligibility (SEPARATE from `top_sort` above).
    // `analyze_top_sort` is intentionally permissive — it returns at the top
    // Sort and drives only the read-only segment-iteration reversal. Scan-level
    // per-RG candidate truncation, however, is unsafe if a cardinality-changing
    // or key-remapping operator (Window/ROW_NUMBER, Aggregate, Join, Distinct,
    // set op, Unnest, inner Sort, …) sits BETWEEN the bounded Sort and the
    // indexed scan: it would consume rows the scan dropped. `7225d84e` armed
    // truncation below Q5's `ROW_NUMBER`/dedup window for exactly this reason.
    // This fail-closed walk verifies the whole Sort→TableScan path is safe and
    // is the ONLY input to the truncation gate below.
    let topk_trunc_top_sort = analyze_scan_topk_truncation_path(&logical_plan);
    let diag_topk_truncation_path_safe = topk_trunc_top_sort.is_some();

    // Sort-aware segment iteration. Mirror of `ContextIndexSearcher.shouldUseTimeSeriesDescSortOptimization`
    // for the indexed-parquet path. When the index has `index.sort.field` and the query's leading
    // ORDER BY runs counter to the catalog's stored direction, reverse the segment vector so a
    // TopK above us pulls the highest-priority segment first and parquet page stats prune the rest.
    //
    // QTF safe: `reverse_segment_iteration_order` deliberately does NOT recompute `global_base`
    // — each segment retains its catalog-order base, so the row IDs query phase emits are still
    // interpretable by `api::fetch_by_row_ids` (which builds its own segments from
    // `ShardView.object_metas` in catalog order).
    if should_reverse_segments(top_sort.as_ref(), &sort_fields, &sort_orders) {
        log_debug!(
            "indexed_executor: reversing segment iteration (catalog leading sort={:?} {:?}, query opposite)",
            sort_fields.first(),
            sort_orders.first()
        );
        reverse_segment_iteration_order(&mut segments);
    }

    let emit_row_ids = requests_row_ids;
    let extraction = match filter_expr {
        None => None,
        Some(ref expr) => Some(
            expr_to_bool_tree(expr, &schema, &state)
                .map_err(|e| DataFusionError::Execution(format!("expr_to_bool_tree: {}", e)))?,
        ),
    };

    // Resolve classification: from Java config if available, otherwise derive from tree
    let classification = match classification_override {
        Some(c) => c,
        None => match &extraction {
            None => FilterClass::None,
            Some(e) => classify_filter(&e.tree),
        },
    };
    // Derive the parquet pushdown predicate from the BoolNode tree.
    // `scan()` ignores DataFusion's filters argument (which contains
    // the `delegated_predicate` UDF marker whose body panics) and uses this
    // field instead.
    //
    // SingleCollector: residual (non-Collector top-AND children) →
    //   PhysicalExpr for `ParquetSource::with_predicate`. In
    //   row-granular mode parquet narrows Collector-matching rows via
    //   RowSelection and drops residual-failing rows via pushdown.
    //   In block-granular mode the evaluator's `on_batch_mask` applies
    //   both mask and residual post-decode, and pushdown is suppressed
    //   by the stream's `will_build_mask` guard (to avoid misalignment).
    // Tree: None — BitmapTreeEvaluator walks the whole BoolNode in
    //   `on_batch_mask` using arrow kernels; no pushdown needed.
    let pushdown_predicate: Option<Arc<dyn PhysicalExpr>> = match &classification {
        FilterClass::SingleCollector => extraction.as_ref().and_then(|e| {
            let residual_bool = extract_single_collector_residual(&e.tree);
            residual_bool
                .as_ref()
                .and_then(residual_bool_to_physical_expr)
        }),
        FilterClass::None => {
            // Predicate-only: push the whole tree (may be an unfoldable constant);
            // None = no filter = full scan.
            extraction
                .as_ref()
                .and_then(|e| residual_bool_to_physical_expr(&e.tree))
        }
        FilterClass::Tree => None,
    };

    let leaf_exprs = collect_leaf_exprs(extraction.as_ref());

    let predicate_columns = collect_predicate_column_indices_from_exprs(&leaf_exprs);

    // Activation diagnostic (profile-only): does the BoolNode tree shape support
    // the sort-range count/tautology fast path? Mirrors the `shape_ok` gate in
    // the `sort_range_within_rgs` binding below, but is captured unconditionally
    // (even when no WITHIN row group exists) so the profile can distinguish
    // "shape unsupported" from "shape supported but no RG footer was WITHIN".
    let diag_count_tree_shape_supported = candidate_sort_range
        .as_ref()
        .map(|range| match &classification {
            FilterClass::SingleCollector => extraction
                .as_ref()
                .is_some_and(|e| count_tree_shape_supported(&e.tree, &range.column)),
            FilterClass::None => extraction
                .as_ref()
                .and_then(|e| residual_bool_to_physical_expr(&e.tree))
                .is_some_and(|residual| physical_expr_is_sort_range_only(&residual, &range.column)),
            FilterClass::Tree => false,
        })
        .unwrap_or(false);

    // Timestamp WITHIN fast path (row-group granularity, footer statistics
    // only). When the non-collector residual consists solely of range
    // comparisons on the leading sort field, a row group whose footer min/max
    // lie fully inside the query bounds with zero nulls satisfies that
    // residual for every row. For count-only shapes the scan then emits the
    // candidate cardinality for such row groups without any parquet decode.
    // Tree-class filters and any unsupported residual shape fail closed.
    // Accumulates the reason-level tally for the strict WITHIN classifier pass
    // (below). Surfaced as `activation_within_reason_*` metrics so a
    // `profile:true` run can explain an empty WITHIN set. Diagnostics-only.
    let mut within_reasons = WithinClassifierReasons::default();
    let sort_range_within_rgs: Option<Arc<HashMap<usize, HashSet<usize>>>> =
        candidate_sort_range.as_ref().and_then(|range| {
            let shape_ok = match &classification {
                FilterClass::SingleCollector => extraction
                    .as_ref()
                    .is_some_and(|e| count_tree_shape_supported(&e.tree, &range.column)),
                FilterClass::None => extraction
                    .as_ref()
                    .and_then(|e| residual_bool_to_physical_expr(&e.tree))
                    .is_some_and(|residual| {
                        physical_expr_is_sort_range_only(&residual, &range.column)
                    }),
                FilterClass::Tree => false,
            };
            if !shape_ok {
                return None;
            }
            let map: HashMap<usize, HashSet<usize>> = segments
                .iter()
                .enumerate()
                .map(|(idx, segment)| {
                    (
                        idx,
                        segment_within_rgs_with_reasons(range, segment, &mut within_reasons),
                    )
                })
                .collect();
            map.values()
                .any(|set| !set.is_empty())
                .then(|| Arc::new(map))
        });
    if let Some(map) = sort_range_within_rgs.as_ref() {
        let total: usize = map.values().map(|set| set.len()).sum();
        log_debug!(
            "indexed_executor: sort-range residual is a tautology for {} row group(s); count-only shapes skip their parquet decode",
            total
        );
    }

    // ── Relaxed timestamp WITHIN row groups (projection/predicate pruning) ──
    //
    // A superset of the strict `sort_range_within_rgs` set above. The strict
    // set only activates when the ENTIRE residual is a sort-range tautology
    // (so a WITHIN RG can skip decode for count shapes). This relaxed set
    // activates whenever the residual carries AT LEAST ONE strippable
    // sort-range conjunct on the leading sort field, even alongside other
    // (non-sort-range) conjuncts. For a WITHIN RG the sort-range conjuncts are
    // footer-proven tautologies, so we drop the sort column from that RG's
    // parquet projection AND from the pushdown predicate — the remaining
    // residual (`pushdown_predicate_sans_sort_range`) still filters. The sort
    // column is only dropped when it is NOT part of the query's output
    // projection (handled downstream by `output_projection ∪ required cols`).
    //
    // Scope: `FilterClass::SingleCollector` only. `None` (predicate-only) and
    // `Tree` fail closed — the strict count shortcut already covers pure
    // timestamp-range count shapes, and PredicateOnly/Tree do not carry the
    // per-RG `required_predicate_columns` machinery this relaxation rides on.
    let (timestamp_within_rgs, pushdown_predicate_sans_sort_range): (
        Option<Arc<HashMap<usize, HashSet<usize>>>>,
        Option<Arc<dyn PhysicalExpr>>,
    ) = candidate_sort_range
        .as_ref()
        .and_then(|range| {
            let residual = match &classification {
                FilterClass::SingleCollector => extraction
                    .as_ref()
                    .and_then(|e| extract_single_collector_residual(&e.tree))
                    .as_ref()
                    .and_then(residual_bool_to_physical_expr),
                FilterClass::None | FilterClass::Tree => None,
            }?;
            let (remaining, stripped_any) = strip_sort_range_conjuncts(&residual, &range.column);
            if !stripped_any {
                // Nothing sort-range to strip (or an unstrippable shape) →
                // disable the relaxed path; the full predicate/projection apply.
                return None;
            }
            let map: HashMap<usize, HashSet<usize>> = segments
                .iter()
                .enumerate()
                .map(|(idx, segment)| (idx, segment_within_rgs(range, segment)))
                .collect();
            if !map.values().any(|set| !set.is_empty()) {
                return None;
            }
            Some((Arc::new(map), remaining))
        })
        .map_or((None, None), |(map, remaining)| (Some(map), remaining));
    if let Some(map) = timestamp_within_rgs.as_ref() {
        let total: usize = map.values().map(|set| set.len()).sum();
        log_debug!(
            "indexed_executor: relaxed timestamp-WITHIN active for {} row group(s); sort column dropped from their projection + pushdown (residual_sans_sort_range present: {})",
            total,
            pushdown_predicate_sans_sort_range.is_some()
        );
    }

    // ── Dedicated Top-K WITHIN classification ──────────────────────────────
    //
    // Per-RG Top-K candidate truncation (`sort <key> | head N`) needs a WITHIN
    // set that is INDEPENDENT of the count-shortcut classifier. The strict
    // `sort_range_within_rgs` set above only populates when the ENTIRE residual
    // is a sort-range tautology (`count_tree_shape_supported`) — the count
    // shortcut's precondition. Single-shard Q4 (`match(...) AND ts-range | sort
    // ts | head N`) fails that gate (the residual also carries the match
    // Collector), so the strict set stayed empty and truncation NEVER armed
    // even though its own preconditions — a single-key bounded top-K on the
    // leading catalog sort field and footer-WITHIN row groups — all held. The
    // previous code armed truncation on `sort_range_within_rgs.is_some()`, an
    // incorrect dependency; that is the bug this pass fixes.
    //
    // This classification is gated ONLY by the top-K shape (exactly
    // `compute_sort_topk_truncate`'s gate: single key, bounded fetch, key ==
    // leading catalog sort field) plus footer WITHIN classification
    // (`segment_within_rgs`). It does NOT depend on `count_tree_shape_supported`,
    // `FilterClass`, residual stripping, or projection optimization. Boundary
    // (non-WITHIN) row groups keep full candidates downstream. Reason-level
    // tallies surface as `activation_topk_within_reason_*` metrics.
    let mut topk_within_reasons = WithinClassifierReasons::default();
    // Uses the STRICT `topk_trunc_top_sort` (fail-closed Sort→scan path guard),
    // NOT the permissive `top_sort`. If any barrier (Window/ROW_NUMBER, dedup,
    // Aggregate, Join, …) sits between the bounded Sort and the indexed scan,
    // `analyze_scan_topk_truncation_path` returned `None`, so this is `false`
    // and the WITHIN Top-K classification below never runs.
    let topk_shape_eligible = compute_sort_topk_truncate(
        true,
        topk_trunc_top_sort.as_ref(),
        &sort_fields,
        &sort_orders,
    )
    .is_some();
    let topk_range_within_rgs: Option<Arc<HashMap<usize, HashSet<usize>>>> = candidate_sort_range
        .as_ref()
        .filter(|_| topk_shape_eligible)
        .and_then(|range| {
            let map: HashMap<usize, HashSet<usize>> = segments
                .iter()
                .enumerate()
                .map(|(idx, segment)| {
                    (
                        idx,
                        segment_within_rgs_with_reasons(range, segment, &mut topk_within_reasons),
                    )
                })
                .collect();
            map.values()
                .any(|set| !set.is_empty())
                .then(|| Arc::new(map))
        });
    if let Some(map) = topk_range_within_rgs.as_ref() {
        let total: usize = map.values().map(|set| set.len()).sum();
        log_debug!(
            "indexed_executor: dedicated top-K WITHIN classification active for {} row group(s) (independent of the count-shortcut set)",
            total
        );
    }

    // Top-K candidate truncation for rows shapes (`sort <key> | head N`).
    // Rows within each row group are stored in catalog sort order, so for a
    // single-key top-K on the leading sort field only the first/last N
    // candidates of a Top-K-WITHIN row group can reach the global top-N.
    // Boundary (non-WITHIN) row groups keep full candidates. `keep_last` is
    // relative to storage order. Gated on the DEDICATED `topk_range_within_rgs`
    // set, NOT the strict count-shortcut `sort_range_within_rgs` set. Uses the
    // STRICT `topk_trunc_top_sort` so a barrier between the Sort and the scan
    // keeps truncation off (the `topk_range_within_rgs` gate above already
    // fails closed via the same strict result, so this is doubly guarded).
    let sort_topk_truncate: Option<(bool, usize)> = compute_sort_topk_truncate(
        topk_range_within_rgs.is_some(),
        topk_trunc_top_sort.as_ref(),
        &sort_fields,
        &sort_orders,
    );
    if let Some((keep_last, budget)) = sort_topk_truncate {
        log_debug!(
            "indexed_executor: top-K truncation active (keep_last={}, budget={}) for top-K WITHIN row groups",
            keep_last,
            budget
        );
    }

    // Augment each segment's footer-only metadata with a scoped page index so
    // the indexed PagePruner can page-prune. Both predicate (→ ColumnIndex) and
    // projection (→ OffsetIndex) are wired — a match()-only query still needs a
    // scoped OffsetIndex so the reader fetches only matched pages.
    if page_index::is_scoped_page_index_enabled() {
        let predicate_column_names = collect_predicate_column_names(extraction.as_ref(), &schema);
        let projection_column_names = collect_plan_column_names(&logical_plan);
        if !predicate_column_names.is_empty() || !projection_column_names.is_empty() {
            for segment in segments.iter_mut() {
                let (parquet_cols, offset_cols) = resolve_predicate_parquet_columns_pair(
                    &schema,
                    &segment.metadata,
                    &predicate_column_names,
                    &projection_column_names,
                    &segment.arrow_schema,
                );
                if parquet_cols.is_empty() && offset_cols.is_empty() {
                    continue;
                }
                if let Some(augmented) = load_scoped_page_index_cols(
                    &store,
                    &segment.object_path,
                    &segment.metadata,
                    &parquet_cols,
                    &offset_cols,
                )
                .await
                {
                    segment.metadata = augmented;
                }
            }
        }
    }

    let (factory, prune_tree_config): (EvaluatorFactory, _) = match classification {
        FilterClass::None => {
            // Predicate-only scan: page-pruned universe, residual applied in
            // on_batch_mask. Also covers an unfoldable constant (e.g. mktime('...') >
            // N) — no index column, but every row scanned and the constant applied as
            // residual (pushdown is Exact, so DataFusion drops the FilterExec).
            // Previously errored here when emit_row_ids was false (indexed path only).
            let schema_for_pruner = schema.clone();
            let prune_tree_config = extraction
                .as_ref()
                .and_then(|e| build_prune_tree_config(&e.tree, &schema_for_pruner, &leaf_exprs));
            let residual_expr: Option<Arc<dyn PhysicalExpr>> = extraction
                .as_ref()
                .and_then(|e| residual_bool_to_physical_expr(&e.tree));
            let residual_pruning_predicate: Option<Arc<PruningPredicate>> = residual_expr
                .as_ref()
                .and_then(|expr| build_pruning_predicate(expr, Arc::clone(&schema_for_pruner)));

            (
                Arc::new(
                    move |segment: &SegmentFileInfo,
                          chunk,
                          stream_metrics: &StreamMetrics,
                          stats_prune_tree: Option<&Arc<StatsPruneTree>>| {
                        let pruner = Arc::new(PagePruner::new(
                            &schema_for_pruner,
                            Arc::clone(&segment.metadata),
                            segment.arrow_schema.clone(),
                        ));
                        let rg_index_to_pos: HashMap<usize, usize> = chunk
                            .row_group_indices
                            .iter()
                            .enumerate()
                            .map(|(pos, &idx)| (idx, pos))
                            .collect();
                        let eval: Arc<dyn RowGroupBitsetSource> =
                        Arc::new(crate::indexed_table::eval::predicate_evaluator::PredicateOnlyEvaluator::new(
                            pruner,
                            residual_pruning_predicate.clone(),
                            residual_expr.clone(),
                            Some(PagePruneMetrics::from_stream_metrics(stream_metrics)),
                            stats_prune_tree.cloned(),
                            rg_index_to_pos,
                        ));
                        Ok(eval)
                    },
                ),
                prune_tree_config,
            )
        }
        FilterClass::SingleCollector => {
            let extraction = extraction.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "classify_filter returned SingleCollector but extraction is None".into(),
                )
            })?;
            let schema_for_pruner = schema.clone();
            let prune_tree_config =
                build_prune_tree_config(&extraction.tree, &schema_for_pruner, &leaf_exprs);

            // Correctness-delegated provider (eager). `None` when the query has only
            // performance-delegated leaves and no Collector at all.
            let correctness_provider: Option<Arc<ProviderHandle>> =
                match single_collector_id(&extraction.tree) {
                    Some(annotation_id) => Some(Arc::new(
                        create_provider(context_id, annotation_id)
                            .map_err(|e| DataFusionError::External(e.into()))?,
                    )),
                    None => None,
                };

            // ── Lucene conjunction folding ───────────────────────────────────
            //
            // When a correctness collector exists (e.g. `match(message,..)`),
            // its per-RG Lucene call is MANDATORY — so evaluating each
            // dual-viable (DelegationPossible) leaf as a separate provider
            // costs a second FFM bitmap per RG AND forgoes Lucene's
            // conjunction scorer. Fold every dual-viable leaf into the
            // correctness provider as a Lucene `BooleanQuery` FILTER clause
            // (`createAndProvider` upcall): ONE FFM call per RG, and Lucene
            // leapfrogs the sparser iterator (e.g. a pod term) through the
            // denser one (e.g. a high-frequency token) instead of Rust
            // materializing both bitmaps and intersecting.
            //
            // XOR invariant preserved: a folded leaf is Lucene-authoritative
            // on every RG — its DataFusion expr is never applied (it is
            // removed from `performance_leaves`), and its RG-level stats
            // pruning stays active via `StatsPruneTree`. Fail-closed per
            // leaf: any upcall failure leaves that leaf on the per-RG
            // XOR path unchanged. Without a correctness collector (q7-style
            // pure equality counts) nothing is folded — the per-RG
            // DataFusion-XOR-Lucene choice machinery still decides.
            let mut folded_leaf_ids: std::collections::HashSet<i32> =
                std::collections::HashSet::new();
            let correctness_provider: Option<Arc<ProviderHandle>> = match correctness_provider {
                Some(base) => {
                    let mut chain = base;
                    for (annotation_id, _expr) in extraction.tree.delegation_possible_leaves() {
                        let Ok(leaf_provider) = create_provider(context_id, annotation_id) else {
                            continue; // fail-closed: leaf stays on the XOR path
                        };
                        match crate::indexed_table::ffm_callbacks::create_and_provider(
                            context_id,
                            &chain,
                            &leaf_provider,
                        ) {
                            Some(conj) => {
                                native_bridge_common::log_debug!(
                                    "[scf-rust] folded DelegationPossible leaf {} into Lucene conjunction provider {}",
                                    annotation_id,
                                    conj.key()
                                );
                                folded_leaf_ids.insert(annotation_id);
                                chain = Arc::new(conj);
                            }
                            None => continue, // fail-closed
                        }
                    }
                    Some(chain)
                }
                None => None,
            };

            // Performance-delegated provider locks (lazy). Built ONCE per query,
            // shared across all per-(segment×chunk) closures via Arc::clone — so
            // multiple DataFusion threads racing to populate the same Lucene
            // Weight do so once per (query × annotation_id), not per chunk.
            // Drop releases the Lucene Weight via `releaseProvider`.
            let performance_provider_locks: Arc<
                std::collections::HashMap<i32, Arc<std::sync::OnceLock<ProviderHandle>>>,
            > = {
                let leaves = extraction.tree.delegation_possible_leaves();
                let mut map = std::collections::HashMap::with_capacity(leaves.len());
                for (annotation_id, _expr) in &leaves {
                    map.entry(*annotation_id)
                        .or_insert_with(|| Arc::new(std::sync::OnceLock::new()));
                }
                Arc::new(map)
            };

            // Extract the residual (non-Collector children of top-level
            // AND) as a BoolNode and convert to PhysicalExpr. Used for:
            //   - Page-stats pruning in candidate stage (via PruningPredicate).
            //   - Parquet `with_predicate` pushdown in row-granular mode.
            //   - `on_batch_mask` refinement in block-granular mode.
            //
            // SingleCollector is AND(Collector?, DelegationPossible*, residual*). The residual has
            // zero Collectors AND zero DelegationPossible leaves — both are stripped by
            // `extract_single_collector_residual`. Correctness Collectors run eagerly; each
            // DelegationPossible (dual-viable) leaf is a `PerformanceLeaf` with a per-RG
            // DataFusion-XOR-Lucene choice (see `SingleCollectorEvaluator`), so its expr must NOT be
            // in the always-applied residual (which would push it to parquet / evaluate it even when
            // the per-RG choice selects Lucene).
            let residual_bool = extract_single_collector_residual(&extraction.tree);
            let residual_expr = residual_bool
                .as_ref()
                .and_then(residual_bool_to_physical_expr);
            let residual_pruning_predicate: Option<Arc<PruningPredicate>> = residual_expr
                .as_ref()
                .and_then(|expr| build_pruning_predicate(expr, Arc::clone(&schema_for_pruner)));

            // Performance-delegated (dual-viable) leaves: each carries its native DataFusion expr
            // plus a per-leaf `PruningPredicate` used at runtime to make the sound-stats
            // DataFusion-XOR-Lucene choice per row group.
            let performance_leaves: Vec<
                crate::indexed_table::eval::single_collector::PerformanceLeaf,
            > = extraction
                .tree
                .delegation_possible_leaves()
                .into_iter()
                // Folded leaves ride the Lucene conjunction collector — the
                // evaluator must not consult or apply them again (XOR).
                .filter(|(annotation_id, _)| !folded_leaf_ids.contains(annotation_id))
                .map(|(annotation_id, expr)| {
                    let pruning_predicate =
                        build_pruning_predicate(&expr, Arc::clone(&schema_for_pruner));
                    crate::indexed_table::eval::single_collector::PerformanceLeaf {
                        annotation_id,
                        expr,
                        pruning_predicate,
                    }
                })
                .collect();

            let call_strategy = CollectorCallStrategy::PageRangeSplit;
            let bloom_store = Arc::clone(&store);
            let bloom_schema = schema.clone();
            // Relaxed timestamp-WITHIN plumbing (captured per query, applied per
            // segment×chunk inside the closure). `residual_sans_sort_range` is
            // the always-native residual with the sort-range conjuncts removed;
            // used by the evaluator ONLY for WITHIN row groups.
            let relaxed_within_map = timestamp_within_rgs.clone();
            let residual_sans_sort_range = pushdown_predicate_sans_sort_range.clone();
            (
                Arc::new(
                    move |segment: &SegmentFileInfo,
                          chunk,
                          stream_metrics: &StreamMetrics,
                          stats_prune_tree: Option<&Arc<StatsPruneTree>>| {
                        let collector_opt: Option<Arc<dyn RowGroupDocsCollector>> =
                            match &correctness_provider {
                                Some(provider) => {
                                    let collector = FfmSegmentCollector::create(
                                context_id,
                                provider.key(),
                                segment.writer_generation,
                                chunk.doc_min,
                                chunk.doc_max,
                            )
                            .map_err(|e| {
                                format!(
                                    "FfmSegmentCollector::create(context_id={}, provider={}, writer_generation={}, doc_range=[{},{})): {}",
                                    context_id,
                                    provider.key(),
                                    segment.writer_generation,
                                    chunk.doc_min,
                                    chunk.doc_max,
                                    e
                                )
                            })?;
                                    Some(Arc::new(collector) as Arc<dyn RowGroupDocsCollector>)
                                }
                                None => None,
                            };
                        let pruner = Arc::new(PagePruner::new(
                            &schema_for_pruner,
                            Arc::clone(&segment.metadata),
                            segment.arrow_schema.clone(),
                        ));
                        // Bloom-filter row-group pruning is always enabled on the indexed read path.
                        let bloom_config =
                            Some(crate::indexed_table::eval::single_collector::BloomConfig {
                                store: Arc::clone(&bloom_store),
                                object_path: segment.object_path.clone(),
                                metadata: Arc::clone(&segment.metadata),
                                arrow_schema: Arc::clone(&bloom_schema),
                                io_handle: io_handle.clone(),
                                rg_bloom_pruned: stream_metrics.rg_bloom_pruned.clone(),
                                bloom_filter_eval_time: stream_metrics
                                    .bloom_filter_eval_time
                                    .clone(),
                            });
                        let eval: Arc<dyn RowGroupBitsetSource> =
                        Arc::new(SingleCollectorEvaluator::new(
                            collector_opt,
                            pruner,
                            residual_pruning_predicate.clone(),
                            residual_expr.clone(),
                            Some(PagePruneMetrics::from_stream_metrics(stream_metrics)),
                            stream_metrics.ffm_collector_calls.clone(),
                            call_strategy,
                            Arc::clone(&performance_provider_locks),
                            segment.writer_generation,
                            Arc::new(crate::indexed_table::eval::single_collector::FfmDelegatedBackendCollectorFactory),
                            context_id,
                            bloom_config,
                            stats_prune_tree.cloned(),
                            chunk.row_group_indices.iter().enumerate().map(|(pos, &idx)| (idx, pos)).collect(),
                            performance_leaves.clone(),
                        )
                        .with_relaxed_within(
                            residual_sans_sort_range.clone(),
                            relaxed_within_map
                                .as_ref()
                                .and_then(|m| m.get(&chunk.segment_idx))
                                .cloned()
                                .unwrap_or_default(),
                        )
                        .with_chunk_doc_bounds(chunk.doc_min, chunk.doc_max));
                        Ok(eval)
                    },
                ),
                prune_tree_config,
            )
        }
        FilterClass::Tree => {
            let extraction = extraction.ok_or_else(|| {
                DataFusionError::Internal(
                    "classify_filter returned Tree but extraction is None".into(),
                )
            })?;
            // Normalize: push NOTs to leaves (De Morgan) then flatten nested
            // same-kind connectives. Flatten after push_not_down so the
            // connective changes from De Morgan (e.g. NOT(AND(...)) -> OR(NOT...))
            // get absorbed into the surrounding Or if applicable.
            let tree = Arc::try_unwrap(extraction.tree)
                .unwrap()
                .push_not_down()
                .flatten();
            // One provider per Collector leaf (DFS order).
            let leaf_ids = tree.collector_leaves();
            let mut providers: Vec<Arc<ProviderHandle>> = Vec::with_capacity(leaf_ids.len());
            for annotation_id in &leaf_ids {
                providers.push(Arc::new(
                    create_provider(context_id, *annotation_id)
                        .map_err(|e| DataFusionError::External(e.into()))?,
                ));
            }
            let tree = Arc::new(tree);
            let schema_for_pruner = schema.clone();
            let cost_predicate = query_config.cost_predicate;
            let cost_collector = query_config.cost_collector;
            let max_collector_parallelism = 1;
            let collector_strategy = CollectorCallStrategy::TightenOuterBounds;

            // Build one `PruningPredicate` per unique `Predicate` leaf
            // in the tree. Key = `Arc::as_ptr(expr) as usize` — the
            // same `Arc<PhysicalExpr>` reaches the tree walker at
            // candidate stage. Predicates that fail to translate or
            // resolve to always-true are omitted; the walker's
            // fallback treats missing entries as "no pruning for this
            // leaf" (safe: universe bitmap).
            let mut leaf_exprs: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
            collect_predicate_exprs(&tree, &mut leaf_exprs);
            let pruning_predicates: Arc<HashMap<usize, Arc<PruningPredicate>>> = Arc::new(
                leaf_exprs
                    .iter()
                    .filter_map(|expr| {
                        let result = build_pruning_predicate(expr, Arc::clone(&schema_for_pruner));
                        result.map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
                    })
                    .collect(),
            );

            // Build prune_tree_config from the normalized tree. This ensures
            // StatsPruneTree children indices align with ResolvedNode children
            // (same push_not_down + flatten normalization applied above).
            let prune_tree_config = if pruning_predicates.is_empty() {
                None
            } else {
                Some((
                    Arc::clone(&tree),
                    Arc::clone(&pruning_predicates),
                    schema_for_pruner.clone(),
                ))
            };

            (
                Arc::new(
                    move |segment: &SegmentFileInfo,
                          chunk,
                          stream_metrics: &StreamMetrics,
                          stats_prune_tree: Option<&Arc<StatsPruneTree>>| {
                        // Build one collector per Collector leaf for this chunk.
                        let mut per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> =
                            Vec::with_capacity(providers.len());
                        for (idx, provider) in providers.iter().enumerate() {
                            let collector = FfmSegmentCollector::create(
                                context_id,
                                provider.key(),
                                segment.writer_generation,
                                chunk.doc_min,
                                chunk.doc_max,
                            )
                            .map_err(|e| format!("leaf {} collector: {}", idx, e))?;
                            per_leaf.push((
                                provider.key(),
                                Arc::new(collector) as Arc<dyn RowGroupDocsCollector>,
                            ));
                        }

                        let resolved = tree.resolve(&per_leaf).map_err(|e| {
                            format!(
                                "tree.resolve for segment gen={}: {}",
                                segment.writer_generation, e
                            )
                        })?;
                        let resolved = Arc::new(resolved);

                        let pruner = Arc::new(PagePruner::new(
                            &schema_for_pruner,
                            Arc::clone(&segment.metadata),
                            segment.arrow_schema.clone(),
                        ));

                        let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                            tree: resolved,
                            evaluator: Arc::new(BitmapTreeEvaluator),
                            leaves: Arc::new(CollectorLeafBitmaps::new(
                                stream_metrics.ffm_collector_calls.clone(),
                            )),
                            page_pruner: pruner,
                            cost_predicate,
                            cost_collector,
                            max_collector_parallelism,
                            pruning_predicates: Arc::clone(&pruning_predicates),
                            page_prune_metrics: Some(PagePruneMetrics::from_stream_metrics(
                                stream_metrics,
                            )),
                            collector_strategy,
                            stats_prune_tree: stats_prune_tree.cloned(),
                            rg_index_to_pos: chunk
                                .row_group_indices
                                .iter()
                                .enumerate()
                                .map(|(pos, &idx)| (idx, pos))
                                .collect(),
                        });
                        Ok(eval)
                    },
                ),
                prune_tree_config,
            )
        }
    };

    ctx.deregister_table(&register_name)?;
    // Extract the scheme+authority portion of the table URL for
    // DataFusion's FileScanConfig. The full URL includes the path
    // (e.g. "file:///Users/.../parquet/"); ObjectStoreUrl wants only
    // the scheme+authority ("file:///").
    let url_str = table_path.as_str();
    let parsed = url::Url::parse(url_str)
        .map_err(|e| DataFusionError::Execution(format!("parse table_path URL: {}", e)))?;
    let store_url = ObjectStoreUrl::parse(format!("{}://{}", parsed.scheme(), parsed.authority()))?;

    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
        store: Arc::clone(&store),
        store_url,
        evaluator_factory: factory,
        pushdown_predicate,
        query_config: Arc::clone(&query_config),
        predicate_columns,
        emit_row_ids,
        prune_tree_config,
        sort_fields: sort_fields.clone(),
        sort_orders: sort_orders.clone(),
        sort_range_within_rgs,
        timestamp_within_rgs,
        topk_range_within_rgs,
        pushdown_predicate_sans_sort_range,
        sort_topk_truncate,
        activation_diagnostics: crate::indexed_table::table_provider::ActivationDiagnostics {
            filter_expr_present: diag_filter_expr_present,
            sort_column_unit_code: diag_sort_column_unit_code,
            candidate_range_detected: diag_candidate_range_detected,
            candidate_lower: diag_candidate_lower,
            candidate_upper: diag_candidate_upper,
            count_tree_shape_supported: diag_count_tree_shape_supported,
            top_sort_present: diag_top_sort_present,
            top_sort_single_key: diag_top_sort_single_key,
            top_sort_fetch: diag_top_sort_fetch,
            top_sort_key_matches_catalog: diag_top_sort_key_matches_catalog,
            topk_truncation_path_safe: diag_topk_truncation_path_safe,
            within_reasons,
            topk_within_reasons,
        },
        cancellation_token: crate::query_tracker::get_cancellation_token(context_id),
    }));
    ctx.register_table(&register_name, provider)?;

    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;
    log_debug!(
        "DataFusion logical plan:\n{}",
        logical_plan.display_indent()
    );
    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    // Histogram fast path: replace the Partial-aggregate subtree over the
    // indexed scan with UnionExec[boundary decode, footer-stats counts] when
    // the shape gates hold (see indexed_table::histogram). Fail-closed: any
    // unmatched shape returns the plan unchanged.
    let physical_plan = match sort_fields.first() {
        Some(sort_field) => {
            crate::indexed_table::histogram::try_rewrite_histogram(physical_plan, sort_field)
        }
        None => physical_plan,
    };
    // Retag bit-compatible Int↔UInt output mismatches to match the substrait-declared
    // types. The target is schema_coerce::coerce_inferred_schema(physical_schema) — same
    // narrowing the partition-stream registration uses, so consumer-side StreamingTable
    // and producer-side batches agree by construction (see crate::relabel_exec).
    // Apply aggregate mode stripping when prepare_partial_plan was called (engine-native-merge).
    // This makes the indexed executor produce Binary HLL state (Partial) instead of Int64 (Final).
    let physical_plan = if aggregate_mode != crate::agg_mode::Mode::Default {
        crate::agg_mode::apply_aggregate_mode(physical_plan, aggregate_mode, handle.has_topk)?
    } else {
        physical_plan
    };
    let target_schema = crate::schema_coerce::coerce_inferred_schema(physical_plan.schema());
    let physical_plan = crate::relabel_exec::wrap_if_relabel_needed(physical_plan, target_schema)?;
    log_debug!(
        "DataFusion physical plan:\n{}",
        displayable(physical_plan.as_ref()).indent(true)
    );
    let df_stream = execute_stream(physical_plan.clone(), ctx.task_ctx())
        .map_err(|e| DataFusionError::Execution(format!("execute_stream: {}", e)))?;

    let (cross_rt_stream, abort_handle, _task_done) =
        CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_executor.clone(), None);

    if let Some(h) = abort_handle {
        crate::query_tracker::set_abort_handle(context_id, h);
    }
    if let Some(rt) = cpu_executor.handle() {
        crate::query_tracker::set_cpu_runtime_handle(context_id, rt);
    }

    let schema = cross_rt_stream.schema();
    let wrapped = RecordBatchStreamAdapter::new(schema, cross_rt_stream);
    let stream_handle = crate::api::QueryStreamHandle::with_physical_plan(
        wrapped,
        query_context,
        ctx,
        Some(permit),
        physical_plan,
    );
    Ok(Box::into_raw(Box::new(stream_handle)) as i64)
}
