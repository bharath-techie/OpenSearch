/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unified DataFusion `TableProvider` for all indexed-query paths.
//!
//! This is the ONE provider. Paths B and C differ only in the evaluator
//! factory closure supplied in `IndexedTableConfig`. The provider itself,
//! the `QueryShardExec` it wraps, and the `IndexedExec`s it spawns are
//! identical across paths.
//!
//! ```text
//!     IndexedTableProvider (scan)
//!             │
//!             ▼
//!     QueryShardExec (1 per query, partitioned across chunks)
//!             │
//!             ├── IndexedExec(chunk_0) ── IndexedStream ── RowGroupBitsetSource
//!             ├── IndexedExec(chunk_1) ── IndexedStream ── RowGroupBitsetSource
//!             └── IndexedExec(chunk_N) ── IndexedStream ── RowGroupBitsetSource
//! ```

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, Statistics};
use datafusion::datasource::TableType;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::expressions::col as physical_col;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, Partitioning, PhysicalSortExpr,
};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::DataFusionError;

use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;

use super::bool_tree::BoolNode;
use super::eval::RowGroupBitsetSource;
use super::metrics::PartitionMetrics;
use super::parquet_bridge::ReadIoStats;
use super::partitioning::{
    compute_assignments, compute_assignments_one_per_segment, segments_chain_on_sort_key,
    PartitionAssignment, SegmentChunk, SegmentLayout,
};
use super::stream::{IndexedExec, RowGroupInfo};
use crate::datafusion_query_config::DatafusionQueryConfig;
use crate::indexed_table::metrics::StreamMetrics;
use crate::indexed_table::page_pruner::StatsPruneTree;
use crate::search_stats::accumulate_from_exec;
use std::collections::HashSet;

/// Info about a segment and its corresponding parquet file.
#[derive(Debug, Clone)]
pub struct SegmentFileInfo {
    /// Writer generation for this segment — the stable per-segment identifier
    /// that crosses the FFM boundary to identify a segment on the Java side.
    /// Read from the parquet footer key-value metadata
    /// (`opensearch.writer_generation`) at `build_segments` time.
    pub writer_generation: i64,
    pub max_doc: i64,
    /// Object-store-relative path to the parquet file (same as the
    /// `ObjectMeta.location` DataFusion uses for the vanilla `ListingTable`).
    pub object_path: object_store::path::Path,
    pub parquet_size: u64,
    pub row_groups: Vec<RowGroupInfo>,
    pub metadata: Arc<ParquetMetaData>,
    /// Arrow schema derived from this segment's parquet footer. Computed once
    /// at `build_segments` time by `parquet_to_arrow_schema` and reused by
    /// page pruning, dynamic filter pruning, and column schema resolution
    /// instead of re-deriving from the footer on every access.
    pub arrow_schema: SchemaRef,
    /// Cumulative row count from all preceding segments. Used to compute
    /// shard-global row IDs: `global_base + rg.first_row + position_in_rg`.
    pub global_base: u64,
    /// Min/max of the LEAD `index.sort.field` column across all row groups
    /// in this segment, read from parquet footer column statistics.
    /// Both `None` when no sort field is configured, when any RG is missing
    /// stats for the lead column, or when the column type isn't supported by
    /// `StatisticsConverter`. `None` means the segment cannot participate in
    /// the chain decision (treated as "can't chain").
    pub sort_min: Option<datafusion::common::ScalarValue>,
    pub sort_max: Option<datafusion::common::ScalarValue>,
}

/// Factory: build a `RowGroupBitsetSource` for one `SegmentChunk`.
///
/// Invoked once per chunk per query. For the single-collector path this
/// produces a `SingleCollectorEvaluator`. For the multi-filter tree path it
/// produces a `BitmapTreeEvaluator`-backed `TreeBitsetSource`.
///
/// The closure is cloneable (stored in an `Arc`) so the provider can spawn
/// many `IndexedExec`s from a single config.
///
/// # Pluggability
///
/// `RowGroupBitsetSource` is the single seam that determines *where* tree
/// evaluation happens. Today the built-in impls all walk the tree in Rust,
/// but a future `JavaTreeBitsetSource` could route per-RG evaluation to
/// analytics-core via an FFM upcall without touching `IndexedStream`,
/// `IndexedExec`, or this factory's signature. Evaluators that carry
/// cross-chunk or cross-query state (e.g. a Java-resident tree) should
/// keep that state external and reference it by handle from the evaluator.
pub type EvaluatorFactory = Arc<
    dyn Fn(
            &SegmentFileInfo,
            &SegmentChunk,
            &StreamMetrics,
            Option<&Arc<StatsPruneTree>>,
        ) -> Result<Arc<dyn RowGroupBitsetSource>, String>
        + Send
        + Sync,
>;

/// Build a `LexOrdering` from `sort_fields` / `sort_orders` against the given
/// projected schema, mirroring DataFusion's `create_ordering`
/// (`physical-expr/src/physical_expr.rs:134`):
/// - on first column that doesn't resolve, **break** out of the loop and
///   keep whatever prefix we built (the rest is "violated"),
/// - returns `None` when the prefix is empty (no useful claim to advertise).
///
/// Direction strings are `"asc"` / `"desc"` (lowercase, as plumbed from Java).
/// Nulls placement matches Lucene's convention: ASC → NULLS FIRST,
/// DESC → NULLS LAST. Same as the vanilla path's `build_file_sort_order` in
/// `session_context.rs`.
fn build_projected_lex_ordering(
    projected_schema: &SchemaRef,
    sort_fields: &[String],
    sort_orders: &[String],
) -> Option<LexOrdering> {
    if sort_fields.is_empty() {
        return None;
    }
    let mut exprs: Vec<PhysicalSortExpr> = Vec::with_capacity(sort_fields.len());
    for (i, field) in sort_fields.iter().enumerate() {
        let phys = match physical_col(field, projected_schema) {
            Ok(e) => e,
            Err(_) => break,
        };
        let descending = sort_orders
            .get(i)
            .map(|s| s.eq_ignore_ascii_case("desc"))
            .unwrap_or(false);
        let ascending = !descending;
        let opts = SortOptions {
            descending,
            // ASC → NULLS FIRST, DESC → NULLS LAST (matches Lucene + vanilla path).
            nulls_first: ascending,
        };
        exprs.push(PhysicalSortExpr::new(phys, opts));
    }
    LexOrdering::new(exprs)
}

/// Planning-time snapshot of the timestamp fast-path / QTF top-K "activation
/// chain" decisions, carried from `execute_indexed_with_context_inner` to
/// `IndexedTableProvider::scan` where each field is surfaced as a
/// profile-visible metric (see `scan()` — the `activation_*` counters/gauges).
///
/// This is diagnostics-only: it never influences execution. It exists so a
/// `profile:true` run shows exactly which condition gated (or failed to gate)
/// the per-row-group top-K truncation, without adding per-row logging. All
/// fields default to the "inactive" value, so test constructors can use
/// `Default::default()`.
#[derive(Debug, Clone, Default)]
pub struct ActivationDiagnostics {
    /// A shard filter predicate was extracted from the logical plan.
    pub filter_expr_present: bool,
    /// Arrow `TimeUnit` the leading sort column resolved to, encoded as
    /// `0` = none/not-a-timestamp, `1` = second, `2` = millisecond,
    /// `3` = microsecond, `4` = nanosecond. The footer/bound unit mismatch
    /// (ns folded bound vs ms footer) is diagnosed by comparing this against
    /// the normalized `candidate_*` bounds.
    pub sort_column_unit_code: u8,
    /// A conjunctive sort-column range was derived from the (const-folded)
    /// predicate after unit normalization.
    pub candidate_range_detected: bool,
    /// Normalized inclusive lower bound (in the sort column's footer unit).
    pub candidate_lower: Option<i64>,
    /// Normalized inclusive upper bound (in the sort column's footer unit).
    pub candidate_upper: Option<i64>,
    /// The BoolNode tree shape supports the count/tautology fast path for the
    /// sort-range column (`count_tree_shape_supported`).
    pub count_tree_shape_supported: bool,
    /// A top-level ORDER BY was found (`analyze_top_sort`).
    pub top_sort_present: bool,
    /// The top-level sort has exactly one sort key.
    pub top_sort_single_key: bool,
    /// The enclosing top-K row budget (`fetch`), when bounded.
    pub top_sort_fetch: Option<usize>,
    /// The leading sort key matches the catalog's leading `index.sort.field`.
    pub top_sort_key_matches_catalog: bool,
    /// The fail-closed Sort→TableScan path guard
    /// (`analyze_scan_topk_truncation_path`) proved every operator between the
    /// bounded top-K Sort and the indexed scan is on the safe allowlist, so
    /// scan-level candidate truncation is eligible. `false` means a barrier
    /// (Window/ROW_NUMBER, Aggregate, Join, Distinct, set op, Unnest, inner
    /// Sort, …) sits on the path — truncation is fail-closed OFF regardless of
    /// the top-K shape. Surfaced as `activation_topk_truncation_path_safe`.
    pub topk_truncation_path_safe: bool,
    /// Reason-level tally from the strict WITHIN sort-range classifier
    /// (`segment_within_rgs`) — why the WITHIN set is (or is not) empty.
    pub within_reasons: WithinClassifierReasons,
    /// Reason-level tally from the dedicated Top-K WITHIN classifier pass — the
    /// SAME `segment_within_rgs` footer check, but run under the Top-K shape
    /// gate instead of the count-shortcut gate. Independent of `within_reasons`
    /// (populated even when the strict count set is empty). Surfaced as
    /// `activation_topk_within_reason_*` gauges. Diagnostics-only.
    pub topk_within_reasons: WithinClassifierReasons,
}

/// Reason-level counters for the strict WITHIN sort-range classifier
/// (`segment_within_rgs`), aggregated across every segment of the classifier
/// pass. Diagnostics-only: incrementing a counter never influences the WITHIN
/// set the classifier returns. Surfaced as `activation_within_reason_*` gauges
/// so a `profile:true` run shows exactly which fail-closed exit / row-group
/// rejection kept the WITHIN set empty (the `sort_range_within_rgs = 0` /
/// `rg_topk_truncated = 0` gap) without any per-row logging.
///
/// Segment-level counters (`*_error`, `*_missing`, `*_failure`,
/// `vector_length_mismatch`) tally once per segment that took the matching
/// fail-closed early exit. Row-group counters tally once per row group; a
/// single row group may contribute to more than one rejection counter (e.g. a
/// missing min AND an unavailable null count) — they are independent tallies,
/// not a partition. `within_accepted` is mutually exclusive with the row-group
/// rejection counters for a given row group.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WithinClassifierReasons {
    /// `StatisticsConverter::try_new` failed (segment-level fail-closed exit).
    pub converter_creation_failure: usize,
    /// The converter resolved no parquet column index for the sort column
    /// (segment-level fail-closed exit).
    pub parquet_column_index_missing: usize,
    /// `row_group_mins` returned an error (segment-level fail-closed exit).
    pub row_group_mins_error: usize,
    /// `row_group_maxes` returned an error (segment-level fail-closed exit).
    pub row_group_maxes_error: usize,
    /// `row_group_null_counts` returned an error (segment-level fail-closed exit).
    pub row_group_null_counts_error: usize,
    /// mins/maxes/null_counts length disagreed with the row-group count
    /// (segment-level fail-closed exit).
    pub vector_length_mismatch: usize,
    /// A row group's min statistic was absent or not an i64/timestamp scalar.
    pub min_scalar_unsupported: usize,
    /// A row group's max statistic was absent or not an i64/timestamp scalar.
    pub max_scalar_unsupported: usize,
    /// A row group's null count statistic was unavailable (null).
    pub null_count_unavailable: usize,
    /// A row group had a non-zero null count.
    pub null_count_nonzero: usize,
    /// A row group's min was below the range's lower bound.
    pub lower_bound_rejection: usize,
    /// A row group's max was above the range's upper bound.
    pub upper_bound_rejection: usize,
    /// A row group was accepted as fully WITHIN.
    pub within_accepted: usize,
}

/// Emit the activation-chain diagnostics onto `metrics` as `global_*`
/// (partition-less) counters/gauges. `activation_*` boolean flags are a
/// `Count` set to 0/1 (always registered so a `0` is explicit in the profile);
/// scalar values are gauges. Called once per shard scan and re-called when the
/// node is rebuilt for a dynamic-filter pushdown, so the counters survive a
/// TopK filter pushdown. Diagnostics-only: reads config, never mutates it.
fn record_activation_diagnostics(metrics: &ExecutionPlanMetricsSet, config: &IndexedTableConfig) {
    let d = &config.activation_diagnostics;
    let flag = |name: &'static str, on: bool| {
        let c: Count = MetricBuilder::new(metrics).global_counter(name);
        c.add(on as usize);
    };
    let gauge = |name: &'static str, v: usize| {
        let g: Gauge = MetricBuilder::new(metrics).global_gauge(name);
        g.set(v);
    };
    // Row-id emission / QTF request (== requests_row_ids on the Java side).
    flag("activation_emit_row_ids", config.emit_row_ids);
    flag("activation_filter_expr_present", d.filter_expr_present);
    // Sort catalog + resolved unit.
    gauge("activation_sort_fields_count", config.sort_fields.len());
    gauge(
        "activation_sort_column_unit",
        d.sort_column_unit_code as usize,
    );
    // Top-level ORDER BY shape.
    flag("activation_top_sort_present", d.top_sort_present);
    flag("activation_top_sort_single_key", d.top_sort_single_key);
    gauge("activation_top_sort_fetch", d.top_sort_fetch.unwrap_or(0));
    flag(
        "activation_top_sort_key_matches_catalog",
        d.top_sort_key_matches_catalog,
    );
    // Strict Sort→scan path guard for scan-level candidate truncation. `0` here
    // (with `top_sort_present=1`) is the Q5 signature: a bounded top-K exists
    // but a barrier operator between the Sort and the scan disqualified
    // truncation.
    flag(
        "activation_topk_truncation_path_safe",
        d.topk_truncation_path_safe,
    );
    // Candidate sort-range detection + normalized bounds. `has_*` flags
    // disambiguate an open bound from a clamped `0`; the value gauges clamp
    // negatives to 0 (log timestamps are non-negative).
    flag(
        "activation_candidate_sort_range_detected",
        d.candidate_range_detected,
    );
    flag(
        "activation_candidate_sort_range_has_lower",
        d.candidate_lower.is_some(),
    );
    flag(
        "activation_candidate_sort_range_has_upper",
        d.candidate_upper.is_some(),
    );
    gauge(
        "activation_candidate_sort_range_lower",
        d.candidate_lower.unwrap_or(0).max(0) as usize,
    );
    gauge(
        "activation_candidate_sort_range_upper",
        d.candidate_upper.unwrap_or(0).max(0) as usize,
    );
    flag(
        "activation_count_tree_shape_supported",
        d.count_tree_shape_supported,
    );
    // Segment / row-group universe examined by the WITHIN classifier.
    gauge("activation_segments_total", config.segments.len());
    let rg_total: usize = config.segments.iter().map(|s| s.row_groups.len()).sum();
    gauge("activation_row_groups_total", rg_total);
    // Strict (count-shortcut) and relaxed (projection-pruning) WITHIN row-group
    // counts, summed across segments.
    let within_strict: usize = config
        .sort_range_within_rgs
        .as_ref()
        .map(|m| m.values().map(|s| s.len()).sum())
        .unwrap_or(0);
    gauge("activation_sort_range_within_rgs", within_strict);
    // Reason-level breakdown of the strict WITHIN classifier — why the WITHIN
    // set above is (or is not) empty. Aggregated across all segments of the
    // strict pass. Segment-level fail-closed exits and per-row-group rejections
    // are independent tallies; `within_accepted` sums the RGs actually admitted.
    record_within_reason_metrics(metrics, &d.within_reasons);
    let within_relaxed: usize = config
        .timestamp_within_rgs
        .as_ref()
        .map(|m| m.values().map(|s| s.len()).sum())
        .unwrap_or(0);
    gauge("activation_timestamp_within_rgs", within_relaxed);
    // Dedicated Top-K WITHIN row-group count (drives `sort_topk_truncate`),
    // independent of the strict count set above. Plus its reason-level
    // breakdown (`activation_topk_within_reason_*`) so a `profile:true` run can
    // explain an empty Top-K WITHIN set even when the strict tally is empty.
    let topk_within: usize = config
        .topk_range_within_rgs
        .as_ref()
        .map(|m| m.values().map(|s| s.len()).sum())
        .unwrap_or(0);
    gauge("activation_topk_range_within_rgs", topk_within);
    record_topk_within_reason_metrics(metrics, &d.topk_within_reasons);
    // Top-K truncation config actually placed on the provider.
    let (tk_conf, tk_keep, tk_budget) = match config.sort_topk_truncate {
        Some((keep_last, budget)) => (true, keep_last, budget),
        None => (false, false, 0),
    };
    flag("activation_sort_topk_truncate_configured", tk_conf);
    flag("activation_sort_topk_truncate_keep_last", tk_keep);
    gauge("activation_sort_topk_truncate_budget", tk_budget);
}

/// Emit the strict WITHIN classifier's reason-level tally as
/// `activation_within_reason_*` global gauges. Split out of
/// [`record_activation_diagnostics`] so the exact metric names can be
/// unit-verified without constructing a full `IndexedTableConfig`.
fn record_within_reason_metrics(metrics: &ExecutionPlanMetricsSet, r: &WithinClassifierReasons) {
    emit_within_reason_gauges(metrics, "activation_within_reason_", r);
}

/// Emit the dedicated Top-K WITHIN classifier pass's reason-level tally as
/// `activation_topk_within_reason_*` global gauges. Same footer classifier as
/// the strict pass, but gated by the Top-K shape instead of the count shortcut,
/// so a `profile:true` run can explain an empty `topk_range_within_rgs` set
/// even when the strict `within_reasons` tally is empty.
fn record_topk_within_reason_metrics(
    metrics: &ExecutionPlanMetricsSet,
    r: &WithinClassifierReasons,
) {
    emit_within_reason_gauges(metrics, "activation_topk_within_reason_", r);
}

/// Emit a [`WithinClassifierReasons`] tally as global gauges under `<prefix>*`.
/// Shared by the strict and Top-K WITHIN passes so the two only differ by their
/// metric-name prefix. `prefix` must include the trailing separator (e.g.
/// `"activation_within_reason_"`).
fn emit_within_reason_gauges(
    metrics: &ExecutionPlanMetricsSet,
    prefix: &str,
    r: &WithinClassifierReasons,
) {
    let gauge = |suffix: &str, v: usize| {
        let g: Gauge = MetricBuilder::new(metrics).global_gauge(format!("{prefix}{suffix}"));
        g.set(v);
    };
    gauge("converter_creation_failure", r.converter_creation_failure);
    gauge(
        "parquet_column_index_missing",
        r.parquet_column_index_missing,
    );
    gauge("row_group_mins_error", r.row_group_mins_error);
    gauge("row_group_maxes_error", r.row_group_maxes_error);
    gauge("row_group_null_counts_error", r.row_group_null_counts_error);
    gauge("vector_length_mismatch", r.vector_length_mismatch);
    gauge("min_scalar_unsupported", r.min_scalar_unsupported);
    gauge("max_scalar_unsupported", r.max_scalar_unsupported);
    gauge("null_count_unavailable", r.null_count_unavailable);
    gauge("null_count_nonzero", r.null_count_nonzero);
    gauge("lower_bound_rejection", r.lower_bound_rejection);
    gauge("upper_bound_rejection", r.upper_bound_rejection);
    gauge("within_accepted", r.within_accepted);
}

#[cfg(test)]
mod within_reason_metric_tests {
    use super::*;

    /// The reason-level counters surface under their exact
    /// `activation_within_reason_*` names, each carrying its own value — the
    /// contract a `profile:true` inspection relies on.
    #[test]
    fn within_reason_gauges_surface_by_exact_name() {
        let reasons = WithinClassifierReasons {
            converter_creation_failure: 1,
            parquet_column_index_missing: 2,
            row_group_mins_error: 3,
            row_group_maxes_error: 4,
            row_group_null_counts_error: 5,
            vector_length_mismatch: 6,
            min_scalar_unsupported: 7,
            max_scalar_unsupported: 8,
            null_count_unavailable: 9,
            null_count_nonzero: 10,
            lower_bound_rejection: 11,
            upper_bound_rejection: 12,
            within_accepted: 13,
        };
        let metrics = ExecutionPlanMetricsSet::new();
        record_within_reason_metrics(&metrics, &reasons);
        let set = metrics.clone_inner();
        let g = |name: &str| set.sum_by_name(name).map(|v| v.as_usize());

        assert_eq!(
            g("activation_within_reason_converter_creation_failure"),
            Some(1)
        );
        assert_eq!(
            g("activation_within_reason_parquet_column_index_missing"),
            Some(2)
        );
        assert_eq!(g("activation_within_reason_row_group_mins_error"), Some(3));
        assert_eq!(g("activation_within_reason_row_group_maxes_error"), Some(4));
        assert_eq!(
            g("activation_within_reason_row_group_null_counts_error"),
            Some(5)
        );
        assert_eq!(
            g("activation_within_reason_vector_length_mismatch"),
            Some(6)
        );
        assert_eq!(
            g("activation_within_reason_min_scalar_unsupported"),
            Some(7)
        );
        assert_eq!(
            g("activation_within_reason_max_scalar_unsupported"),
            Some(8)
        );
        assert_eq!(
            g("activation_within_reason_null_count_unavailable"),
            Some(9)
        );
        assert_eq!(g("activation_within_reason_null_count_nonzero"), Some(10));
        assert_eq!(
            g("activation_within_reason_lower_bound_rejection"),
            Some(11)
        );
        assert_eq!(
            g("activation_within_reason_upper_bound_rejection"),
            Some(12)
        );
        assert_eq!(g("activation_within_reason_within_accepted"), Some(13));
    }
}
pub struct IndexedTableConfig {
    pub schema: SchemaRef,
    pub segments: Vec<SegmentFileInfo>,
    /// Object store for reading parquet bytes. All I/O on the indexed path
    /// goes through this same store resolution as vanilla — no hardcoded
    /// LocalFileSystem. Resolved once per query from the runtime env.
    pub store: Arc<dyn object_store::ObjectStore>,
    /// URL of the store for DataFusion's `FileScanConfig`.
    pub store_url: datafusion::execution::object_store::ObjectStoreUrl,
    pub evaluator_factory: EvaluatorFactory,
    /// Parquet-native residual predicate to push into decode time via
    /// `ParquetSource::with_predicate`. Derived from the BoolNode tree
    /// by `execute_indexed_query`:
    /// - `FilterClass::SingleCollector`: residual (non-Collector
    ///   children of top AND) as a single `PhysicalExpr`.
    /// - `FilterClass::Tree`: `None` (BitmapTreeEvaluator does all
    ///   refinement in `on_batch_mask`; pushdown would risk invoking
    ///   the `index_filter` UDF).
    ///
    /// `scan()` uses this rather than the `filters` argument it
    /// receives from DataFusion, because DataFusion's filters include
    /// the `index_filter(...)` UDF marker whose body panics.
    pub pushdown_predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Query-scoped tunables (batch_size, target_partitions, costs, …).
    /// Shared by reference across fanned-out `QueryShardExec` instances.
    pub query_config: Arc<DatafusionQueryConfig>,
    /// Full-schema column indices referenced by BoolNode Predicate leaves.
    pub predicate_columns: Vec<usize>,
    /// When true, the `___row_id` column in the output projection is computed
    /// from position (global_base + rg.first_row + position_in_rg) instead of
    /// being read from parquet. Other projected columns are read normally.
    pub emit_row_ids: bool,
    /// Query-level data for building StatsPruneTree per segment.
    /// (BoolNode tree, prebuilt PruningPredicates keyed by Arc ptr, schema)
    pub prune_tree_config: Option<(
        Arc<BoolNode>,
        Arc<std::collections::HashMap<usize, Arc<PruningPredicate>>>,
        SchemaRef,
    )>,
    /// `index.sort.field` — column names that the parquet writer used to sort
    /// rows on disk. Empty when the index has no `index.sort.field`.
    pub sort_fields: Vec<String>,
    /// Parallel to `sort_fields`. Each entry is `"asc"` or `"desc"` (lowercase,
    /// matches the wire format from `DataFusionPlugin`). Same length as
    /// `sort_fields` (validated at index creation).
    pub sort_orders: Vec<String>,
    /// Per-segment sets of row-group indices whose sort-column FOOTER
    /// statistics lie fully inside the query's timestamp range with zero
    /// nulls, valid ONLY when the non-collector residual consists solely of
    /// that range (so the residual is a tautology on these row groups).
    /// Keyed by segment index (post any iteration-order reversal, matching
    /// `segments`). `None` disables the WITHIN count shortcut.
    pub sort_range_within_rgs:
        Option<Arc<std::collections::HashMap<usize, std::collections::HashSet<usize>>>>,
    /// Dedicated per-segment WITHIN sets that drive per-RG Top-K candidate
    /// truncation (`sort <key> | head N`). Computed INDEPENDENTLY of
    /// `sort_range_within_rgs` and its `count_tree_shape_supported` gate: this
    /// set is gated ONLY by the Top-K shape (single-key, bounded fetch, key ==
    /// leading catalog sort field) plus footer-WITHIN classification
    /// (`segment_within_rgs`). Keyed by segment index (matching `segments`).
    /// `None` disables Top-K truncation.
    ///
    /// Kept separate from `sort_range_within_rgs` on purpose: the strict count
    /// shortcut only populates that field when the ENTIRE residual is a
    /// sort-range tautology, so a `match(...) AND ts-range | sort ts | head N`
    /// query (single-shard Q4) left it empty and truncation never armed even
    /// though the Top-K preconditions all held. Truncation must also NOT ride
    /// the relaxed `timestamp_within_rgs` projection-stripping set, hence a
    /// third, dedicated map here.
    pub topk_range_within_rgs:
        Option<Arc<std::collections::HashMap<usize, std::collections::HashSet<usize>>>>,
    /// Relaxed per-segment WITHIN sets for projection/predicate pruning. A
    /// superset of `sort_range_within_rgs`: populated whenever the residual has
    /// at least one strippable sort-range conjunct on the leading sort field
    /// (even alongside other conjuncts). For these row groups the sort column
    /// is dropped from the per-RG parquet projection and the pushdown predicate
    /// is replaced with `pushdown_predicate_sans_sort_range`. Keyed by segment
    /// index (matching `segments`). `None` disables the relaxation.
    pub timestamp_within_rgs:
        Option<Arc<std::collections::HashMap<usize, std::collections::HashSet<usize>>>>,
    /// The `pushdown_predicate` with the sort-range conjuncts removed. Handed to
    /// parquet's `with_predicate` (row-granular) and applied post-decode by the
    /// evaluator (block-granular) for row groups in `timestamp_within_rgs`.
    /// `None` means the residual was solely the sort range (fully stripped).
    pub pushdown_predicate_sans_sort_range:
        Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Top-K candidate truncation for single-key sorts on the leading sort
    /// field: `(keep_last_in_storage_order, row_budget)`. Applied by
    /// `IndexedStream` to `topk_range_within_rgs` row groups only (NOT the
    /// strict `sort_range_within_rgs` count set). `None` disables.
    pub sort_topk_truncate: Option<(bool, usize)>,
    /// Planning-time activation-chain diagnostics, surfaced as `activation_*`
    /// profile metrics in `scan()`. Diagnostics-only — never affects execution.
    pub activation_diagnostics: ActivationDiagnostics,
    /// Per-query cancellation token (from the global `QUERY_REGISTRY`). Threaded
    /// down to `IndexReader` so the scan cooperatively stops when the query task
    /// is cancelled. `None` for untracked queries (`context_id == 0`) and tests.
    pub cancellation_token: Option<tokio_util::sync::CancellationToken>,
}

/// Table provider. Returns a `QueryShardExec` that fans out across chunks.
pub struct IndexedTableProvider {
    config: Arc<IndexedTableConfig>,
}

impl fmt::Debug for IndexedTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedTableProvider")
            .field("segments", &self.config.segments.len())
            .field("partitions", &self.config.query_config.target_partitions)
            .finish()
    }
}

impl IndexedTableProvider {
    pub fn new(config: IndexedTableConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

#[async_trait]
impl TableProvider for IndexedTableProvider {
    fn schema(&self) -> SchemaRef {
        self.config.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // `Exact` — the BoolNode tree held by the evaluator factory
        // fully handles every WHERE filter (Collectors via FFM bitsets,
        // Predicates via arrow kernels in refinement). DataFusion
        // removes the outer FilterExec, which is important because
        // otherwise FilterExec would try to evaluate the
        // `index_filter(...)` UDF whose body panics by design.
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let full_schema = self.config.schema.clone();

        // Detect __row_id__ in the output projection when emit_row_ids=true.
        // If present, we strip it from the parquet read and compute it from position.
        let row_id_col_in_full_schema = full_schema.index_of(crate::ROW_ID_COLUMN_NAME).ok();
        let row_id_output_index: Option<usize> = if self.config.emit_row_ids {
            match projection {
                Some(proj) => proj
                    .iter()
                    .position(|&idx| Some(idx) == row_id_col_in_full_schema),
                None => row_id_col_in_full_schema,
            }
        } else {
            None
        };

        // Output schema = what DataFusion expects (includes ___row_id if projected).
        // When computing row IDs, replace the ___row_id field type with UInt64.
        let output_schema: SchemaRef = {
            let base: SchemaRef = match projection {
                Some(proj) => Arc::new(full_schema.project(proj)?),
                None => full_schema.clone(),
            };
            if let Some(idx) = row_id_output_index {
                let mut fields: Vec<Field> =
                    base.fields().iter().map(|f| f.as_ref().clone()).collect();
                fields[idx] = Field::new(crate::ROW_ID_COLUMN_NAME, DataType::Int64, false);
                Arc::new(Schema::new(fields))
            } else {
                base
            }
        };

        // Read projection = output columns (minus ___row_id) + predicate columns for evaluator.
        let read_projection: Option<Vec<usize>> = if self.config.emit_row_ids {
            let output_cols: Vec<usize> = match projection {
                Some(proj) => proj
                    .iter()
                    .filter(|&&idx| Some(idx) != row_id_col_in_full_schema)
                    .copied()
                    .collect(),
                None => (0..full_schema.fields().len())
                    .filter(|&idx| Some(idx) != row_id_col_in_full_schema)
                    .collect(),
            };
            let mut cols = output_cols;
            for &idx in &self.config.predicate_columns {
                if !cols.contains(&idx) {
                    cols.push(idx);
                }
            }
            cols.sort();
            Some(cols)
        } else if self.config.predicate_columns.is_empty() {
            projection.cloned()
        } else {
            projection.map(|proj| {
                let mut cols = proj.clone();
                for &idx in &self.config.predicate_columns {
                    if !cols.contains(&idx) {
                        cols.push(idx);
                    }
                }
                cols.sort();
                cols
            })
        };

        let projected_schema = output_schema;

        // Ignore DataFusion's `filters` argument. The `index_filter(...)`
        // UDF call would be in there (its body panics), and the
        // BoolNode tree held by the evaluator factory already contains
        // the full WHERE semantics.
        //
        // The pushdown predicate — the parquet-native residual to hand
        // to `ParquetSource::with_predicate` — is derived from the
        // BoolNode in `execute_indexed_query` and stashed on the
        // config by that caller.
        let predicate = self.config.pushdown_predicate.clone();

        // Row-group-aligned partition assignments
        let layouts: Vec<SegmentLayout> = self
            .config
            .segments
            .iter()
            .map(|seg| SegmentLayout {
                row_groups: seg.row_groups.clone(),
            })
            .collect();

        // Decide whether to use the sort-aware path: requires a configured
        // index sort, segments that chain on the lead sort key (per-segment
        // min/max are disjoint), `target_partitions >= num_segments` so the
        // optimizer's chain check at `file_scan_config.rs:551` would accept,
        // and that we're not on the QTF row-id-emit path (gated for v1).
        let target_partitions = self.config.query_config.target_partitions.max(1);
        let chain_ok = !self.config.sort_fields.is_empty()
            && !self.config.emit_row_ids
            && segments_chain_on_sort_key(&self.config.segments)
            && target_partitions >= self.config.segments.len();

        // Build the LexOrdering against the projected schema. If the lead
        // sort field is projected away, this returns None and we fall back to
        // the row-count partitioning. Mirror of `create_ordering` at
        // `physical-expr/src/physical_expr.rs:134` — break on first
        // unresolvable column rather than erroring out.
        let lex_ordering = if chain_ok {
            build_projected_lex_ordering(
                &projected_schema,
                &self.config.sort_fields,
                &self.config.sort_orders,
            )
        } else {
            None
        };

        let (assignments, eq_properties, advertised_ordering) = if chain_ok
            && lex_ordering.is_some()
        {
            let assignments = compute_assignments_one_per_segment(&self.config.segments, &layouts);
            let lex = lex_ordering.unwrap();
            let eq = EquivalenceProperties::new_with_orderings(
                projected_schema.clone(),
                vec![lex.clone()],
            );
            (assignments, eq, Some(lex))
        } else {
            let assignments = compute_assignments(&layouts, target_partitions);
            (
                assignments,
                EquivalenceProperties::new(projected_schema.clone()),
                None,
            )
        };

        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(assignments.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        // Surface the sort-aware-path decision as a counter so it's visible in
        // EXPLAIN ANALYZE / `metrics()` output: 1 when ordering was advertised
        // (chain held + lead column projected + non-QTF), 0 otherwise.
        let metrics = ExecutionPlanMetricsSet::new();
        let ordering_optimized: Count =
            MetricBuilder::new(&metrics).global_counter("ordering_optimized");
        if advertised_ordering.is_some() {
            ordering_optimized.add(1);
        }

        // ── Activation-chain diagnostics (timestamp fast-path / QTF top-K) ──
        //
        // Surface every planning-time condition in the per-row-group top-K
        // truncation activation chain as a profile-visible metric, so a
        // `profile:true` run shows exactly where the fast path did (or did not)
        // engage — without any per-row logging. Emitted here AND re-emitted in
        // `clone_with_dynamic_filters` so the counters survive a TopK dynamic
        // filter pushdown (which rebuilds the node with a fresh metrics set).
        record_activation_diagnostics(&metrics, &self.config);

        Ok(Arc::new(QueryShardExec {
            config: Arc::clone(&self.config),
            full_schema,
            projected_schema,
            projection: read_projection,
            assignments,
            properties,
            predicate,
            metrics,
            inner_parquet_metrics: Arc::new(std::sync::Mutex::new(Vec::new())),
            io_stats: Arc::new(ReadIoStats::default()),
            row_id_output_index,
            dynamic_filters: Vec::new(),
            advertised_ordering,
        }))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

// ── QueryShardExec ───────────────────────────────────────────────────

/// One execution plan per query. Partitions into `assignments.len()` streams,
/// each backed by one or more `IndexedExec`s (chained per-chunk).
pub struct QueryShardExec {
    config: Arc<IndexedTableConfig>,
    full_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    assignments: Vec<PartitionAssignment>,
    properties: Arc<PlanProperties>,
    /// Residual physical predicate pushed down from the planner. Threaded
    /// into each `IndexedExec` so `ParquetSource.with_predicate(...)` can
    /// apply it during decode.
    predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    metrics: ExecutionPlanMetricsSet,
    inner_parquet_metrics: Arc<std::sync::Mutex<Vec<MetricsSet>>>,
    io_stats: Arc<ReadIoStats>,
    /// Column index in the OUTPUT schema where computed `___row_id` should be
    /// injected. `None` means no row ID computation (normal data path).
    row_id_output_index: Option<usize>,
    /// Runtime dynamic filters accepted from a parent operator (typically a
    /// `SortExec`-TopK `DynamicFilterPhysicalExpr`) via physical filter
    /// pushdown. Each is read per row-group at execution time to prune RGs
    /// whose parquet statistics cannot satisfy the (tightening) predicate.
    /// Empty unless `handle_child_pushdown_result` accepted one.
    ///
    /// These reference only the SORT columns, never the WHERE clause — so they
    /// are orthogonal to the Lucene/parquet boolean split. See
    /// `docs/dynamic-filters-indexed-table-impl.md` §4b.
    dynamic_filters: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Sort ordering this scan claims to produce — `Some` only when the
    /// sort-aware partitioning fired (chain holds + lead column is in the
    /// projected schema). Each `IndexedExec` spawned by `execute()` advertises
    /// the same ordering so DataFusion's `EnforceSorting` can substitute
    /// `SortPreservingMergeExec` for the outer `SortExec(TopK)`.
    advertised_ordering: Option<LexOrdering>,
}

impl fmt::Debug for QueryShardExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryShardExec")
            .field("partitions", &self.assignments.len())
            .field("segments", &self.config.segments.len())
            .finish()
    }
}

impl DisplayAs for QueryShardExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> fmt::Result {
        // `ordering=` shows whether the sort-aware path fired:
        //   - `unsorted`: no `index.sort.field`, OR sort field set but the
        //     sort-aware path didn't fire (chain didn't hold, target_partitions
        //     too low, lead column projected away, or QTF row-id-emit gate).
        //   - `sorted=[<col> ASC|DESC, ...]`: chain held, output_ordering
        //     advertised → DataFusion can substitute SortPreservingMergeExec.
        write!(
            f,
            "QueryShardExec: partitions={}, segments={}, ordering={}",
            self.assignments.len(),
            self.config.segments.len(),
            describe_ordering(&self.advertised_ordering),
        )
    }
}

fn describe_ordering(ordering: &Option<LexOrdering>) -> String {
    match ordering {
        None => "unsorted".to_string(),
        Some(lex) => {
            use std::fmt::Write;
            let mut s = String::from("sorted=[");
            for (i, e) in lex.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                let dir = if e.options.descending { "DESC" } else { "ASC" };
                let _ = write!(&mut s, "{} {}", e.expr, dir);
            }
            s.push(']');
            s
        }
    }
}

impl ExecutionPlan for QueryShardExec {
    fn name(&self) -> &str {
        "QueryShardExec"
    }
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
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
        let mut combined = self.metrics.clone_inner();
        if let Ok(inner) = self.inner_parquet_metrics.lock() {
            for set in inner.iter() {
                for m in set.iter() {
                    let name = m.value().name();
                    if name == "output_rows" || name == "output_batches" || name == "output_bytes" {
                        continue;
                    }
                    combined.push(m.clone());
                }
            }
        }
        Some(combined)
    }

    /// Accept runtime dynamic filters (TopK / join) pushed from a parent.
    ///
    /// `QueryShardExec` is a leaf (no children), so the default
    /// `gather_filters_for_pushdown` already returns an empty description — the
    /// parent's self-filter arrives here as `parent_filters`. We accept a filter
    /// only in the `Post` phase and only when every column it references is a
    /// readable parquet column in our schema (so per-RG statistics pruning is
    /// possible). Anything else is declined, leaving the parent's safety-net
    /// `FilterExec` in place — declining is always correctness-safe.
    fn handle_child_pushdown_result(
        &self,
        phase: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase,
        child_pushdown_result: datafusion::physical_plan::filter_pushdown::ChildPushdownResult,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<
        datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation<
            Arc<dyn ExecutionPlan>,
        >,
    > {
        use datafusion::physical_plan::filter_pushdown::{
            FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
        };

        // Only the Post phase carries dynamic filters; in Pre we own static
        // WHERE semantics via the BoolNode tree and want no interference.
        if phase != FilterPushdownPhase::Post {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }

        let mut statuses = Vec::with_capacity(child_pushdown_result.parent_filters.len());
        let mut accepted: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = Vec::new();
        for f in &child_pushdown_result.parent_filters {
            if self.dynamic_filter_is_acceptable(&f.filter) {
                statuses.push(PushedDown::Yes);
                accepted.push(Arc::clone(&f.filter));
            } else {
                statuses.push(PushedDown::No);
            }
        }

        if accepted.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                statuses,
            ));
        }

        let new_self = self.clone_with_dynamic_filters(accepted);
        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(statuses)
                .with_updated_node(Arc::new(new_self) as Arc<dyn ExecutionPlan>),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let assignment = self.assignments.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!("partition {} out of range", partition))
        })?;

        let pmetrics = PartitionMetrics::new(&self.metrics, partition);
        let mut stream_metrics =
            pmetrics.into_stream_metrics(Some(Arc::clone(&self.inner_parquet_metrics)));
        stream_metrics.io_stats = Some(Arc::clone(&self.io_stats));

        let dynamic_filter: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = (!self
            .dynamic_filters
            .is_empty())
        .then(|| datafusion::physical_expr::utils::conjunction(self.dynamic_filters.clone()));

        let mut streams: Vec<SendableRecordBatchStream> =
            Vec::with_capacity(assignment.chunks.len());
        for chunk in &assignment.chunks {
            let segment = self.config.segments.get(chunk.segment_idx).ok_or_else(|| {
                DataFusionError::Internal(format!("segment_idx {} out of range", chunk.segment_idx))
            })?;

            let rg_set: HashSet<usize> = chunk.row_group_indices.iter().copied().collect();
            let row_groups: Vec<RowGroupInfo> = segment
                .row_groups
                .iter()
                .filter(|rg| rg_set.contains(&rg.index))
                .cloned()
                .collect();

            if row_groups.is_empty() {
                continue;
            }

            // Build stats prune tree for segment/RG/subtree-level pruning.
            let stats_prune_tree =
                self.config
                    .prune_tree_config
                    .as_ref()
                    .map(|(tree, preds, schema)| {
                        let rg_indices: Vec<usize> = row_groups.iter().map(|rg| rg.index).collect();
                        Arc::new(StatsPruneTree::build_from_bool_node(
                            tree,
                            preds,
                            &segment.metadata,
                            schema,
                            &rg_indices,
                            &segment.arrow_schema,
                        ))
                    });

            // Segment-level skip: if no RG in the chunk can match, skip entirely.
            if let Some(ref spt) = stats_prune_tree {
                if !spt.rg_can_match.iter().any(|&k| k) {
                    native_bridge_common::log_debug!(
                        "[segment-skip] skipping chunk — pruned by segment-level stats"
                    );
                    continue;
                }
            }

            let evaluator = (self.config.evaluator_factory)(
                segment,
                chunk,
                &stream_metrics,
                stats_prune_tree.as_ref(),
            )
            .map_err(|e| DataFusionError::External(e.into()))?;

            // When the sort-aware path fired (`advertised_ordering: Some`), the
            // chain-aware partitioning guarantees this chunk is one whole
            // segment, and the writer's k-way-merge guarantees rows in this
            // segment are in lead-key order. So this `IndexedExec` produces a
            // sorted run and we advertise the same ordering as the parent
            // `QueryShardExec`. Without this, `EnforceSorting` can't see that
            // input partitions are already sorted and won't substitute
            // `SortPreservingMergeExec` for the outer `SortExec(TopK)`.
            let exec_eq_props = match &self.advertised_ordering {
                Some(lex) => EquivalenceProperties::new_with_orderings(
                    self.projected_schema.clone(),
                    vec![lex.clone()],
                ),
                None => EquivalenceProperties::new(self.projected_schema.clone()),
            };
            let props = Arc::new(PlanProperties::new(
                exec_eq_props,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));

            let within_rgs: HashSet<usize> = self
                .config
                .sort_range_within_rgs
                .as_ref()
                .and_then(|map| map.get(&chunk.segment_idx))
                .cloned()
                .unwrap_or_default();

            // Dedicated Top-K WITHIN set for this segment — drives per-RG
            // candidate truncation, independent of the strict `within_rgs`
            // count set above (which stays tied to the count shortcut).
            let topk_within_rgs: HashSet<usize> = self
                .config
                .topk_range_within_rgs
                .as_ref()
                .and_then(|map| map.get(&chunk.segment_idx))
                .cloned()
                .unwrap_or_default();

            let timestamp_within_rgs: HashSet<usize> = self
                .config
                .timestamp_within_rgs
                .as_ref()
                .and_then(|map| map.get(&chunk.segment_idx))
                .cloned()
                .unwrap_or_default();

            let exec = IndexedExec {
                schema: self.projected_schema.clone(),
                full_schema: self.full_schema.clone(),
                object_path: segment.object_path.clone(),
                file_size: segment.parquet_size,
                store: Arc::clone(&self.config.store),
                store_url: self.config.store_url.clone(),
                row_groups,
                projection: self.projection.clone(),
                properties: props,
                metadata: Arc::clone(&segment.metadata),
                predicate: self.predicate.clone(),
                evaluator: std::sync::Mutex::new(Some(evaluator)),
                doc_range: Some((chunk.doc_min, chunk.doc_max)),
                metrics: ExecutionPlanMetricsSet::new(),
                stream_metrics: stream_metrics.clone(),
                query_config: Arc::clone(&self.config.query_config),
                global_base: segment.global_base,
                emit_row_ids: self.config.emit_row_ids,
                row_id_output_index: self.row_id_output_index,
                dynamic_filter: dynamic_filter.clone(),
                cancellation_token: self.config.cancellation_token.clone(),
                seg_arrow_schema: segment.arrow_schema.clone(),
                within_rgs,
                topk_within_rgs,
                timestamp_within_rgs,
                predicate_sans_sort_range: self.config.pushdown_predicate_sans_sort_range.clone(),
                // Per-RG top-K candidate truncation is sound under QTF
                // (`emit_row_ids`) too: it shrinks the post-match candidate
                // bitmap to at most `budget` set bits per WITHIN row group
                // BEFORE the RowSelection / PositionMap / decode are built
                // (see `IndexedStream::poll` and `row_id_injection`). Surviving
                // bits keep their true storage positions, so position-derived
                // `__row_id__` values stay exact. Budget equals the global
                // fetch K and truncation is limited to a single leading-sort-key
                // top-K over WITHIN row groups (see `indexed_executor`), so the
                // global top-K is preserved — identical to the non-QTF path.
                sort_topk_truncate: self.config.sort_topk_truncate,
            };
            streams.push(exec.execute(0, Arc::clone(&context))?);
        }

        let stream: SendableRecordBatchStream = match streams.len() {
            0 => {
                let empty =
                    datafusion::physical_plan::empty::EmptyExec::new(self.projected_schema.clone());
                empty.execute(0, context)?
            }
            1 => streams.into_iter().next().unwrap(),
            _ => {
                let schema = self.projected_schema.clone();
                let chained = futures::stream::iter(streams).flatten();
                Box::pin(RecordBatchStreamAdapter::new(schema, chained))
            }
        };
        Ok(stream)
    }
}

impl Drop for QueryShardExec {
    fn drop(&mut self) {
        accumulate_from_exec(&self.metrics, &self.inner_parquet_metrics, &self.io_stats);
    }
}

impl QueryShardExec {
    /// True if `filter` can be used for per-RG statistics pruning: every column
    /// it references must exist in our full (parquet) schema. Dynamic filters
    /// reference sort columns; a sort on a Lucene-only / computed column (not in
    /// the parquet file) is declined so we never try to prune on absent stats.
    ///
    /// Conservative by design — a `false` here just keeps the parent's
    /// `FilterExec` and forgoes the optimization; it can never drop a row.
    fn dynamic_filter_is_acceptable(
        &self,
        filter: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    ) -> bool {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        use datafusion::physical_expr::expressions::Column;

        let mut all_cols_known = true;
        let mut saw_column = false;
        let _ = filter.apply(|e| {
            if let Some(c) = e.downcast_ref::<Column>() {
                saw_column = true;
                if self.full_schema.index_of(c.name()).is_err() {
                    all_cols_known = false;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        // Require at least one resolved column — a column-free predicate (e.g.
        // the bare `true` placeholder) carries no pruning signal.
        saw_column && all_cols_known
    }

    /// The indexed configuration this exec scans (segments, evaluator
    /// factory, fast-path metadata). Used by the histogram plan rewrite.
    pub(crate) fn indexed_config(&self) -> &Arc<IndexedTableConfig> {
        &self.config
    }

    /// Row-group-aligned partition assignments. Used by the histogram plan
    /// rewrite to mirror partitioning in its counts operator.
    pub(crate) fn assignments(&self) -> &[PartitionAssignment] {
        &self.assignments
    }

    /// Clone of this exec with the given per-segment row groups REMOVED from
    /// every assignment (empty chunks dropped, partition count preserved).
    /// Used by the histogram rewrite: interior row groups move to
    /// `HistogramCountsExec`; the remainder keeps the decode path.
    pub(crate) fn with_row_groups_removed(
        &self,
        remove: &std::collections::HashMap<usize, HashSet<usize>>,
    ) -> Self {
        let assignments: Vec<PartitionAssignment> = self
            .assignments
            .iter()
            .map(|assignment| PartitionAssignment {
                chunks: assignment
                    .chunks
                    .iter()
                    .filter_map(|chunk| {
                        let kept: Vec<usize> = match remove.get(&chunk.segment_idx) {
                            Some(gone) => chunk
                                .row_group_indices
                                .iter()
                                .copied()
                                .filter(|rg| !gone.contains(rg))
                                .collect(),
                            None => chunk.row_group_indices.clone(),
                        };
                        (!kept.is_empty()).then(|| SegmentChunk {
                            segment_idx: chunk.segment_idx,
                            doc_min: chunk.doc_min,
                            doc_max: chunk.doc_max,
                            row_group_indices: kept,
                        })
                    })
                    .collect(),
            })
            .collect();
        QueryShardExec {
            config: Arc::clone(&self.config),
            full_schema: self.full_schema.clone(),
            projected_schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
            assignments,
            properties: Arc::clone(&self.properties),
            predicate: self.predicate.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            inner_parquet_metrics: Arc::clone(&self.inner_parquet_metrics),
            io_stats: Arc::clone(&self.io_stats),
            row_id_output_index: self.row_id_output_index,
            dynamic_filters: self.dynamic_filters.clone(),
            advertised_ordering: self.advertised_ordering.clone(),
        }
    }

    /// Rebuild this exec with accepted dynamic filters attached. `QueryShardExec`
    /// holds a non-`Clone` `ExecutionPlanMetricsSet`; we mint a fresh one (the
    /// pushdown rewrite happens before execution, so no metrics are lost) and
    /// reuse the shared `Arc` fields verbatim.
    fn clone_with_dynamic_filters(
        &self,
        dynamic_filters: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    ) -> Self {
        // Rebuilt node gets a fresh metrics set; re-emit the activation-chain
        // diagnostics so a TopK dynamic-filter pushdown (which produces this
        // clone) does not drop the `activation_*` counters from the profile.
        let metrics = ExecutionPlanMetricsSet::new();
        record_activation_diagnostics(&metrics, &self.config);
        QueryShardExec {
            config: Arc::clone(&self.config),
            full_schema: self.full_schema.clone(),
            projected_schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
            assignments: self.assignments.clone(),
            properties: Arc::clone(&self.properties),
            predicate: self.predicate.clone(),
            metrics,
            inner_parquet_metrics: Arc::clone(&self.inner_parquet_metrics),
            io_stats: Arc::clone(&self.io_stats),
            row_id_output_index: self.row_id_output_index,
            dynamic_filters,
            advertised_ordering: self.advertised_ordering.clone(),
        }
    }
}

#[cfg(test)]
impl QueryShardExec {
    /// Test-only accessor for the conjoined physical predicate produced
    /// by `scan()`. `None` when no filters were pushed down.
    pub(crate) fn test_predicate(
        &self,
    ) -> Option<&Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
        self.predicate.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use datafusion::prelude::SessionContext;

    fn empty_config() -> IndexedTableConfig {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        IndexedTableConfig {
            schema,
            segments: Vec::new(),
            store: Arc::new(object_store::local::LocalFileSystem::new()),
            store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
            // Evaluator factory would never be invoked for this test (no segments).
            evaluator_factory: Arc::new(|_, _, _, _| unreachable!()),
            pushdown_predicate: None,
            query_config: std::sync::Arc::new(
                crate::datafusion_query_config::DatafusionQueryConfig::test_default(),
            ),
            predicate_columns: vec![],
            emit_row_ids: false,
            prune_tree_config: None,
            sort_fields: vec![],
            sort_orders: vec![],
            sort_range_within_rgs: None,
            topk_range_within_rgs: None,
            timestamp_within_rgs: None,
            pushdown_predicate_sans_sort_range: None,
            sort_topk_truncate: None,
            activation_diagnostics: Default::default(),
            cancellation_token: None,
        }
    }

    // QueryShardExec holds an ExecutionPlanMetricsSet (not Clone). We only
    // need to inspect `.predicate`, so read through a reference.
    async fn scan_predicate(
        provider: &IndexedTableProvider,
        filters: &[Expr],
    ) -> Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
        let ctx = SessionContext::new();
        let plan = provider
            .scan(&ctx.state(), None, filters, None)
            .await
            .expect("scan");
        let shard = plan
            .downcast_ref::<QueryShardExec>()
            .expect("scan returns QueryShardExec");
        shard.test_predicate().cloned()
    }

    #[tokio::test]
    async fn scan_with_no_filters_produces_none_predicate() {
        let provider = IndexedTableProvider::new(empty_config());
        let pred = scan_predicate(&provider, &[]).await;
        assert!(pred.is_none(), "no filters → no predicate");
    }
}
