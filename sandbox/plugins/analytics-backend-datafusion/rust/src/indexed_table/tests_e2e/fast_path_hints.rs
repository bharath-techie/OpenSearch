/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! PR2 Stage C3 — the per-RG fast-path match arms, driven end-to-end.
//!
//! These tests construct [`FastPathHints`] directly (no plan sniffing) and
//! derive the per-segment [`RowGroupPlan`] vector with
//! [`plan_row_groups`], exactly as the production path will once the Java
//! planner ships the hints. Over a two-row-group `ts` fixture where RG0 is
//! fully inside the range (WITHIN) and RG1 straddles it, they prove:
//!   (a) `COUNT_ONLY` answers RG0 from the index (no parquet read) and RG1 by
//!       decode — `rg_count_from_index=1`, `rg_full=1`, exact count;
//!   (b) `deleted_doc_filtering_required` forces every RG to `Full`
//!       (`rg_count_from_index=0`) with an identical count;
//!   (c) a strippable shape decodes RG0 with the sort-range stripped and yields
//!       rows identical to the full path (`rg_timestamp_stripped>=1`);
//!   (d) `NONE` hints leave every RG on the `Full` path (`rg_full == #RGs`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Operator;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{MetricType, MetricsSet};
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use tempfile::NamedTempFile;

use crate::fast_path_hints::{FastPathHints, SortRange, FASTPATHHINTS_BYTE_SIZE};
use crate::indexed_table::row_group_plan::{plan_row_groups, RowGroupPlan};

use super::super::eval::predicate_evaluator::PredicateOnlyEvaluator;
use super::super::eval::single_collector::{
    FfmDelegatedBackendCollectorFactory, SingleCollectorEvaluator,
};
use super::super::eval::CollectorCallStrategy;
use super::super::eval::RowGroupBitsetSource;
use super::super::index::{CollectDocsResult, RowGroupDocsCollector};
use super::super::page_pruner::{PagePruneMetrics, PagePruner};
use super::super::stream::{FilterStrategy, RowGroupInfo};
use super::super::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};

/// 16 rows: `ts` = 0..15, `v` = 100..115, two row groups of 8
/// (RG0 = ts[0..7], RG1 = ts[8..15]).
fn write_ts_fixture() -> (NamedTempFile, SchemaRef) {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let ts: Vec<i64> = (0..16).collect();
    let v: Vec<i32> = (100..116).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(Int32Array::from(v)),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(8)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();
    let tmp = NamedTempFile::new().unwrap();
    let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    (tmp, schema)
}

/// Build [`FastPathHints`] via the wire round-trip (the private `flags` field
/// is only settable through `from_ffm_ptr`). `shape_byte`: 0 NONE, 1 COUNT_ONLY.
/// Declared unit = millis so a millisecond physical unit coerces 1:1.
fn make_hints(shape_byte: u8, strippable: bool, lower: i64, upper: i64) -> FastPathHints {
    let mut b = [0u8; FASTPATHHINTS_BYTE_SIZE];
    b[0] = 1; // version
    b[1] = shape_byte;
    b[2] = 0b01 | if strippable { 0b10 } else { 0 }; // range_on_leading_sort_field [+ strippable]
    b[3] = 2; // millis
    b[8..16].copy_from_slice(&lower.to_le_bytes());
    b[16..24].copy_from_slice(&upper.to_le_bytes());
    // SAFETY: fixed 40-byte buffer written per the documented wire layout; `b`
    // is live for the duration of this call.
    unsafe { FastPathHints::from_ffm_ptr(b.as_ptr() as i64) }
}

/// `ts <= 11` — fully covers RG0 (ts[0..7]) and half of RG1 (ts[8..11]).
fn ts_le_11() -> Arc<dyn PhysicalExpr> {
    let ts: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 0));
    let lit: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(11))));
    Arc::new(BinaryExpr::new(ts, Operator::LtEq, lit))
}

fn sum_counter(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
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

/// Load footer metadata + build the row-group list for the fixture file.
fn load_segment(path: &std::path::Path, schema: &SchemaRef) -> SegmentFileInfo {
    let size = std::fs::metadata(path).unwrap().len();
    let file = std::fs::File::open(path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let parquet_meta = meta.metadata().clone();
    let mut rgs = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let n = parquet_meta.row_group(i).num_rows();
        rgs.push(RowGroupInfo {
            index: i,
            first_row: offset,
            num_rows: n,
        });
        offset += n;
    }
    SegmentFileInfo {
        writer_generation: 0,
        max_doc: 16,
        object_path: object_store::path::Path::from(path.to_string_lossy().as_ref()),
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
        arrow_schema: schema.clone(),
        global_base: 0,
        sort_min: None,
        sort_max: None,
    }
}

/// Per-segment plan vector keyed by writer_generation (0), derived from the
/// footer + hints exactly as production will.
fn plans_for(
    segment: &SegmentFileInfo,
    hints: &FastPathHints,
    range: Option<&SortRange>,
    has_deletes: bool,
) -> Arc<HashMap<i64, Vec<RowGroupPlan>>> {
    let plans = plan_row_groups(&segment.metadata, Some(0), hints, range, has_deletes);
    Arc::new(HashMap::from([(0i64, plans)]))
}

/// Run `SELECT count(*)` with the residual `ts <= 11` living in the
/// `PredicateOnlyEvaluator` (BooleanMask, no parquet pushdown — mirrors the
/// count-only shape). Returns `(count, rg_count_from_index, rg_full)`.
async fn run_count(hints: &FastPathHints, has_deletes: bool) -> (i64, usize, usize) {
    let (tmp, schema) = write_ts_fixture();
    let segment = load_segment(tmp.path(), &schema);
    let range = SortRange {
        lower_inclusive: i64::MIN,
        upper_inclusive: 11,
    };
    let row_group_plans = plans_for(&segment, hints, Some(&range), has_deletes);

    let factory: EvaluatorFactory = {
        let schema = schema.clone();
        Arc::new(
            move |segment: &SegmentFileInfo, _chunk, stream_metrics, _spt| {
                let pruner = Arc::new(PagePruner::new(
                    &schema,
                    Arc::clone(&segment.metadata),
                    schema.clone(),
                ));
                let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(PredicateOnlyEvaluator::new(
                    pruner,
                    None,
                    Some(ts_le_11()),
                    Some(PagePruneMetrics::from_stream_metrics(stream_metrics)),
                    None,
                    HashMap::new(),
                ));
                Ok(eval)
            },
        )
    };

    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .force_strategy(Some(FilterStrategy::BooleanMask))
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
        predicate_columns: vec![0],
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        cancellation_token: None,
        row_group_plans,
        sort_column: Some("ts".to_string()),
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), ctx.task_ctx()).unwrap();
    let mut count: i64 = -1;
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        if b.num_rows() > 0 {
            count = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
        }
    }
    (
        count,
        sum_counter(&plan, "rg_count_from_index"),
        sum_counter(&plan, "rg_full"),
    )
}

/// Run `SELECT v FROM t WHERE ts <= 11` with the range as the parquet pushdown
/// predicate (RowSelection + pushdown ON, no evaluator residual) so the
/// `TimestampStripped` RG can drop the range/sort-column safely. Returns
/// `(sorted v values, rg_timestamp_stripped, rg_full)`.
async fn run_select_v(
    row_group_plans: Arc<HashMap<i64, Vec<RowGroupPlan>>>,
) -> (Vec<i32>, usize, usize) {
    let (tmp, schema) = write_ts_fixture();
    let segment = load_segment(tmp.path(), &schema);

    let factory: EvaluatorFactory = {
        let schema = schema.clone();
        Arc::new(
            move |segment: &SegmentFileInfo, _chunk, stream_metrics, _spt| {
                let pruner = Arc::new(PagePruner::new(
                    &schema,
                    Arc::clone(&segment.metadata),
                    schema.clone(),
                ));
                // No evaluator residual: correctness on the straddling RG comes from
                // the parquet pushdown predicate, so the stripped RG can drop `ts`.
                let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(PredicateOnlyEvaluator::new(
                    pruner,
                    None,
                    None,
                    Some(PagePruneMetrics::from_stream_metrics(stream_metrics)),
                    None,
                    HashMap::new(),
                ));
                Ok(eval)
            },
        )
    };

    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .force_strategy(Some(FilterStrategy::RowSelection))
        .indexed_pushdown_filters(true)
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![segment],
        store: Arc::new(object_store::local::LocalFileSystem::new()),
        store_url: ObjectStoreUrl::local_filesystem(),
        evaluator_factory: factory,
        pushdown_predicate: Some(ts_le_11()),
        query_config: std::sync::Arc::new(qc),
        predicate_columns: vec![0],
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        cancellation_token: None,
        row_group_plans,
        sort_column: Some("ts".to_string()),
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT v FROM t WHERE ts <= 11").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), ctx.task_ctx()).unwrap();
    let mut vs: Vec<i32> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            vs.push(col.value(i));
        }
    }
    vs.sort_unstable();
    (
        vs,
        sum_counter(&plan, "rg_timestamp_stripped"),
        sum_counter(&plan, "rg_full"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_only_answers_within_rg_from_index() {
    // COUNT_ONLY: RG0 WITHIN → CountFromIndex (8 rows, no parquet read); RG1
    // straddles → Full (4 rows match ts<=11). Total 12.
    let hints = make_hints(1 /* COUNT_ONLY */, false, i64::MIN, 11);
    let (count, count_from_index, full) = run_count(&hints, false).await;
    assert_eq!(count, 12, "8 index-counted + 4 decoded rows");
    assert_eq!(count_from_index, 1, "exactly RG0 takes the count shortcut");
    assert_eq!(full, 1, "exactly RG1 decodes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deletes_force_full_but_count_unchanged() {
    // deleted_doc_filtering_required => every RG classified Full; the count is
    // still exact via the normal decode path.
    let hints = make_hints(1 /* COUNT_ONLY */, false, i64::MIN, 11);
    let (count, count_from_index, full) = run_count(&hints, true).await;
    assert_eq!(count, 12, "identical count via the delete-safe Full path");
    assert_eq!(count_from_index, 0, "no count shortcut under deletions");
    assert_eq!(full, 2, "both RGs decode");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strippable_shape_matches_full_path() {
    let (tmp, schema) = write_ts_fixture();
    let segment = load_segment(tmp.path(), &schema);
    let range = SortRange {
        lower_inclusive: i64::MIN,
        upper_inclusive: 11,
    };

    // Full-path baseline: no fast-path plans (every RG Full).
    let (baseline, ts_stripped_base, _) = run_select_v(Arc::new(HashMap::new())).await;
    assert_eq!(ts_stripped_base, 0, "baseline strips nothing");
    // ts<=11 keeps rows 0..11 → v = 100..111.
    assert_eq!(baseline, (100..=111).collect::<Vec<i32>>());

    // Strippable shape: RG0 WITHIN → TimestampStripped, RG1 → Full.
    let hints = make_hints(0 /* NONE shape, strippable flag */, true, i64::MIN, 11);
    let plans = plans_for(&segment, &hints, Some(&range), false);
    let (stripped, ts_stripped, full) = run_select_v(plans).await;
    assert_eq!(stripped, baseline, "stripped path yields identical rows");
    assert!(ts_stripped >= 1, "RG0 took the timestamp-stripped path");
    assert_eq!(full, 1, "RG1 stays on the Full path");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn none_hints_keep_every_rg_full() {
    // NONE shape, not strippable => plan_row_groups short-circuits to all-Full.
    let hints = make_hints(0 /* NONE */, false, i64::MIN, 11);
    let (count, count_from_index, full) = run_count(&hints, false).await;
    assert_eq!(count, 12);
    assert_eq!(count_from_index, 0, "no shortcut for NONE hints");
    assert_eq!(full, 2, "rg_full == number of row groups");
}

// ── PR2 Stage C4: docFreq count short-circuit (count_rg / Fix 2/3) ──────────

/// Collector that tracks how it was called and can force `count_docs` to
/// decline. `count_override`:
///   * `None`      → `count_docs` computes the exact count from `docs`;
///   * `Some(v)`   → `count_docs` returns `Ok(v)` (use `Some(None)` to decline).
#[derive(Debug)]
struct CountingCollector {
    docs: Vec<i32>,
    count_docs_calls: Arc<AtomicUsize>,
    collect_calls: Arc<AtomicUsize>,
    count_override: Option<Option<u64>>,
}

impl RowGroupDocsCollector for CountingCollector {
    fn collect_packed_u64_bitset(
        &self,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<CollectDocsResult, String> {
        self.collect_calls.fetch_add(1, Ordering::SeqCst);
        let span = (max_doc - min_doc).max(0) as usize;
        let mut words = vec![0u64; span.div_ceil(64)];
        for &d in &self.docs {
            if d >= min_doc && d < max_doc {
                let idx = (d - min_doc) as usize;
                words[idx / 64] |= 1u64 << (idx % 64);
            }
        }
        Ok(words.into())
    }

    fn count_docs(&self, min_doc: i32, max_doc: i32) -> Result<Option<u64>, String> {
        self.count_docs_calls.fetch_add(1, Ordering::SeqCst);
        match self.count_override {
            Some(v) => Ok(v),
            None => Ok(Some(
                self.docs
                    .iter()
                    .filter(|&&d| d >= min_doc && d < max_doc)
                    .count() as u64,
            )),
        }
    }
}

/// 8 rows, one row group: `ts` = 0..7 (all ≤ 11 → WITHIN), `v` = 100..107.
fn write_ts_fixture_single() -> (NamedTempFile, SchemaRef) {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let ts: Vec<i64> = (0..8).collect();
    let v: Vec<i32> = (100..108).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ts)),
            Arc::new(Int32Array::from(v)),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(64)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();
    let tmp = NamedTempFile::new().unwrap();
    let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    (tmp, schema)
}

/// Like [`load_segment`] but for the single-RG fixture (`max_doc = 8`).
fn load_segment_single(path: &std::path::Path, schema: &SchemaRef) -> SegmentFileInfo {
    let size = std::fs::metadata(path).unwrap().len();
    let file = std::fs::File::open(path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let parquet_meta = meta.metadata().clone();
    let mut rgs = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let n = parquet_meta.row_group(i).num_rows();
        rgs.push(RowGroupInfo {
            index: i,
            first_row: offset,
            num_rows: n,
        });
        offset += n;
    }
    SegmentFileInfo {
        writer_generation: 0,
        max_doc: 8,
        object_path: object_store::path::Path::from(path.to_string_lossy().as_ref()),
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
        arrow_schema: schema.clone(),
        global_base: 0,
        sort_min: None,
        sort_max: None,
    }
}

/// Run `SELECT count(*)` over the single-RG WITHIN fixture through a
/// `SingleCollectorEvaluator` (no residual, no pruning predicate) so the RG is
/// eligible for the `count_rg` docFreq short-circuit. Returns
/// `(count, rg_count_from_docfreq, rg_count_from_index, rg_full)`.
async fn run_count_single_collector(
    collector: Option<Arc<dyn RowGroupDocsCollector>>,
    hints: &FastPathHints,
) -> (i64, usize, usize, usize) {
    let (tmp, schema) = write_ts_fixture_single();
    let segment = load_segment_single(tmp.path(), &schema);
    let range = SortRange {
        lower_inclusive: i64::MIN,
        upper_inclusive: 11,
    };
    let row_group_plans = plans_for(&segment, hints, Some(&range), false);

    let factory: EvaluatorFactory = {
        let schema = schema.clone();
        let collector = collector.clone();
        Arc::new(
            move |segment: &SegmentFileInfo, _chunk, stream_metrics, _spt| {
                let pruner = Arc::new(PagePruner::new(
                    &schema,
                    Arc::clone(&segment.metadata),
                    schema.clone(),
                ));
                let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(SingleCollectorEvaluator::new(
                    collector.clone(),
                    pruner,
                    None, // pruning_predicate
                    None, // residual_expr — count-only, nothing to refine
                    Some(PagePruneMetrics::from_stream_metrics(stream_metrics)),
                    None, // ffm_collector_calls
                    CollectorCallStrategy::FullRange,
                    Arc::new(HashMap::new()),
                    segment.writer_generation,
                    Arc::new(FfmDelegatedBackendCollectorFactory),
                    0,    // context_id
                    None, // bloom
                    None, // stats_prune_tree
                    HashMap::new(),
                    Vec::new(), // performance_leaves
                ));
                Ok(eval)
            },
        )
    };

    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .force_strategy(Some(FilterStrategy::BooleanMask))
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
        predicate_columns: vec![0],
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        cancellation_token: None,
        row_group_plans,
        sort_column: Some("ts".to_string()),
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), ctx.task_ctx()).unwrap();
    let mut count: i64 = -1;
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        if b.num_rows() > 0 {
            count = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
        }
    }
    (
        count,
        sum_counter(&plan, "rg_count_from_docfreq"),
        sum_counter(&plan, "rg_count_from_index"),
        sum_counter(&plan, "rg_full"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_only_within_rg_answered_by_docfreq() {
    // COUNT_ONLY over a WITHIN RG: count_rg → collector.count_docs → 8. No
    // bitmap materialized (collect_packed_u64_bitset never called).
    let count_docs_calls = Arc::new(AtomicUsize::new(0));
    let collect_calls = Arc::new(AtomicUsize::new(0));
    let collector: Arc<dyn RowGroupDocsCollector> = Arc::new(CountingCollector {
        docs: (0..8).collect(),
        count_docs_calls: count_docs_calls.clone(),
        collect_calls: collect_calls.clone(),
        count_override: None,
    });
    let hints = make_hints(1 /* COUNT_ONLY */, false, i64::MIN, 11);
    let (count, docfreq, from_index, full) =
        run_count_single_collector(Some(collector), &hints).await;
    assert_eq!(count, 8, "all 8 rows counted from the index");
    assert_eq!(docfreq, 1, "RG0 answered by the docFreq short-circuit");
    assert_eq!(from_index, 1, "docfreq path also bumps rg_count_from_index");
    assert_eq!(full, 0, "no RG decoded");
    assert_eq!(
        count_docs_calls.load(Ordering::SeqCst),
        1,
        "count_docs called exactly once"
    );
    assert_eq!(
        collect_calls.load(Ordering::SeqCst),
        0,
        "collect_packed_u64_bitset never called — no bitmap materialized"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_docs_none_falls_back_to_candidates() {
    // count_docs declines (Ok(None)) → count_rg declines → prefetch_rg runs and
    // the candidates-based gate answers the RG (rg_count_from_index, not docfreq).
    let count_docs_calls = Arc::new(AtomicUsize::new(0));
    let collect_calls = Arc::new(AtomicUsize::new(0));
    let collector: Arc<dyn RowGroupDocsCollector> = Arc::new(CountingCollector {
        docs: (0..8).collect(),
        count_docs_calls: count_docs_calls.clone(),
        collect_calls: collect_calls.clone(),
        count_override: Some(None), // decline
    });
    let hints = make_hints(1 /* COUNT_ONLY */, false, i64::MIN, 11);
    let (count, docfreq, from_index, full) =
        run_count_single_collector(Some(collector), &hints).await;
    assert_eq!(count, 8, "candidate fallback yields the same count");
    assert_eq!(docfreq, 0, "count_docs declined → no docfreq shortcut");
    assert_eq!(from_index, 1, "candidates-based gate answered RG0");
    assert_eq!(full, 0, "still no full decode (count-only shape)");
    assert_eq!(
        count_docs_calls.load(Ordering::SeqCst),
        1,
        "count_docs consulted once before falling back"
    );
    assert!(
        collect_calls.load(Ordering::SeqCst) >= 1,
        "fell back to bitmap materialization"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_collector_zero_leaf_universe_count() {
    // No correctness collector, no performance leaves: count_rg returns the RG
    // span (universe) since the footer proved WITHIN. Count == num_rows.
    let hints = make_hints(1 /* COUNT_ONLY */, false, i64::MIN, 11);
    let (count, docfreq, from_index, full) = run_count_single_collector(None, &hints).await;
    assert_eq!(count, 8, "universe count == RG num_rows");
    assert_eq!(docfreq, 1, "answered by the docFreq/universe short-circuit");
    assert_eq!(from_index, 1);
    assert_eq!(full, 0);
}
