/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Timestamp-WITHIN count shortcut (row-group granularity).
//!
//! A row group whose sort-column footer stats are fully inside the query's
//! timestamp range (and null-free) has a tautological range residual, so for
//! count-only shapes its answer is the candidate cardinality — no parquet
//! read. This suite proves, over a two-row-group fixture where the range
//! fully covers RG0 and only half of RG1:
//!   - `SELECT count(*)` returns the exact count (shortcut RG + residual RG),
//!   - `rg_count_from_index` fires exactly once (RG0 only),
//!   - disabling the shortcut yields the same count (equivalence).

use std::collections::{HashMap, HashSet};
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

use super::super::eval::predicate_evaluator::PredicateOnlyEvaluator;
use super::super::eval::RowGroupBitsetSource;
use super::super::page_pruner::{PagePruneMetrics, PagePruner};
use super::super::stream::{FilterStrategy, RowGroupInfo};
use super::super::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};

/// 16 rows, `ts` = 0..15, two row groups of 8 (RG0 = [0..7], RG1 = [8..15]),
/// plus a payload column so a real decode is observable.
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

/// `ts <= 11` — fully covers RG0 ([0..7]) and half of RG1 ([8..11] of
/// [8..15]).
fn ts_range_residual() -> Arc<dyn PhysicalExpr> {
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

/// Run `SELECT count(*)` over the fixture through the predicate-only indexed
/// path with `ts <= 11` as residual. `within` supplies the RG indices whose
/// residual is treated as a tautology (the shortcut set).
async fn run_count(within: Option<HashSet<usize>>) -> (i64, usize, usize) {
    let (tmp, schema) = write_ts_fixture();
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::File::open(&path).unwrap();
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

    let segment = SegmentFileInfo {
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
    };

    let residual = ts_range_residual();
    let factory: EvaluatorFactory = {
        let schema = schema.clone();
        let residual = Arc::clone(&residual);
        Arc::new(
            move |segment: &SegmentFileInfo, _chunk, stream_metrics, _stats_prune_tree| {
                let pruner = Arc::new(PagePruner::new(
                    &schema,
                    Arc::clone(&segment.metadata),
                    schema.clone(),
                ));
                let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(PredicateOnlyEvaluator::new(
                    pruner,
                    None,
                    Some(Arc::clone(&residual)),
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
        predicate_columns: vec![0], // ts needed for RG1's residual eval
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        sort_range_within_rgs: within.map(|set| Arc::new(HashMap::from([(0usize, set)]))),
        sort_topk_truncate: None,
        timestamp_within_rgs: None,
        pushdown_predicate_sans_sort_range: None,
        cancellation_token: None,
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
    let shortcut = sum_counter(&plan, "rg_count_from_index");
    let parquet_batches = sum_counter(&plan, "parquet_batches_received");
    (count, shortcut, parquet_batches)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn within_rg_count_comes_from_index_without_parquet_read() {
    // RG0 is WITHIN (shortcut, 8 rows from candidate cardinality); RG1 is a
    // boundary RG evaluated by the normal residual path (4 of 8 rows match).
    let (count, shortcut, parquet_batches) = run_count(Some(HashSet::from([0]))).await;
    assert_eq!(count, 12, "8 shortcut rows + 4 residual rows");
    assert_eq!(shortcut, 1, "exactly RG0 must take the count shortcut");
    assert_eq!(
        parquet_batches, 1,
        "only RG1 may touch parquet; RG0 must not be read"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shortcut_disabled_produces_identical_count() {
    let (count, shortcut, parquet_batches) = run_count(None).await;
    assert_eq!(count, 12, "baseline residual path must agree");
    assert_eq!(shortcut, 0);
    assert_eq!(parquet_batches, 2, "both RGs decode without the shortcut");
}

/// Evaluator whose bitset path panics: proves the WITHIN count path calls
/// `count_docs_range` and never falls back to prefetch for count-only RGs.
/// Records every count call so the whole-chunk single-call mode is
/// observable.
#[derive(Debug)]
struct CountOnlyPoisonEvaluator {
    calls: Arc<std::sync::Mutex<Vec<(i32, i32)>>>,
}

impl super::super::eval::RowGroupBitsetSource for CountOnlyPoisonEvaluator {
    fn prefetch_rg(
        &self,
        _rg: &RowGroupInfo,
        _min_doc: i32,
        _max_doc: i32,
    ) -> Result<Option<super::super::eval::PrefetchedRg>, String> {
        panic!("bitset prefetch must not run for count-only WITHIN row groups");
    }

    fn on_batch_mask(
        &self,
        _rg_state: &dyn std::any::Any,
        _rg_first_row: i64,
        _position_map: &super::super::row_selection::PositionMap,
        _batch_offset: usize,
        _batch_len: usize,
        _batch: &RecordBatch,
    ) -> Result<Option<datafusion::arrow::array::BooleanArray>, String> {
        Ok(None)
    }

    fn count_docs_range(&self, min_doc: i32, max_doc: i32) -> Result<Option<u64>, String> {
        self.calls.lock().unwrap().push((min_doc, max_doc));
        // Pretend 3 matches per 8-row RG span.
        Ok(Some(((max_doc - min_doc) as u64) / 8 * 3))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whole_chunk_within_issues_single_count_call() {
    let (tmp, schema) = write_ts_fixture();
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::File::open(&path).unwrap();
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
    let segment = SegmentFileInfo {
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
    };

    let calls: Arc<std::sync::Mutex<Vec<(i32, i32)>>> = Arc::new(std::sync::Mutex::new(vec![]));
    let factory: EvaluatorFactory = {
        let calls = Arc::clone(&calls);
        Arc::new(move |_segment: &SegmentFileInfo, _chunk, _metrics, _spt| {
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(CountOnlyPoisonEvaluator {
                calls: Arc::clone(&calls),
            });
            Ok(eval)
        })
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
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        // BOTH row groups are WITHIN → whole-chunk single-call mode.
        sort_range_within_rgs: Some(Arc::new(HashMap::from([(
            0usize,
            HashSet::from([0usize, 1usize]),
        )]))),
        sort_topk_truncate: None,
        timestamp_within_rgs: None,
        pushdown_predicate_sans_sort_range: None,
        cancellation_token: None,
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
    // One call covering the whole 16-doc chunk; 3 per 8-doc RG → 6.
    assert_eq!(&*calls.lock().unwrap(), &[(0, 16)]);
    assert_eq!(count, 6);
    assert_eq!(sum_counter(&plan, "rg_count_from_index"), 2);
    assert_eq!(sum_counter(&plan, "parquet_batches_received"), 0);
}

/// Rows-shape top-K over the sorted fixture: per-RG candidate truncation must
/// return the identical top-K result while decoding at most `budget` rows per
/// WITHIN row group.
async fn run_topk(truncate: Option<(bool, usize)>) -> (Vec<i64>, usize) {
    let (tmp, schema) = write_ts_fixture();
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::File::open(&path).unwrap();
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
    let segment = SegmentFileInfo {
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
    };
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
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec![],
        sort_orders: vec![],
        sort_range_within_rgs: Some(Arc::new(HashMap::from([(
            0usize,
            HashSet::from([0usize, 1usize]),
        )]))),
        sort_topk_truncate: truncate,
        timestamp_within_rgs: None,
        pushdown_predicate_sans_sort_range: None,
        cancellation_token: None,
    }));
    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx
        .sql("SELECT ts FROM t ORDER BY ts DESC LIMIT 3")
        .await
        .unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), ctx.task_ctx()).unwrap();
    let mut vals = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..a.len() {
            vals.push(a.value(i));
        }
    }
    let truncated = sum_counter(&plan, "rg_topk_truncated");
    (vals, truncated)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topk_truncation_returns_identical_rows_with_less_decode() {
    // DESC query over ASC storage → keep_last = true; budget = 3.
    let (with_trunc, truncated) = run_topk(Some((true, 3))).await;
    let (baseline, baseline_truncated) = run_topk(None).await;
    assert_eq!(baseline, vec![15, 14, 13]);
    assert_eq!(with_trunc, baseline, "top-K rows must be identical");
    assert_eq!(truncated, 2, "both WITHIN RGs truncated to the budget");
    assert_eq!(baseline_truncated, 0);
}

/// Histogram fast path: rewrite must produce identical (bucket, count)
/// results with the interior row groups answered from stats+counts (no
/// parquet), leaving boundary row groups on the decode path.
async fn run_histogram(rewrite: bool) -> (Vec<(i64, i64)>, usize, usize) {
    use crate::indexed_table::histogram::try_rewrite_histogram;
    let (tmp, schema) = write_ts_fixture();
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::File::open(&path).unwrap();
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
    let segment = SegmentFileInfo {
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
    };
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
        .target_partitions(2)
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
        emit_row_ids: false,
        prune_tree_config: None,
        sort_fields: vec!["ts".to_string()],
        sort_orders: vec!["asc".to_string()],
        sort_range_within_rgs: Some(Arc::new(HashMap::from([(
            0usize,
            HashSet::from([0usize, 1usize]),
        )]))),
        sort_topk_truncate: None,
        timestamp_within_rgs: None,
        pushdown_predicate_sans_sort_range: None,
        cancellation_token: None,
    }));
    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    // ts 0..15, RG size 8, bucket width 8 → RG0 all bucket 0, RG1 all bucket 8.
    let df = ctx
        .sql("SELECT ts / 8 * 8 AS b, count(*) AS c FROM t GROUP BY ts / 8 * 8 ORDER BY b")
        .await
        .unwrap();
    let mut plan = df.create_physical_plan().await.unwrap();
    if rewrite {
        plan = try_rewrite_histogram(plan, "ts");
    }
    let display = format!(
        "{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
    );
    let hist_nodes = display.matches("HistogramCountsExec").count();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), ctx.task_ctx()).unwrap();
    let mut out: Vec<(i64, i64)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let keys = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let cnts = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((keys.value(i), cnts.value(i)));
        }
    }
    let parquet_batches = sum_counter(&plan, "parquet_batches_received");
    (out, hist_nodes, parquet_batches)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn histogram_rewrite_matches_decode_path_without_parquet_reads() {
    let (baseline, base_nodes, _) = run_histogram(false).await;
    assert_eq!(baseline, vec![(0, 8), (8, 8)]);
    assert_eq!(base_nodes, 0);

    let (fast, nodes, parquet_batches) = run_histogram(true).await;
    assert_eq!(nodes, 1, "rewrite must install HistogramCountsExec");
    assert_eq!(fast, baseline, "histogram results must be identical");
    assert_eq!(
        parquet_batches, 0,
        "all row groups are interior → zero parquet decode"
    );
}
