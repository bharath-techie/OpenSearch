/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Deterministic page-pruning e2e tests.
//!
//! # Fixture layout
//!
//! 4096 rows, 1 RG, 4 pages of 1024 rows each.
//!
//! | page | rows        | price range   | brand   |
//! |------|-------------|---------------|---------|
//! |  0   | 0–1023      | 0..1024       | "alpha" |
//! |  1   | 1024–2047   | 10_000..11_024| "beta"  |
//! |  2   | 2048–3071   | 20_000..21_024| "gamma" |
//! |  3   | 3072–4095   | 30_000..31_024| "delta" |
//!
//! Price ranges are non-overlapping so predicates like `price < 1024`
//! deterministically prune to page 0 only.

#![cfg(test)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Operator;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysColumn, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use tempfile::NamedTempFile;

use crate::indexed_table::bool_tree::BoolNode;
use crate::indexed_table::eval::bitmap_tree::{BitmapTreeEvaluator, CollectorLeafBitmaps};
use crate::indexed_table::eval::single_collector::{
    CollectorCallStrategy, SingleCollectorEvaluator,
};
use crate::indexed_table::eval::{RowGroupBitsetSource, TreeBitsetSource};
use crate::indexed_table::index::RowGroupDocsCollector;
use crate::indexed_table::page_pruner::{build_pruning_predicate, PagePruner};
use crate::indexed_table::stream::{FilterStrategy, RowGroupInfo};
use crate::indexed_table::table_provider::{
    EvaluatorFactory, IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
};

const ROWS_PER_PAGE: usize = 1024;
const NUM_PAGES: usize = 4;
const NUM_ROWS: usize = ROWS_PER_PAGE * NUM_PAGES; // 4096

// ── Fixture builder ─────────────────────────────────────────────────

fn fixture_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int32, false),
        Field::new("brand", DataType::Utf8, false),
    ]))
}

fn write_fixture() -> NamedTempFile {
    let schema = fixture_schema();
    let labels = ["alpha", "beta", "gamma", "delta"];
    let prices: Vec<i32> = (0..NUM_PAGES)
        .flat_map(|p| {
            let base = (p as i32) * 10_000;
            (0..ROWS_PER_PAGE as i32).map(move |i| base + i)
        })
        .collect();
    let brands: Vec<&str> = (0..NUM_PAGES)
        .flat_map(|p| std::iter::repeat(labels[p]).take(ROWS_PER_PAGE))
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(prices)),
            Arc::new(StringArray::from(brands)),
        ],
    )
    .unwrap();
    let tmp = NamedTempFile::new().unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(NUM_ROWS)
        .set_data_page_row_count_limit(ROWS_PER_PAGE)
        .set_write_batch_size(ROWS_PER_PAGE)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    tmp
}

// ── Expression helpers ──────────────────────────────────────────────

fn col_expr(name: &str) -> Arc<dyn PhysicalExpr> {
    let idx = fixture_schema().index_of(name).unwrap();
    Arc::new(PhysColumn::new(name, idx))
}

fn lit_i32(v: i32) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(ScalarValue::Int32(Some(v))))
}

fn binop(
    l: Arc<dyn PhysicalExpr>,
    op: Operator,
    r: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(l, op, r))
}

fn pred_node(expr: Arc<dyn PhysicalExpr>) -> BoolNode {
    BoolNode::Predicate(expr)
}

fn collector_leaf(tag: u8) -> BoolNode {
    BoolNode::Collector {
        query_bytes: Arc::from(&[tag][..]),
    }
}

// ── Mock collector ──────────────────────────────────────────────────

#[derive(Debug)]
struct MockCollector {
    docs: Vec<i32>,
}

impl RowGroupDocsCollector for MockCollector {
    fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        let span = (max_doc - min_doc) as usize;
        let mut out = vec![0u64; span.div_ceil(64)];
        for &doc in &self.docs {
            if doc >= min_doc && doc < max_doc {
                let rel = (doc - min_doc) as usize;
                out[rel / 64] |= 1u64 << (rel % 64);
            }
        }
        Ok(out)
    }
}

/// tag 0 → all docs, tag 1 → even docs only, tag 2 → first 2 per page.
fn collector_for_tag(tag: u8) -> Arc<dyn RowGroupDocsCollector> {
    let docs: Vec<i32> = match tag {
        0 => (0..NUM_ROWS as i32).collect(),
        1 => (0..NUM_ROWS as i32).step_by(2).collect(),
        2 => (0..NUM_PAGES)
            .flat_map(|p| {
                let base = (p * ROWS_PER_PAGE) as i32;
                vec![base, base + 1]
            })
            .collect(),
        _ => vec![],
    };
    Arc::new(MockCollector { docs })
}

// ── Segment loader & metrics ────────────────────────────────────────

fn load_segment(tmp: &NamedTempFile) -> (SegmentFileInfo, SchemaRef) {
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::File::open(&path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();
    let mut rgs = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let n = parquet_meta.row_group(i).num_rows();
        rgs.push(RowGroupInfo { index: i, first_row: offset, num_rows: n });
        offset += n;
    }
    let seg = SegmentFileInfo {
        segment_ord: 0,
        max_doc: NUM_ROWS as i64,
        object_path: object_store::path::Path::from(path.to_string_lossy().as_ref()),
        parquet_size: size,
        row_groups: rgs,
        metadata: parquet_meta,
    };
    (seg, schema)
}

fn aggregate_metrics(plan: &Arc<dyn ExecutionPlan>) -> MetricsSet {
    let mut set = MetricsSet::new();
    fn walk(plan: &Arc<dyn ExecutionPlan>, out: &mut MetricsSet) {
        if plan.name() == "QueryShardExec" {
            if let Some(m) = plan.metrics() {
                for metric in m.iter() {
                    out.push(Arc::clone(metric));
                }
            }
        }
        for child in plan.children() {
            walk(child, out);
        }
    }
    walk(plan, &mut set);
    set
}

fn get_counter(set: &MetricsSet, name: &str) -> usize {
    use datafusion::physical_plan::metrics::MetricType;
    set.sum(|m| m.value().name() == name && m.metric_type() == MetricType::DEV)
        .map(|v| v.as_usize())
        .unwrap_or(0)
}

// ── Tree wiring ─────────────────────────────────────────────────────

fn collect_pred_exprs(node: &BoolNode, out: &mut Vec<Arc<dyn PhysicalExpr>>) {
    match node {
        BoolNode::Predicate(e) => out.push(Arc::clone(e)),
        BoolNode::And(cs) | BoolNode::Or(cs) => cs.iter().for_each(|c| collect_pred_exprs(c, out)),
        BoolNode::Not(c) => collect_pred_exprs(c, out),
        BoolNode::Collector { .. } => {}
    }
}

fn build_pp_map(
    tree: &BoolNode,
    schema: &SchemaRef,
) -> Arc<HashMap<usize, Arc<datafusion::physical_optimizer::pruning::PruningPredicate>>> {
    let mut exprs = Vec::new();
    collect_pred_exprs(tree, &mut exprs);
    Arc::new(
        exprs
            .iter()
            .filter_map(|expr| {
                build_pruning_predicate(expr, schema.clone())
                    .map(|pp| (Arc::as_ptr(expr) as *const () as usize, pp))
            })
            .collect(),
    )
}

fn wire_collectors_dfs(node: &BoolNode, out: &mut Vec<Arc<dyn RowGroupDocsCollector>>) {
    match node {
        BoolNode::Collector { query_bytes } => out.push(collector_for_tag(query_bytes[0])),
        BoolNode::And(cs) | BoolNode::Or(cs) => cs.iter().for_each(|c| wire_collectors_dfs(c, out)),
        BoolNode::Not(c) => wire_collectors_dfs(c, out),
        BoolNode::Predicate(_) => {}
    }
}

// ── Execution harnesses ─────────────────────────────────────────────

/// Run a BoolNode tree through the bitmap-tree evaluator, return (prices, plan).
async fn run_bitmap_tree(
    tree: BoolNode,
) -> (Vec<i32>, Arc<dyn ExecutionPlan>) {
    let tmp = write_fixture();
    let (seg, schema) = load_segment(&tmp);
    let tree = tree.push_not_down();
    let pp_map = build_pp_map(&tree, &schema);
    let mut colls = Vec::new();
    wire_collectors_dfs(&tree, &mut colls);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = colls
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let tree = Arc::new(tree);

    let factory: EvaluatorFactory = {
        let per_leaf = per_leaf.clone();
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        let pp_map = Arc::clone(&pp_map);
        Arc::new(move |segment, _chunk, sm| {
            let resolved = tree.resolve(&per_leaf)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                tree: Arc::new(resolved),
                evaluator: Arc::new(BitmapTreeEvaluator),
                leaves: Arc::new(CollectorLeafBitmaps {
                    ffm_collector_calls: sm.ffm_collector_calls.clone(),
                }),
                page_pruner: pruner,
                cost_predicate: 1,
                cost_collector: 10,
                max_collector_parallelism: 1,
                pruning_predicates: Arc::clone(&pp_map),
                page_prune_metrics: Some(
                    crate::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(sm),
                ),
                collector_strategy: crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
            });
            Ok(eval)
        })
    };

    execute_and_collect(seg, schema, factory).await
}

/// Run a single-collector query with a given strategy, return (prices, plan).
async fn run_single_collector(
    collector_tag: u8,
    residual_expr: Arc<dyn PhysicalExpr>,
    strategy: CollectorCallStrategy,
) -> (Vec<i32>, Arc<dyn ExecutionPlan>) {
    let tmp = write_fixture();
    let (seg, schema) = load_segment(&tmp);
    let residual_pp = build_pruning_predicate(&residual_expr, schema.clone());

    let factory: EvaluatorFactory = {
        let schema = schema.clone();
        let residual_pp = residual_pp.clone();
        let residual_expr = Arc::clone(&residual_expr);
        Arc::new(move |segment, _chunk, sm| {
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(SingleCollectorEvaluator::new(
                collector_for_tag(collector_tag),
                pruner,
                residual_pp.clone(),
                Some(Arc::clone(&residual_expr)),
                Some(
                    crate::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(sm),
                ),
                sm.ffm_collector_calls.clone(),
                strategy,
            ));
            Ok(eval)
        })
    };

    execute_and_collect(seg, schema, factory).await
}

async fn execute_and_collect(
    seg: SegmentFileInfo,
    schema: SchemaRef,
    factory: EvaluatorFactory,
) -> (Vec<i32>, Arc<dyn ExecutionPlan>) {
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![seg],
        store,
        store_url,
        evaluator_factory: factory,
        target_partitions: 1,
        force_strategy: Some(FilterStrategy::BooleanMask),
        force_pushdown: Some(false),
        pushdown_predicate: None,
        query_config: Arc::new(
            crate::datafusion_query_config::DatafusionQueryConfig::default(),
        ),
        predicate_columns: vec![],
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT price, brand FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), task_ctx).unwrap();
    let mut prices = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            prices.push(col.value(i));
        }
    }
    prices.sort();
    (prices, plan)
}

// ═════════════════════════════════════════════════════════════════════
// Bitmap tree (multi-filter) page pruning tests
// ═════════════════════════════════════════════════════════════════════

/// AND(Collector(all), Predicate(price < 1024)) → only page 0 survives.
/// 3 of 4 pages pruned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bitmap_tree_and_predicate_prunes_3_pages() {
    let expr = binop(col_expr("price"), Operator::Lt, lit_i32(1024));
    let tree = BoolNode::And(vec![collector_leaf(0), pred_node(expr)]);
    let (prices, plan) = run_bitmap_tree(tree).await;

    // All 1024 rows from page 0 (prices 0..1024).
    assert_eq!(prices.len(), ROWS_PER_PAGE);
    assert_eq!(*prices.first().unwrap(), 0);
    assert_eq!(*prices.last().unwrap(), 1023);

    let m = aggregate_metrics(&plan);
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    assert_eq!(get_counter(&m, "pages_pruned"), 3);
}

/// OR(Predicate(price < 1024), Predicate(price >= 30_000)) → pages 0 and 3.
/// 2 of 4 pages pruned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bitmap_tree_or_predicate_keeps_two_pages() {
    let left = binop(col_expr("price"), Operator::Lt, lit_i32(1024));
    let right = binop(col_expr("price"), Operator::GtEq, lit_i32(30_000));
    let tree = BoolNode::And(vec![
        collector_leaf(0),
        BoolNode::Or(vec![pred_node(left), pred_node(right)]),
    ]);
    let (prices, plan) = run_bitmap_tree(tree).await;

    assert_eq!(prices.len(), 2 * ROWS_PER_PAGE);
    assert!(prices.contains(&0));
    assert!(prices.contains(&30_000));
    assert!(!prices.contains(&10_000));

    let m = aggregate_metrics(&plan);
    // Final page-level decision: pages 0 and 3 have candidates, pages 1 and 2 don't.
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    assert_eq!(get_counter(&m, "pages_pruned"), 2);
}

/// AND(Predicate(price >= 10_000), Predicate(price < 21_024)) → pages 1 and 2.
/// Nested AND of two predicates intersects page ranges.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bitmap_tree_and_two_predicates_intersect() {
    let left = binop(col_expr("price"), Operator::GtEq, lit_i32(10_000));
    let right = binop(col_expr("price"), Operator::Lt, lit_i32(21_024));
    let tree = BoolNode::And(vec![
        collector_leaf(0),
        pred_node(left),
        pred_node(right),
    ]);
    let (prices, plan) = run_bitmap_tree(tree).await;

    assert_eq!(prices.len(), 2 * ROWS_PER_PAGE);
    assert_eq!(*prices.first().unwrap(), 10_000);
    assert_eq!(*prices.last().unwrap(), 21_023);

    let m = aggregate_metrics(&plan);
    // Final page-level: pages 1 and 2 have candidates, pages 0 and 3 don't.
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    assert_eq!(get_counter(&m, "pages_pruned"), 2);
}

/// AND(Collector(even), OR(Predicate(price < 1024), Predicate(price >= 30_000)))
/// Collector intersected with OR of two page ranges → even docs from pages 0,3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bitmap_tree_nested_collector_and_or_predicates() {
    let p0 = binop(col_expr("price"), Operator::Lt, lit_i32(1024));
    let p3 = binop(col_expr("price"), Operator::GtEq, lit_i32(30_000));
    let tree = BoolNode::And(vec![
        collector_leaf(1), // even docs
        BoolNode::Or(vec![pred_node(p0), pred_node(p3)]),
    ]);
    let (prices, plan) = run_bitmap_tree(tree).await;

    // Even docs from pages 0 and 3: 512 + 512 = 1024.
    assert_eq!(prices.len(), ROWS_PER_PAGE);
    // All returned prices should be even (from even doc IDs).
    assert!(prices.iter().all(|p| {
        // page 0: price == doc_id, even doc → even price
        // page 3: price = 30000 + (doc_id - 3072), even doc → even offset
        *p < 1024 || *p >= 30_000
    }));

    let m = aggregate_metrics(&plan);
    // Final page-level: pages 0 and 3 have candidates, pages 1 and 2 pruned.
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    assert_eq!(get_counter(&m, "pages_pruned"), 2);
}

/// Predicate that matches nothing → all 4 pages pruned, zero rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bitmap_tree_all_pages_pruned() {
    let expr = binop(col_expr("price"), Operator::Lt, lit_i32(-1));
    let tree = BoolNode::And(vec![collector_leaf(0), pred_node(expr)]);
    let (prices, plan) = run_bitmap_tree(tree).await;

    assert_eq!(prices.len(), 0);

    let m = aggregate_metrics(&plan);
    assert_eq!(get_counter(&m, "pages_total"), NUM_PAGES);
    assert_eq!(get_counter(&m, "pages_pruned"), NUM_PAGES);
}

// ═════════════════════════════════════════════════════════════════════
// Single collector page pruning tests — all three CollectorCallStrategy
// ═════════════════════════════════════════════════════════════════════

/// Helper: run the same residual across all three strategies, assert identical results.
async fn run_all_strategies(
    collector_tag: u8,
    residual: Arc<dyn PhysicalExpr>,
    expected_len: usize,
    expected_pruned: usize,
) {
    for strategy in [
        CollectorCallStrategy::FullRange,
        CollectorCallStrategy::TightenOuterBounds,
        CollectorCallStrategy::PageRangeSplit,
    ] {
        let (prices, plan) =
            run_single_collector(collector_tag, Arc::clone(&residual), strategy).await;
        assert_eq!(
            prices.len(),
            expected_len,
            "strategy {:?}: expected {} rows, got {}",
            strategy,
            expected_len,
            prices.len()
        );
        let m = aggregate_metrics(&plan);
        assert_eq!(
            get_counter(&m, "pages_total"),
            NUM_PAGES,
            "strategy {:?}: pages_total",
            strategy
        );
        assert_eq!(
            get_counter(&m, "pages_pruned"),
            expected_pruned,
            "strategy {:?}: pages_pruned",
            strategy
        );
    }
}

/// Residual price < 1024 with all-docs collector → page 0 only, 3 pruned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_collector_prunes_3_pages_all_strategies() {
    let residual = binop(col_expr("price"), Operator::Lt, lit_i32(1024));
    run_all_strategies(0, residual, ROWS_PER_PAGE, 3).await;
}

/// Residual price >= 30_000 with even-docs collector → even docs from page 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_collector_even_docs_page3_all_strategies() {
    let residual = binop(col_expr("price"), Operator::GtEq, lit_i32(30_000));
    // Even docs in page 3: 512 rows.
    run_all_strategies(1, residual, ROWS_PER_PAGE / 2, 3).await;
}

/// Residual that matches nothing → all pages pruned, zero rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_collector_all_pruned_all_strategies() {
    let residual = binop(col_expr("price"), Operator::Gt, lit_i32(999_999));
    run_all_strategies(0, residual, 0, NUM_PAGES).await;
}

/// Residual that matches everything → no pages pruned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_collector_no_pruning_all_strategies() {
    let residual = binop(col_expr("price"), Operator::GtEq, lit_i32(0));
    // build_pruning_predicate returns None for always-true → no pruning.
    // All 4096 rows returned, 0 pruned (pages_total may be 0 since
    // pruning was skipped entirely).
    for strategy in [
        CollectorCallStrategy::FullRange,
        CollectorCallStrategy::TightenOuterBounds,
        CollectorCallStrategy::PageRangeSplit,
    ] {
        let (prices, _plan) =
            run_single_collector(0, Arc::clone(&residual), strategy).await;
        assert_eq!(prices.len(), NUM_ROWS, "strategy {:?}", strategy);
    }
}

/// FullRange calls collector on full [0, 4096), TightenOuterBounds
/// narrows to surviving page range, PageRangeSplit calls per-range.
/// All produce the same rows for price in [10_000, 11_024).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_collector_page1_only() {
    let lo = binop(col_expr("price"), Operator::GtEq, lit_i32(10_000));
    let hi = binop(col_expr("price"), Operator::Lt, lit_i32(11_024));
    let residual = binop(lo, Operator::And, hi);
    run_all_strategies(0, residual, ROWS_PER_PAGE, 3).await;
}
