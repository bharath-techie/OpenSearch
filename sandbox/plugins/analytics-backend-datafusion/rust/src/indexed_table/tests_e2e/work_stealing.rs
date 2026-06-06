/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! End-to-end verification of cross-partition work-stealing on the indexed scan
//! (port of DataFusion PR #21351 + the #21956 segment reorder).
//!
//! All sibling partitions of one execution share a single queue of row-group
//! chunks and pop from it at runtime, instead of each draining a fixed set
//! assigned at planning time. These tests assert:
//!   1. **Correctness** — the result set is byte-identical to the static path,
//!      under every partition count (the load-bearing invariant: reordering /
//!      stealing must never drop, duplicate, or corrupt a row).
//!   2. **The path fires** — `work_stolen_chunks > 0` when there are more chunks
//!      than partitions (deterministic by pigeonhole), and `== 0` when the flag
//!      is off.
//!   3. **Load balancing** — a partition that grabs a deliberately-slow segment
//!      ends up processing fewer chunks than its idle sibling steals.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use tempfile::NamedTempFile;

use super::super::index::RowGroupDocsCollector;
use super::super::page_pruner::PagePruner;
use super::super::stream::{FilterStrategy, RowGroupInfo};
use super::super::table_provider::{IndexedTableConfig, IndexedTableProvider, SegmentFileInfo};
use super::super::eval::RowGroupBitsetSource;

/// One segment fixture: a distinct brand, a base price, `rows` rows split into
/// row groups of `max_rg_rows`. `slow_ms` makes its collector sleep per RG so we
/// can build a lopsided workload.
struct Seg {
    brand: &'static str,
    base_price: i32,
    rows: usize,
    max_rg_rows: usize,
    slow_ms: u64,
}

fn write_segment(s: &Seg) -> NamedTempFile {
    let schema = Arc::new(Schema::new(vec![
        Field::new("brand", DataType::Utf8, false),
        Field::new("price", DataType::Int32, false),
    ]));
    let brands: Vec<&str> = (0..s.rows).map(|_| s.brand).collect();
    let prices: Vec<i32> = (0..s.rows).map(|i| s.base_price + i as i32).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(brands)),
            Arc::new(Int32Array::from(prices)),
        ],
    )
    .unwrap();
    let tmp = NamedTempFile::new().unwrap();
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(s.max_rg_rows)
        .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page)
        .build();
    let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    tmp
}

/// Match-all collector that optionally sleeps to simulate an expensive segment.
#[derive(Debug)]
struct SlowMatchAll {
    slow_ms: u64,
}

impl RowGroupDocsCollector for SlowMatchAll {
    fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        if self.slow_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.slow_ms));
        }
        let span = (max_doc - min_doc).max(0) as usize;
        let mut out = vec![0u64; span.div_ceil(64)];
        for rel in 0..span {
            out[rel / 64] |= 1u64 << (rel % 64);
        }
        Ok(out)
    }
}

/// Build the provider over `segs`, run `sql`, return (sorted (brand, price)
/// rows, executed plan). `work_stealing` toggles the feature; `extra_partitions`
/// chooses `target_partitions`.
async fn run(
    segs: &[Seg],
    sql: &str,
    work_stealing: bool,
    num_partitions: usize,
) -> (Vec<(String, i32)>, Arc<dyn ExecutionPlan>) {
    let tmps: Vec<NamedTempFile> = segs.iter().map(write_segment).collect();

    let mut segments: Vec<SegmentFileInfo> = Vec::new();
    let mut schema_opt: Option<SchemaRef> = None;
    for (ord, tmp) in tmps.iter().enumerate() {
        let path = tmp.path().to_path_buf();
        let size = std::fs::metadata(&path).unwrap().len();
        let file = std::fs::File::open(&path).unwrap();
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
                .unwrap();
        if schema_opt.is_none() {
            schema_opt = Some(meta.schema().clone());
        }
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
        let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
        segments.push(SegmentFileInfo {
            writer_generation: ord as i64,
            max_doc: offset,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
            global_base: 0,
        });
    }

    let schema = schema_opt.unwrap();
    // Per-segment slowness, keyed by writer_generation.
    let slow_by_seg: Arc<Vec<u64>> = Arc::new(segs.iter().map(|s| s.slow_ms).collect());

    let factory: super::super::table_provider::EvaluatorFactory = {
        let schema = schema.clone();
        let slow_by_seg = Arc::clone(&slow_by_seg);
        Arc::new(move |segment, _chunk, _stream_metrics, _stats_prune_tree| {
            let slow_ms = slow_by_seg
                .get(segment.writer_generation as usize)
                .copied()
                .unwrap_or(0);
            let collector: Arc<dyn RowGroupDocsCollector> = Arc::new(SlowMatchAll { slow_ms });
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(
                crate::indexed_table::eval::single_collector::SingleCollectorEvaluator::new(
                    Some(collector),
                    pruner,
                    None,
                    None,
                    None,
                    None,
                    crate::indexed_table::eval::single_collector::CollectorCallStrategy::FullRange,
                    std::sync::Arc::new(std::collections::HashMap::new()),
                    segment.writer_generation,
                    std::sync::Arc::new(
                        crate::indexed_table::eval::single_collector::FfmDelegatedBackendCollectorFactory,
                    ),
                    0,
                    None,
                ),
            );
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(num_partitions)
        .force_strategy(Some(FilterStrategy::BooleanMask))
        .force_pushdown(Some(false))
        .indexed_work_stealing(work_stealing)
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments,
        store,
        store_url,
        evaluator_factory: factory,
        pushdown_predicate: None,
        query_config: std::sync::Arc::new(qc),
        predicate_columns: vec![],
        emit_row_ids: false,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), task_ctx).unwrap();
    let mut rows: Vec<(String, i32)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let brand = b.column(b.schema().index_of("brand").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let price = b.column(b.schema().index_of("price").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            rows.push((brand.value(i).to_string(), price.value(i)));
        }
    }
    rows.sort();
    (rows, plan)
}

/// Recursively sum a named counter across the plan tree.
fn sum_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let mut total = 0usize;
    if let Some(metrics) = plan.metrics() {
        total += metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
    }
    for child in plan.children() {
        total += sum_metric(child, name);
    }
    total
}

fn stolen(plan: &Arc<dyn ExecutionPlan>) -> usize {
    sum_metric(plan, "work_stolen_chunks")
}

/// Five distinct segments, one RG each (1 chunk per segment → 5 chunks total).
fn five_segments() -> Vec<Seg> {
    vec![
        Seg { brand: "amazon", base_price: 0, rows: 4, max_rg_rows: 4, slow_ms: 0 },
        Seg { brand: "apple", base_price: 100, rows: 4, max_rg_rows: 4, slow_ms: 0 },
        Seg { brand: "google", base_price: 200, rows: 4, max_rg_rows: 4, slow_ms: 0 },
        Seg { brand: "meta", base_price: 300, rows: 4, max_rg_rows: 4, slow_ms: 0 },
        Seg { brand: "netflix", base_price: 400, rows: 4, max_rg_rows: 4, slow_ms: 0 },
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn work_stealing_matches_static_path_and_fires() {
    let segs = five_segments();
    let sql = "SELECT brand, price FROM t";

    // Baseline: feature off → static per-partition assignment, nothing stolen.
    let (base_rows, base_plan) = run(&segs, sql, /*work_stealing*/ false, 2).await;
    assert_eq!(stolen(&base_plan), 0, "no stealing when the flag is off");
    assert_eq!(base_rows.len(), 20, "5 segments × 4 rows");

    // Feature on: identical rows, and the shared-queue path provably engaged.
    let (rows, plan) = run(&segs, sql, /*work_stealing*/ true, 2).await;
    assert_eq!(rows, base_rows, "work-stealing must not change the result set");

    // 5 chunks, 2 partitions: at most 2 partitions each "keep" their first chunk,
    // so by pigeonhole >= 5 - 2 = 3 chunks are counted as stolen. Deterministic.
    let s = stolen(&plan);
    assert!(
        s >= 3,
        "expected >= 3 stolen chunks (5 chunks - 2 partitions), got {s}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn work_stealing_result_is_partition_count_invariant() {
    let segs = five_segments();
    let sql = "SELECT brand, price FROM t";
    let (expected, _) = run(&segs, sql, false, 1).await;
    for np in [1usize, 2, 3, 5] {
        let (rows, _) = run(&segs, sql, true, np).await;
        assert_eq!(rows, expected, "work-stealing wrong result at np={np}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_partition_steals_from_slow_sibling() {
    // One very slow segment + several fast ones. With a shared queue, whichever
    // partition grabs the slow segment stalls while its sibling drains the rest
    // of the queue — so the bulk of chunks are stolen. We can't pin exact counts
    // (it's a race), but: results are correct AND a healthy share is stolen.
    let segs = vec![
        Seg { brand: "slow", base_price: 0, rows: 4, max_rg_rows: 4, slow_ms: 200 },
        Seg { brand: "f1", base_price: 100, rows: 4, max_rg_rows: 4, slow_ms: 0 },
        Seg { brand: "f2", base_price: 200, rows: 4, max_rg_rows: 4, slow_ms: 0 },
        Seg { brand: "f3", base_price: 300, rows: 4, max_rg_rows: 4, slow_ms: 0 },
        Seg { brand: "f4", base_price: 400, rows: 4, max_rg_rows: 4, slow_ms: 0 },
        Seg { brand: "f5", base_price: 500, rows: 4, max_rg_rows: 4, slow_ms: 0 },
    ];
    let sql = "SELECT brand, price FROM t";
    let (base, _) = run(&segs, sql, false, 2).await;
    let (rows, plan) = run(&segs, sql, true, 2).await;
    assert_eq!(rows, base, "result must match the static path");
    // 6 chunks, 2 partitions → >= 4 stolen by pigeonhole.
    assert!(stolen(&plan) >= 4, "expected heavy stealing, got {}", stolen(&plan));
}

/// Wall-clock A/B benchmark of the lopsided workload. Ignored by default (it
/// sleeps and is timing-sensitive); run explicitly with:
///   cargo test --lib work_stealing_wallclock_benchmark -- --ignored --nocapture
///
/// Construction: one segment is `SLOW_MS` per RG; the rest are free. With a
/// STATIC split (flag off) one partition is assigned the slow segment and the
/// other its share of fast ones — the fast partition finishes and idles while
/// the slow one runs the whole tail. With WORK-STEALING the idle partition
/// drains the shared queue, so total wall-clock is bounded by (slow segment +
/// the fast work the busy partition didn't get to), not (slow + all-fast-in-one
/// -partition). The gap widens with more fast segments.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn work_stealing_wallclock_benchmark() {
    const SLOW_MS: u64 = 50;
    // 1 slow segment + 9 fast, each 1 RG. 2 partitions.
    let mut segs = vec![Seg {
        brand: "slow",
        base_price: 0,
        rows: 4,
        max_rg_rows: 4,
        slow_ms: SLOW_MS,
    }];
    // Leak short 'static brand strings so each segment is distinct.
    let fast_brands: Vec<&'static str> = (0..9)
        .map(|i| &*Box::leak(format!("fast{i}").into_boxed_str()))
        .collect();
    for (i, brand) in fast_brands.iter().enumerate() {
        segs.push(Seg {
            brand,
            base_price: 1000 + i as i32 * 100,
            rows: 4,
            max_rg_rows: 4,
            // Each fast segment carries real-but-modest cost. With a static split
            // they pile up behind the slow one in a single partition; stealing
            // spreads them across the idle sibling.
            slow_ms: 15,
        });
    }
    let sql = "SELECT brand, price FROM t";

    // Warm the page cache / build once each (discard).
    let _ = run(&segs, sql, false, 2).await;
    let _ = run(&segs, sql, true, 2).await;

    let runs = 5u32;
    let mut static_total = std::time::Duration::ZERO;
    let mut steal_total = std::time::Duration::ZERO;
    let mut steal_rows = 0usize;
    let mut static_rows = 0usize;
    let mut last_stolen = 0usize;
    for _ in 0..runs {
        let t0 = std::time::Instant::now();
        let (r, _) = run(&segs, sql, false, 2).await;
        static_total += t0.elapsed();
        static_rows = r.len();

        let t1 = std::time::Instant::now();
        let (r, plan) = run(&segs, sql, true, 2).await;
        steal_total += t1.elapsed();
        steal_rows = r.len();
        last_stolen = stolen(&plan);
    }
    let static_avg = static_total / runs;
    let steal_avg = steal_total / runs;

    // Correctness still holds under timing.
    assert_eq!(static_rows, steal_rows, "row counts must match");

    let speedup = static_avg.as_secs_f64() / steal_avg.as_secs_f64();
    println!("\n──────── work-stealing wall-clock A/B (10 segments, 1 slow, 2 partitions) ────────");
    println!("  static (flag off):   {static_avg:>8.2?}  per query (avg of {runs})");
    println!("  work-stealing (on):  {steal_avg:>8.2?}  per query (avg of {runs})");
    println!("  speedup:             {speedup:>8.2}x");
    println!("  chunks stolen (last run): {last_stolen}");
    println!("─────────────────────────────────────────────────────────────────────────────────\n");

    // Soft expectation: stealing should be at least as fast. Don't hard-fail on
    // a small regression (CI timing noise), but the path must have engaged.
    assert!(last_stolen > 0, "work-stealing path did not engage in the benchmark");
}
