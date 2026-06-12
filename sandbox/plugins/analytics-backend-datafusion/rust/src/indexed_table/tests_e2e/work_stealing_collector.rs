/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Collector-granular work-stealing tests.
//!
//! The sibling file `work_stealing.rs` proves the shared-queue path fires and is
//! result-correct, but only over **one-row-group-per-segment** fixtures — so a
//! segment is never partitioned into more than one chunk, and its collector
//! (`SlowMatchAll`) is trivially thread-safe. That leaves the load-bearing
//! invariant of this feature untested:
//!
//! > The real unit of stealable work is a **collector**, not a segment or a row
//! > group. A collector is created for one specific `(segment, doc_range)` chunk.
//! > When a segment is partitioned into several chunks, each chunk gets its **own**
//! > collector with its own `doc_min`/`doc_max`. Two chunks of the same segment
//! > are therefore two *different* collectors and may be advanced on different
//! > threads — but **a single collector must only ever be advanced by one
//! > thread** (it wraps a non-reentrant Lucene `Scorer` cursor in production;
//! > see `LuceneFilterDelegationHandle.collectDocs`).
//!
//! These tests force a segment to split across partitions (multiple RGs +
//! `target_partitions > 1`, so `compute_assignments` cuts mid-segment), then run
//! the shared queue with a [`SerialGuardCollector`] that **panics if any single
//! collector instance is entered by two threads at once**. A correct
//! chunk-granular implementation never trips it, because the production
//! `evaluator_factory` builds a fresh collector per chunk and each chunk is popped
//! and advanced by exactly one task.
//!
//! Coverage matrix (each run with delegation via `SingleCollectorEvaluator`, one
//! deliberately-slow chunk, and the per-collector serial guard armed):
//!   - single segment, many RGs, partition counts {1,2,3,4} → segment split into
//!     1..4 chunks;
//!   - multiple segments of *different* sizes, partition counts {1,2,3,5}.
//! Every case asserts byte-identical results vs the static (flag-off) path.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use tempfile::NamedTempFile;

use super::super::eval::single_collector::{
    CollectorCallStrategy, DelegatedBackendCollectorFactory, SingleCollectorEvaluator,
};
use super::super::eval::RowGroupBitsetSource;
use super::super::index::RowGroupDocsCollector;
use super::super::page_pruner::PagePruner;
use super::super::stream::{FilterStrategy, RowGroupInfo};
use super::super::table_provider::{IndexedTableConfig, IndexedTableProvider, SegmentFileInfo};

/// A segment fixture: distinct brand, base price, `rows` rows in row groups of
/// `max_rg_rows`. With `max_rg_rows < rows` the segment has multiple RGs, which
/// is what lets `compute_assignments` split it across partitions.
struct Seg {
    brand: &'static str,
    base_price: i32,
    rows: usize,
    max_rg_rows: usize,
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

/// A deterministic, content-addressed match rule: a doc matches if
/// `abs_doc % modulus == residue`. Crucially this depends ONLY on the absolute
/// doc id — NOT on the chunk's bounds — so it faithfully models a real Lucene
/// `Scorer` (whose matches are a function of segment content, not partitioning).
///
/// That's what makes the correctness assertions sharp under work-stealing: if a
/// stolen chunk's collector were created with the wrong `doc_range`, or a chunk
/// were dropped / double-processed, the SELECTIVE result set would diverge from
/// the static path. A match-all collector would mask all of those bugs.
#[derive(Clone, Copy, Debug)]
struct MatchRule {
    modulus: i32,
    residue: i32,
}

impl MatchRule {
    fn matches(&self, abs_doc: i32) -> bool {
        abs_doc.rem_euclid(self.modulus) == self.residue
    }
}

/// Identity of a chunk for double-dispatch detection: `(segment, doc range)`.
/// This is exactly the tuple a production collector is keyed on
/// (`createCollector(.., writer_generation, doc_min, doc_max)`), so two work
/// items with this identity ARE the same collector.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct ChunkId {
    segment_idx: usize,
    doc_min: i32,
    doc_max: i32,
}

/// Cross-partition guard that PANICS if the same chunk (== same collector) is
/// ever dispatched to a collector twice — whether concurrently or sequentially.
///
/// This is the strongest form of the one-thread-per-collector invariant: under
/// work-stealing every chunk must be popped from the shared queue and built into
/// a collector *exactly once*. If two partitions ever raced and both popped the
/// same item (a `SharedChunkQueue::pop` bug), or a node-reconstruction handed out
/// a non-drained queue, the same `ChunkId` would be registered twice and we fail
/// loudly — pinpointing the bug at the moment of the duplicate dispatch rather
/// than as a confusing wrong-row-count downstream.
#[derive(Debug, Default)]
struct ChunkDispatchGuard {
    seen: Mutex<HashSet<ChunkId>>,
}

impl ChunkDispatchGuard {
    fn register(&self, id: ChunkId) {
        let mut seen = self.seen.lock().unwrap();
        assert!(
            seen.insert(id),
            "chunk {id:?} dispatched to a collector twice — two partitions \
             processed the same chunk/collector (work-stealing double-pop bug)"
        );
    }

    fn distinct_chunks(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

/// Per-collector concurrency guard wrapping a selective [`MatchRule`].
///
/// Each instance models ONE production collector (one `(segment, doc_range)`
/// chunk). `collect_packed_u64_bitset` flips a per-instance in-flight flag and
/// sleeps briefly: if a second thread enters the SAME instance while the first
/// is mid-call, the flag is already set and we panic — exactly the corruption a
/// non-reentrant Lucene `Scorer` would suffer. A correct chunk-granular
/// implementation gives every chunk its own instance and advances each by a
/// single task, so this never trips even when sibling chunks of the same segment
/// run on different threads.
///
/// `slow_ms` makes this instance the lopsided one (forces a stealing race). The
/// shared `concurrent_now`/`max_concurrent` cross-instance counters let a test
/// assert DIFFERENT collectors *did* run concurrently (stealing genuinely
/// happened — and is safe), while the per-instance guard proves the SAME
/// collector never did.
#[derive(Debug)]
struct SerialGuardCollector {
    rule: MatchRule,
    in_flight: AtomicUsize,
    slow_ms: u64,
    /// Number of distinct collectors concurrently active (cross-instance).
    concurrent_now: Arc<AtomicUsize>,
    max_concurrent: Arc<AtomicUsize>,
    /// Counts the total docs this collector was asked to scan, summed across all
    /// instances. Lets a test prove no range was scanned twice (== total rows).
    total_scanned: Arc<AtomicUsize>,
}

impl RowGroupDocsCollector for SerialGuardCollector {
    fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        // Per-instance reentrancy guard: only one thread may be inside THIS
        // collector at a time.
        let prev = self.in_flight.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            prev, 0,
            "SerialGuardCollector entered concurrently by two threads — a single \
             collector was advanced by more than one thread (work-stealing \
             granularity bug)"
        );

        // Track cross-instance concurrency (this IS allowed and is the point of
        // stealing): record the peak number of collectors running at once.
        let now = self.concurrent_now.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_concurrent.fetch_max(now, Ordering::SeqCst);

        if self.slow_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.slow_ms));
        } else {
            // Tiny sleep so concurrent entries actually overlap in wall-clock,
            // giving the guards a chance to observe a race if one exists.
            std::thread::sleep(std::time::Duration::from_millis(2));
        }

        let span = (max_doc - min_doc).max(0) as usize;
        self.total_scanned.fetch_add(span, Ordering::SeqCst);
        let mut out = vec![0u64; span.div_ceil(64)];
        for abs_doc in min_doc..max_doc {
            if self.rule.matches(abs_doc) {
                let rel = (abs_doc - min_doc) as usize;
                out[rel / 64] |= 1u64 << (rel % 64);
            }
        }

        self.concurrent_now.fetch_sub(1, Ordering::SeqCst);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(out)
    }
}

/// Factory shim so `SingleCollectorEvaluator`'s performance-delegated path is
/// never taken (we drive the always-call correctness collector instead).
#[derive(Debug)]
struct UnusedDelegatedFactory;
impl DelegatedBackendCollectorFactory for UnusedDelegatedFactory {
    fn create(
        &self,
        _context_id: i64,
        _provider_key: i32,
        _writer_generation: i64,
        _doc_min: i32,
        _doc_max: i32,
    ) -> Result<Arc<dyn RowGroupDocsCollector>, String> {
        Err("delegated path not expected in this test".to_string())
    }
}

/// Outcome of one run: sorted rows, the executed plan, the peak number of
/// distinct collectors active at the same moment, and the total docs scanned
/// across all collectors (== total rows iff no range was double-scanned).
struct RunOutcome {
    rows: Vec<(String, i32)>,
    plan: Arc<dyn ExecutionPlan>,
    max_concurrent_collectors: usize,
    total_scanned: usize,
    /// Number of DISTINCT chunks dispatched (no duplicates — the guard panics on
    /// a repeat, so this equals the count of chunks actually processed).
    distinct_chunks: usize,
}

/// Build the provider over `segs`, run `SELECT brand, price`, and return the
/// outcome. `slow_seg` marks one segment whose collectors sleep `slow_ms`,
/// forcing a lopsided workload so an idle sibling steals. `rule` is the
/// SELECTIVE match predicate every collector applies (by absolute doc id) — so a
/// stealing bug that mis-scopes / drops / duplicates a chunk changes the result.
async fn run(
    segs: &[Seg],
    work_stealing: bool,
    num_partitions: usize,
    slow_seg: Option<usize>,
    slow_ms: u64,
    rule: MatchRule,
) -> RunOutcome {
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
    let concurrent_now = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));
    let total_scanned = Arc::new(AtomicUsize::new(0));
    let dispatch_guard = Arc::new(ChunkDispatchGuard::default());

    let factory: super::super::table_provider::EvaluatorFactory = {
        let schema = schema.clone();
        let concurrent_now = Arc::clone(&concurrent_now);
        let max_concurrent = Arc::clone(&max_concurrent);
        let total_scanned = Arc::clone(&total_scanned);
        let dispatch_guard = Arc::clone(&dispatch_guard);
        Arc::new(move |segment, chunk, _stream_metrics, _stats_prune_tree| {
            let seg_idx = segment.writer_generation as usize;
            let is_slow = slow_seg == Some(seg_idx);
            // The factory runs once per chunk a partition decides to process.
            // Register the chunk's identity: a SECOND dispatch of the same
            // (segment, doc_range) — by any partition — fails immediately. This
            // is the "two partitions collecting from the same chunk" check.
            dispatch_guard.register(ChunkId {
                segment_idx: chunk.segment_idx,
                doc_min: chunk.doc_min,
                doc_max: chunk.doc_max,
            });
            // One FRESH collector per (segment, chunk) — exactly what the
            // production factory does (FfmSegmentCollector::create per chunk).
            // The guard proves THIS instance is never advanced by two threads.
            // The collector matches by ABSOLUTE doc id (not chunk bounds), so a
            // mis-scoped / dropped / duplicated chunk shows up as a wrong result.
            let collector: Arc<dyn RowGroupDocsCollector> = Arc::new(SerialGuardCollector {
                rule,
                in_flight: AtomicUsize::new(0),
                slow_ms: if is_slow { slow_ms } else { 0 },
                concurrent_now: Arc::clone(&concurrent_now),
                max_concurrent: Arc::clone(&max_concurrent),
                total_scanned: Arc::clone(&total_scanned),
            });
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(SingleCollectorEvaluator::new(
                Some(collector),
                pruner,
                None,
                None,
                None,
                None,
                CollectorCallStrategy::FullRange,
                Arc::new(HashMap::new()),
                segment.writer_generation,
                Arc::new(UnusedDelegatedFactory),
                0,
                None,
                _stats_prune_tree.cloned(),
            ));
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
        query_config: Arc::new(qc),
        predicate_columns: vec![],
        emit_row_ids: false,
        prune_tree_config: None,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT brand, price FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), task_ctx).unwrap();
    let mut rows: Vec<(String, i32)> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let brand = b
            .column(b.schema().index_of("brand").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let price = b
            .column(b.schema().index_of("price").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            rows.push((brand.value(i).to_string(), price.value(i)));
        }
    }
    rows.sort();
    RunOutcome {
        rows,
        plan,
        max_concurrent_collectors: max_concurrent.load(Ordering::SeqCst),
        total_scanned: total_scanned.load(Ordering::SeqCst),
        distinct_chunks: dispatch_guard.distinct_chunks(),
    }
}

/// Ground-truth expected rows for `segs` under `rule`, computed independently of
/// the scan (so it can't share a bug with it). A row at absolute doc `d` in
/// segment `s` survives iff `rule.matches(d)`; its emitted columns are
/// `(brand, base_price + d)` matching `write_segment`.
fn expected_rows(segs: &[Seg], rule: MatchRule) -> Vec<(String, i32)> {
    let mut out = Vec::new();
    for s in segs {
        for d in 0..s.rows as i32 {
            if rule.matches(d) {
                out.push((s.brand.to_string(), s.base_price + d));
            }
        }
    }
    out.sort();
    out
}

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

/// One segment of 40 rows in 8 RGs of 5. With N partitions, `compute_assignments`
/// cuts the segment into N chunks (≈40/N rows each, RG-aligned). Each chunk → its
/// own collector with its own doc_range.
fn one_big_segment() -> Vec<Seg> {
    vec![Seg {
        brand: "solo",
        base_price: 0,
        rows: 40,
        max_rg_rows: 5,
    }]
}

/// Several segments of DIFFERENT sizes, each multi-RG so the partitioner can also
/// split individual segments.
fn varied_segments() -> Vec<Seg> {
    vec![
        Seg { brand: "tiny", base_price: 0, rows: 8, max_rg_rows: 4 },
        Seg { brand: "small", base_price: 100, rows: 20, max_rg_rows: 5 },
        Seg { brand: "medium", base_price: 1000, rows: 35, max_rg_rows: 5 },
        Seg { brand: "large", base_price: 10_000, rows: 60, max_rg_rows: 6 },
    ]
}

/// A selective rule: ~1/3 of docs match (`doc % 3 == 1`). Spread across every RG,
/// so every chunk contributes some matches and the result is sensitive to which
/// chunk a collector was scoped to.
const SELECTIVE: MatchRule = MatchRule { modulus: 3, residue: 1 };

/// Single segment, split into 1..4 chunks by partition count, SELECTIVE delegation.
///
/// With a slow first chunk a sibling steals the rest — every chunk's collector is
/// distinct, so the per-collector serial guard never trips. Because matching is by
/// absolute doc id, the result is verified against an independent ground truth
/// AND against the static path: a mis-scoped / dropped / double-counted chunk
/// would change the rows (and `total_scanned`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_segment_split_selective_delegation_no_collector_race() {
    let segs = one_big_segment();
    let expected = expected_rows(&segs, SELECTIVE);
    // Ground truth is non-trivial and selective (neither empty nor all rows).
    assert!(!expected.is_empty() && expected.len() < 40, "rule must be selective");

    // Static path must already produce the ground truth.
    let base = run(&segs, false, 1, None, 0, SELECTIVE).await;
    assert_eq!(base.rows, expected, "static path disagrees with ground truth");

    for np in [1usize, 2, 3, 4] {
        // Slow the segment's collectors so, when split, an idle partition races
        // to steal the remaining chunks of the SAME segment.
        let out = run(&segs, true, np, Some(0), 25, SELECTIVE).await;
        assert_eq!(
            out.rows, expected,
            "work-stealing produced wrong rows at np={np}"
        );
        // Every doc scanned exactly once across all collectors → no chunk was
        // dropped or double-processed (independent of the row-equality check).
        assert_eq!(
            out.total_scanned, 40,
            "expected each of 40 docs scanned exactly once at np={np}, saw {}",
            out.total_scanned
        );
        // No chunk was dispatched twice. The dispatch guard panics on a repeat,
        // so reaching here at all means every chunk was processed by exactly one
        // partition; distinct_chunks just records how many there were.
        assert!(
            out.distinct_chunks >= np.min(8),
            "expected the 8-RG segment to split into at least {} chunks at np={np}, saw {}",
            np.min(8),
            out.distinct_chunks
        );
        // For np>1 the segment really split into >1 concurrently-running collector
        // (so the "same segment, multiple collectors on different threads" case is
        // genuinely exercised — not just asserted).
        if np > 1 {
            assert!(
                out.max_concurrent_collectors >= 2,
                "expected >=2 concurrent collectors of the same segment at np={np}, saw {}",
                out.max_concurrent_collectors
            );
        }
    }
}

/// Multiple segments of different sizes, several partition counts, one slow
/// segment, SELECTIVE delegation. Rows match ground truth + static; each doc
/// scanned exactly once; no collector advanced by two threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_segment_varied_sizes_selective_delegation_no_collector_race() {
    let segs = varied_segments();
    let total_rows = segs.iter().map(|s| s.rows).sum::<usize>();
    let expected = expected_rows(&segs, SELECTIVE);

    let base = run(&segs, false, 1, None, 0, SELECTIVE).await;
    assert_eq!(base.rows, expected, "static path disagrees with ground truth");

    for np in [1usize, 2, 3, 5] {
        // "medium" (idx 2) is the slow one — forces stealing of the others.
        let out = run(&segs, true, np, Some(2), 20, SELECTIVE).await;
        assert_eq!(
            out.rows, expected,
            "work-stealing produced wrong rows at np={np}"
        );
        assert_eq!(
            out.total_scanned, total_rows,
            "expected each doc scanned exactly once at np={np}, saw {} (total {total_rows})",
            out.total_scanned
        );
        if np > 1 {
            assert!(
                out.max_concurrent_collectors >= 2,
                "expected concurrent distinct collectors at np={np}, saw {}",
                out.max_concurrent_collectors
            );
            assert!(
                stolen(&out.plan) > 0,
                "expected the shared-queue path to engage at np={np}"
            );
        }
    }
}

/// Sweep several selectivities (incl. empty match and all-match) over both
/// fixtures and partition counts, to make sure delegation results are correct
/// regardless of how many rows the delegated filter keeps — the kind of variation
/// real `match()` queries exhibit (see the selectivity spread in the multi-RG
/// decoder notes).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delegation_correct_across_selectivities() {
    let rules = [
        MatchRule { modulus: 1, residue: 0 },   // match all
        MatchRule { modulus: 2, residue: 0 },   // ~half
        MatchRule { modulus: 5, residue: 3 },   // ~1/5
        MatchRule { modulus: 1000, residue: 1 }, // ~empty (only doc 1)
    ];
    for rule in rules {
        for segs in [one_big_segment(), varied_segments()] {
            let total_rows = segs.iter().map(|s| s.rows).sum::<usize>();
            let expected = expected_rows(&segs, rule);
            for np in [1usize, 2, 3, 4] {
                let out = run(&segs, true, np, Some(0), 8, rule).await;
                assert_eq!(
                    out.rows, expected,
                    "wrong rows for rule {rule:?} at np={np}"
                );
                assert_eq!(
                    out.total_scanned, total_rows,
                    "doc scanned more/less than once for rule {rule:?} at np={np}"
                );
            }
        }
    }
}

/// The `work_stolen_chunks` metric must mean exactly "chunks a partition
/// processed that were NOT in its own static assignment" — a true cross-partition
/// steal. Pins three properties:
///   1. Feature OFF ⇒ exactly 0 (the static path never steals).
///   2. Feature ON, single partition ⇒ exactly 0 (one partition owns every chunk,
///      so nothing it pops is foreign — guards against the old proxy that counted
///      "chunks beyond the first" and would report N-1 here).
///   3. Feature ON, lopsided multi-partition ⇒ 0 < steals <= (total chunks - the
///      smallest partition's own chunk count): some work crosses a boundary, but
///      never more than the chunks that could be foreign to a draining partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn work_stolen_chunks_counts_only_true_cross_partition_steals() {
    let segs = one_big_segment(); // 40 rows / 8 RGs

    // (1) Feature off → never steals, regardless of partition count.
    for np in [1usize, 2, 4] {
        let out = run(&segs, false, np, None, 0, SELECTIVE).await;
        assert_eq!(stolen(&out.plan), 0, "flag off must report 0 steals at np={np}");
    }

    // (2) Feature on, single partition → that partition owns all 8 RGs (one chunk),
    // so nothing it processes is foreign. The OLD proxy ("beyond first") would have
    // reported 0 here too only because there's one chunk; make the point explicit
    // with a forced-slow front that does NOT create a sibling to steal from.
    let out = run(&segs, true, 1, Some(0), 10, SELECTIVE).await;
    assert_eq!(
        stolen(&out.plan),
        0,
        "single partition owns every chunk — nothing can be stolen, got {}",
        stolen(&out.plan)
    );
    // Sanity: it really did process the whole segment.
    assert_eq!(out.total_scanned, 40);

    // (3) Lopsided, chunks > partitions: 4 segments → >= 4 chunks, np=2. A single
    // 40-row segment splits into only 2 chunks at np=2 (chunks == partitions), and
    // both partitions pop at startup so each can take its own — no guaranteed
    // steal. Multiple segments cut extra chunks (segment boundaries flush a chunk),
    // so there ARE more chunks than partitions. With a very slow seg0 at the front,
    // the partition that pops it processes just that one chunk while its sibling
    // drains the rest — so the sibling necessarily takes >= 1 chunk it doesn't own.
    let segs = varied_segments();
    let total_rows = segs.iter().map(|s| s.rows).sum::<usize>();
    let out = run(&segs, true, 2, Some(0), 150, SELECTIVE).await;
    let s = stolen(&out.plan);
    assert!(out.distinct_chunks > 2, "fixture must yield > np chunks, saw {}", out.distinct_chunks);
    assert!(s >= 1, "expected a true steal under imbalance, got {s}");
    assert!(
        s <= out.distinct_chunks.saturating_sub(1),
        "steals ({s}) cannot exceed total chunks ({}) minus the puller's own (>=1)",
        out.distinct_chunks
    );
    assert_eq!(out.total_scanned, total_rows, "result coverage unchanged by stealing");
}

/// Meta-test: prove the [`SerialGuardCollector`] guard is NOT a no-op — i.e. it
/// genuinely panics when one collector instance is entered by two threads at
/// once. If this didn't fail, the "no collector race" assertions in the e2e
/// tests above would be vacuous.
#[test]
fn serial_guard_actually_trips_on_concurrent_same_collector() {
    let collector = Arc::new(SerialGuardCollector {
        rule: SELECTIVE,
        in_flight: AtomicUsize::new(0),
        slow_ms: 20, // long enough that the two calls overlap
        concurrent_now: Arc::new(AtomicUsize::new(0)),
        max_concurrent: Arc::new(AtomicUsize::new(0)),
        total_scanned: Arc::new(AtomicUsize::new(0)),
    });
    let c1 = Arc::clone(&collector);
    let c2 = Arc::clone(&collector);
    let h1 = std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            c1.collect_packed_u64_bitset(0, 64)
        }))
    });
    let h2 = std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            c2.collect_packed_u64_bitset(0, 64)
        }))
    });
    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();
    assert!(
        r1.is_err() || r2.is_err(),
        "guard failed to detect two threads inside the same collector"
    );
}

/// Meta-test: prove the [`ChunkDispatchGuard`] is NOT a no-op — registering the
/// same `(segment, doc_range)` twice must panic. This is the guard that backs the
/// "two partitions must never collect from the same chunk/collector" assertion in
/// every e2e test (the factory calls `register` once per dispatched chunk).
#[test]
#[should_panic(expected = "dispatched to a collector twice")]
fn chunk_dispatch_guard_rejects_duplicate_dispatch() {
    let guard = ChunkDispatchGuard::default();
    let id = ChunkId { segment_idx: 0, doc_min: 0, doc_max: 10 };
    guard.register(id); // first dispatch: fine
    guard.register(id); // same chunk again: must panic
}

/// Stress: many runs of the lopsided single-segment split, to give a same-collector
/// race (if one existed) repeated chances to surface under the scheduler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_runs_never_trip_the_collector_guard() {
    let segs = one_big_segment();
    let expected = expected_rows(&segs, SELECTIVE);
    for _ in 0..20 {
        let out = run(&segs, true, 4, Some(0), 5, SELECTIVE).await;
        assert_eq!(out.rows, expected);
        assert_eq!(out.total_scanned, 40);
    }
}
