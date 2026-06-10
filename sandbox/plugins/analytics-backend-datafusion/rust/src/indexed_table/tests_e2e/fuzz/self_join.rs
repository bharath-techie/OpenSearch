/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Fuzz coverage for PER-SCAN filter routing in a single-shard self-join.
//!
//! Production behavior under test: when one data-node fragment scans the same
//! index more than once (a self-join / self-union that did not split into
//! per-branch stages at single shard), each scan carries its OWN pushed-down
//! WHERE. `IndexedTableProvider::scan` must rebuild the evaluator / pushdown
//! predicate from THAT scan's filter (via the `per_scan_builder`) instead of
//! sharing one whole-fragment filter. Sharing one filter is the bug this guards
//! against: it made both branches of a self-join compute the same count
//! (OpenSearch ComplexJoins Q1: `failed_attempts == suspicious_count` for every
//! row), which is wrong.
//!
//! The test registers ONE `IndexedTableProvider` whose `per_scan_builder` is the
//! production `build_filter_artifacts` (predicate-only path — no FFM/Lucene
//! upcalls needed), then runs a two-subquery self-join where each branch has a
//! distinct random `Column op Literal` WHERE. The result must equal the oracle
//! `{rows: leftPred} ∩ {rows: rightPred}` (inner join on the unique `__doc_id`).
//! A regression that shares one filter across both scans produces a different
//! intersection and fails.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{col, lit, Expr, Operator};
use futures::StreamExt;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

use super::corpus::{CellValue, Corpus};
use super::harness::LoadedSegment;
use crate::indexed_table::table_provider::{IndexedTableConfig, IndexedTableProvider};

/// One generated self-join shape: a distinct `Column op Literal` predicate per branch.
pub(in crate::indexed_table::tests_e2e) struct SelfJoinShape {
    pub left: BranchPredicate,
    pub right: BranchPredicate,
}

/// A single branch's WHERE: `col <op> lit`, kept both as a logical `Expr` (for the
/// SQL/DataFrame query) and structurally (for the row-by-row oracle).
pub(in crate::indexed_table::tests_e2e) struct BranchPredicate {
    pub col_name: String,
    pub op: Operator,
    pub lit: ScalarValue,
}

impl BranchPredicate {
    fn to_expr(&self) -> Expr {
        let c = col(&self.col_name);
        let l = lit(self.lit.clone());
        match self.op {
            Operator::Eq => c.eq(l),
            Operator::NotEq => c.not_eq(l),
            Operator::Lt => c.lt(l),
            Operator::LtEq => c.lt_eq(l),
            Operator::Gt => c.gt(l),
            Operator::GtEq => c.gt_eq(l),
            other => panic!("self_join fuzz: unsupported op {:?}", other),
        }
    }
}

/// Generate a self-join shape: two independent random predicates over the corpus
/// schema (skipping `__doc_id` at col 0). The two branches deliberately use
/// different columns/ops so a filter mix-up between scans is observable.
pub(in crate::indexed_table::tests_e2e) fn generate_self_join(
    rng: &mut StdRng,
    corpus: &Corpus,
) -> SelfJoinShape {
    SelfJoinShape {
        left: gen_branch_predicate(rng, corpus),
        right: gen_branch_predicate(rng, corpus),
    }
}

fn gen_branch_predicate(rng: &mut StdRng, corpus: &Corpus) -> BranchPredicate {
    // Columns 1.. are user columns (__doc_id is col 0). Pick one of the scalar
    // types the literal generator + oracle handle (skip e.g. TimestampNanos).
    let supported: Vec<usize> = (1..corpus.schema.fields().len())
        .filter(|&i| {
            matches!(
                corpus.schema.field(i).data_type(),
                DataType::Utf8
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::Date32
            )
        })
        .collect();
    let col_idx = supported[rng.gen_range(0..supported.len())];
    let field = corpus.schema.field(col_idx);
    let ops = [
        Operator::Eq,
        Operator::NotEq,
        Operator::Lt,
        Operator::LtEq,
        Operator::Gt,
        Operator::GtEq,
    ];
    let op = ops[rng.gen_range(0..ops.len())];
    let lit = gen_literal(rng, field.data_type());
    BranchPredicate {
        col_name: field.name().clone(),
        op,
        lit,
    }
}

/// Pick a literal in a range that yields a non-trivial selectivity for the column type.
fn gen_literal(rng: &mut StdRng, dt: &DataType) -> ScalarValue {
    match dt {
        DataType::Utf8 => {
            // Single-char string within the corpus's small alphabet so EQ/range bite.
            let c = (b'a' + rng.gen_range(0..8u8)) as char;
            ScalarValue::Utf8(Some(c.to_string()))
        }
        DataType::Int32 => ScalarValue::Int32(Some(rng.gen_range(0..1000))),
        DataType::Int64 => ScalarValue::Int64(Some(rng.gen_range(0..10_000))),
        DataType::Float64 => ScalarValue::Float64(Some(rng.gen_range(0.0..100.0))),
        DataType::Boolean => ScalarValue::Boolean(Some(rng.gen())),
        DataType::Date32 => ScalarValue::Date32(Some(rng.gen_range(18_262..20_454))),
        other => panic!("self_join fuzz: unsupported literal type {:?}", other),
    }
}

/// Rows where a branch predicate is TRUE (SQL 3VL: NULL/UNKNOWN → not selected).
fn rows_matching(corpus: &Corpus, p: &BranchPredicate) -> Vec<i32> {
    let col_idx = *corpus
        .col_idx
        .get(&p.col_name)
        .unwrap_or_else(|| panic!("column {:?} not in corpus", p.col_name));
    let mut out = Vec::new();
    for row in 0..corpus.num_rows() {
        if compare_cell_lit_true(&corpus.cells[col_idx][row], p.op, &p.lit) {
            out.push(row as i32);
        }
    }
    out
}

/// `cell op lit` under SQL 3VL — UNKNOWN/NULL → false. Mirrors the delegation
/// fuzz's `compare_cell_lit_true` so oracle semantics match the engine.
fn compare_cell_lit_true(cell: &CellValue, op: Operator, lit: &ScalarValue) -> bool {
    use std::cmp::Ordering;
    macro_rules! bail_unknown {
        ($x:expr) => {
            match $x {
                None => return false,
                Some(v) => v,
            }
        };
    }
    let ord: Option<Ordering> = match (cell, lit) {
        (CellValue::Utf8(c), ScalarValue::Utf8(l)) => {
            Some(bail_unknown!(c).as_str().cmp(bail_unknown!(l).as_str()))
        }
        (CellValue::Int32(c), ScalarValue::Int32(l)) => Some(bail_unknown!(c).cmp(bail_unknown!(l))),
        (CellValue::Int64(c), ScalarValue::Int64(l)) => Some(bail_unknown!(c).cmp(bail_unknown!(l))),
        (CellValue::Float64(c), ScalarValue::Float64(l)) => {
            let c = bail_unknown!(c);
            let l = bail_unknown!(l);
            if c.is_nan() || l.is_nan() {
                return false;
            }
            c.partial_cmp(l)
        }
        (CellValue::Boolean(c), ScalarValue::Boolean(l)) => {
            Some((*bail_unknown!(c) as i32).cmp(&(*bail_unknown!(l) as i32)))
        }
        (CellValue::Date32(c), ScalarValue::Date32(l)) => Some(bail_unknown!(c).cmp(bail_unknown!(l))),
        _ => panic!("self_join fuzz: cell/lit type mismatch {:?} vs {:?}", cell, lit),
    };
    let ord = match ord {
        Some(o) => o,
        None => return false,
    };
    match op {
        Operator::Eq => ord == Ordering::Equal,
        Operator::NotEq => ord != Ordering::Equal,
        Operator::Lt => ord == Ordering::Less,
        Operator::LtEq => ord != Ordering::Greater,
        Operator::Gt => ord == Ordering::Greater,
        Operator::GtEq => ord != Ordering::Less,
        other => panic!("self_join fuzz: unsupported op {:?}", other),
    }
}

/// How the two same-index branches are combined. Both collapse into ONE
/// data-node fragment with two scans at single shard, so both exercise per-scan
/// filter routing — they differ only in the set algebra the oracle applies.
#[derive(Clone, Copy, Debug)]
pub(in crate::indexed_table::tests_e2e) enum Combinator {
    /// Inner self-join on the unique `__doc_id` ⇒ intersection.
    Join,
    /// `UNION ALL` of the two arms' `__doc_id`s ⇒ union (deduped — `__doc_id`
    /// is unique so a row in both arms appears once after our sort+dedup).
    Union,
}

/// Oracle: combine the two branches' matching row sets per `Combinator`.
fn expected_doc_ids(corpus: &Corpus, shape: &SelfJoinShape, combinator: Combinator) -> Vec<i32> {
    use std::collections::BTreeSet;
    let l: BTreeSet<i32> = rows_matching(corpus, &shape.left).into_iter().collect();
    let r: BTreeSet<i32> = rows_matching(corpus, &shape.right).into_iter().collect();
    match combinator {
        Combinator::Join => l.intersection(&r).copied().collect(),
        Combinator::Union => l.union(&r).copied().collect(),
    }
}

/// Build the table provider with the production `per_scan_builder` and run the
/// self-join or self-union. Returns the sorted `__doc_id`s the engine produced.
async fn execute_self_join(
    loaded: &LoadedSegment,
    shape: &SelfJoinShape,
    combinator: Combinator,
) -> Vec<i32> {
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .force_pushdown(Some(true))
        .build();

    // The per-scan builder IS production `build_filter_artifacts`. With no
    // Collector markers in these predicates it takes the FilterClass::None
    // (predicate-only) path — no FFM/Lucene upcall — and returns a
    // PredicateOnlyEvaluator wired to THAT scan's filter.
    let schema = loaded.schema.clone();
    let store_for_builder = Arc::clone(&store);
    let io_handle = tokio::runtime::Handle::current();
    let per_scan_builder: Arc<
        dyn Fn(
                &[Expr],
                &dyn datafusion::catalog::Session,
            )
                -> datafusion::common::Result<crate::indexed_executor::IndexedFilterArtifacts>
            + Send
            + Sync,
    > = Arc::new(move |filters: &[Expr], state: &dyn datafusion::catalog::Session| {
        let filter_expr = filters.iter().cloned().reduce(|acc, f| acc.and(f));
        crate::indexed_executor::build_filter_artifacts(
            filter_expr,
            &schema,
            state,
            /*context_id=*/ 0,
            /*classification_override=*/ None,
            &qc,
            &store_for_builder,
            &io_handle,
        )
    });

    // The whole-fragment artifacts are intentionally a full-scan (None filter,
    // unreachable evaluator factory) so that if the per-scan path were NOT taken
    // the test would surface it loudly rather than silently passing.
    let whole_fragment_qc = crate::datafusion_query_config::DatafusionQueryConfig::builder()
        .target_partitions(1)
        .build();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: loaded.schema.clone(),
        segments: loaded.segments.clone(),
        store,
        store_url,
        evaluator_factory: Arc::new(|_, _, _| {
            panic!("self_join fuzz: per-scan builder must supply the evaluator, not the config")
        }),
        pushdown_predicate: None,
        query_config: Arc::new(whole_fragment_qc),
        predicate_columns: vec![],
        emit_row_ids: false,
        per_scan_builder: Some(per_scan_builder),
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();

    // Two branches, each carrying its own WHERE — exactly the shape that collapses
    // into one fragment with two scans of the same index at single shard.
    let combined = match combinator {
        Combinator::Join => {
            let left = ctx
                .table("t").await.unwrap()
                .filter(shape.left.to_expr()).unwrap()
                .select(vec![col("__doc_id")]).unwrap();
            let right = ctx
                .table("t").await.unwrap()
                .filter(shape.right.to_expr()).unwrap()
                .select(vec![col("__doc_id").alias("rid")]).unwrap();
            left.join_on(
                right,
                datafusion::logical_expr::JoinType::Inner,
                [col("__doc_id").eq(col("rid"))],
            )
            .unwrap()
            .select(vec![col("__doc_id")])
            .unwrap()
        }
        Combinator::Union => {
            let left = ctx
                .table("t").await.unwrap()
                .filter(shape.left.to_expr()).unwrap()
                .select(vec![col("__doc_id")]).unwrap();
            let right = ctx
                .table("t").await.unwrap()
                .filter(shape.right.to_expr()).unwrap()
                .select(vec![col("__doc_id")]).unwrap();
            left.union(right).unwrap()
        }
    };

    let plan = combined.create_physical_plan().await.unwrap();
    eprintln!(
        "=== DIAG {:?} PHYSICAL PLAN ===\n{}",
        combinator,
        datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
    );
    let task_ctx = ctx.task_ctx();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), task_ctx).unwrap();
    let mut doc_ids: Vec<i32> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let idx = b.schema().index_of("__doc_id").expect("__doc_id in batch");
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("__doc_id is Int32");
        for i in 0..arr.len() {
            assert!(arr.is_valid(i), "__doc_id is non-null");
            doc_ids.push(arr.value(i));
        }
    }
    doc_ids.sort_unstable();
    doc_ids.dedup();
    doc_ids
}

// ════════════════════════════════════════════════════════════════════════════
// Delegated (Collector) self-join — the production ComplexJoins Q1 shape.
// ════════════════════════════════════════════════════════════════════════════

/// Mock collector replaying a fixed doc-id set over `[min_doc, max_doc)`.
#[derive(Debug)]
struct MockCollector {
    matching: Vec<i32>,
}

impl crate::indexed_table::index::RowGroupDocsCollector for MockCollector {
    fn collect_packed_u64_bitset(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        let span = (max_doc - min_doc) as usize;
        let mut out = vec![0u64; span.div_ceil(64)];
        for &doc in &self.matching {
            if doc >= min_doc && doc < max_doc {
                let rel = (doc - min_doc) as usize;
                out[rel / 64] |= 1u64 << (rel % 64);
            }
        }
        Ok(out)
    }
}

/// Deterministic correctness test for the DELEGATED per-scan path — the actual
/// production shape of OpenSearch ComplexJoins Q1
/// (`... | where A | stats | inner join [... | where B | stats]` at single shard,
/// where A and B delegate to Lucene).
///
/// Each branch's WHERE is a `delegated_predicate`-style filter that resolves to a
/// distinct Collector match-set. The `per_scan_builder` inspects the scan's pushed
/// filter to decide which branch it is and returns a `SingleCollectorEvaluator`
/// wired to THAT branch's mock collector. The shared-filter bug would apply one
/// branch's collector to both scans, making the intersection wrong; per-scan
/// routing keeps them distinct.
///
/// Branch L matches rows where `price < THRESHOLD`; branch R matches rows where
/// `price >= THRESHOLD`. With distinct collectors the inner self-join on the unique
/// `__doc_id` yields the EMPTY set (the two sets are disjoint). The bug — both
/// scans sharing one collector — would instead yield that collector's whole set
/// (self-intersection), which is non-empty. So this asserts a hard, unambiguous
/// `expected == []`.
#[tokio::test(flavor = "multi_thread")]
async fn delegated_self_join_routes_distinct_collectors() {
    use crate::indexed_table::eval::single_collector::{CollectorCallStrategy, SingleCollectorEvaluator};
    use crate::indexed_table::eval::RowGroupBitsetSource;
    use crate::indexed_table::index::RowGroupDocsCollector;
    use crate::indexed_table::page_pruner::PagePruner;
    use crate::indexed_table::table_provider::EvaluatorFactory;

    let corpus = super::build_corpus(super::FixtureConfig::small(0xC0FFEE));
    let loaded = super::load_segment(&corpus);

    // Partition rows by price < 500 (left) vs >= 500 (right) — disjoint sets.
    let price_idx = *corpus.col_idx.get("price").unwrap();
    let mut left_rows = Vec::new();
    let mut right_rows = Vec::new();
    for row in 0..corpus.num_rows() {
        if let CellValue::Int32(Some(v)) = &corpus.cells[price_idx][row] {
            if *v < 500 {
                left_rows.push(row as i32);
            } else {
                right_rows.push(row as i32);
            }
        }
    }
    assert!(!left_rows.is_empty() && !right_rows.is_empty(), "need both branches non-empty");

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let schema = loaded.schema.clone();

    // per_scan_builder: route by the scan's pushed filter. `price < ...` → left
    // collector; otherwise → right collector. Mirrors how production resolves a
    // distinct delegated provider per branch — here keyed off the rendered filter.
    let left_rows_c = left_rows.clone();
    let right_rows_c = right_rows.clone();
    let schema_c = schema.clone();
    let per_scan_builder: Arc<
        dyn Fn(
                &[Expr],
                &dyn datafusion::catalog::Session,
            )
                -> datafusion::common::Result<crate::indexed_executor::IndexedFilterArtifacts>
            + Send
            + Sync,
    > = Arc::new(move |filters: &[Expr], _state: &dyn datafusion::catalog::Session| {
        let rendered = filters.iter().map(|f| format!("{f}")).collect::<Vec<_>>().join(" AND ");
        let is_left = rendered.contains('<');
        let matching = if is_left { left_rows_c.clone() } else { right_rows_c.clone() };
        let schema = schema_c.clone();
        let factory: EvaluatorFactory = Arc::new(move |segment, _chunk, stream_metrics| {
            let collector: Arc<dyn RowGroupDocsCollector> =
                Arc::new(MockCollector { matching: matching.clone() });
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(SingleCollectorEvaluator::new(
                Some(collector),
                pruner,
                None,
                None,
                None,
                stream_metrics.ffm_collector_calls.clone(),
                CollectorCallStrategy::FullRange,
                Arc::new(std::collections::HashMap::new()),
                segment.writer_generation,
                Arc::new(crate::indexed_table::eval::single_collector::FfmDelegatedBackendCollectorFactory),
                0,
                None,
            ));
            Ok(eval)
        });
        Ok(crate::indexed_executor::IndexedFilterArtifacts {
            evaluator_factory: factory,
            pushdown_predicate: None,
            predicate_columns: vec![],
        })
    });

    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: loaded.schema.clone(),
        segments: loaded.segments.clone(),
        store,
        store_url,
        evaluator_factory: Arc::new(|_, _, _| panic!("must use per-scan builder")),
        pushdown_predicate: None,
        query_config: Arc::new(
            crate::datafusion_query_config::DatafusionQueryConfig::builder()
                .target_partitions(1)
                .build(),
        ),
        predicate_columns: vec![],
        emit_row_ids: false,
        per_scan_builder: Some(per_scan_builder),
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    // Two branches with distinct (disjoint) filters; inner join on unique __doc_id.
    let left = ctx
        .table("t").await.unwrap()
        .filter(col("price").lt(lit(500i32))).unwrap()
        .select(vec![col("__doc_id")]).unwrap();
    let right = ctx
        .table("t").await.unwrap()
        .filter(col("price").gt_eq(lit(500i32))).unwrap()
        .select(vec![col("__doc_id").alias("rid")]).unwrap();
    let joined = left
        .join_on(right, datafusion::logical_expr::JoinType::Inner, [col("__doc_id").eq(col("rid"))])
        .unwrap()
        .select(vec![col("__doc_id")]).unwrap();
    let plan = joined.create_physical_plan().await.unwrap();
    let mut stream =
        datafusion::physical_plan::execute_stream(Arc::clone(&plan), ctx.task_ctx()).unwrap();
    let mut n = 0usize;
    while let Some(batch) = stream.next().await {
        n += batch.unwrap().num_rows();
    }
    assert_eq!(
        n, 0,
        "disjoint per-branch collectors must yield an empty inner join; \
         got {} rows — both scans likely shared one collector (the bug)",
        n
    );
}

/// One iteration: the engine's result must equal the oracle for the given
/// combinator (intersection for Join, union for Union).
pub(in crate::indexed_table::tests_e2e) async fn run_self_join_iteration(
    corpus: &Corpus,
    loaded: &LoadedSegment,
    shape: &SelfJoinShape,
    combinator: Combinator,
) -> Result<(), String> {
    let expected = expected_doc_ids(corpus, shape, combinator);
    let actual = execute_self_join(loaded, shape, combinator).await;
    if expected != actual {
        return Err(format!(
            "{:?} mismatch:\n  left:  {} {:?} {:?}\n  right: {} {:?} {:?}\n  \
             expected.len={} actual.len={} first_diff_idx={}",
            combinator,
            shape.left.col_name,
            shape.left.op,
            shape.left.lit,
            shape.right.col_name,
            shape.right.op,
            shape.right.lit,
            expected.len(),
            actual.len(),
            expected
                .iter()
                .zip(actual.iter())
                .position(|(a, b)| a != b)
                .map(|i| i.to_string())
                .unwrap_or_else(|| "len".to_string()),
        ));
    }
    Ok(())
}
