# Per-scan filter routing (self-join / self-union at single shard)

## TL;DR

A single-shard self-join or self-union scans the **same index twice in one
fragment**, and each scan carries its **own** delegated `WHERE`. The indexed
executor used to build **one** filter/evaluator for the whole fragment and apply
it to every scan — so both branches ran the *same* filter. This produced wrong
results (e.g. `failed_attempts == suspicious_count` for every row in
ComplexJoins Q1).

The fix makes `IndexedTableProvider::scan()` rebuild the filter artifacts **per
scan**, from the predicate DataFusion already pushes into that specific
`scan()` call. No FFM/protocol/Java-wire changes — it's contained to the Rust
indexed path.

---

## Background: how the indexed path turns a filter into an evaluator

When the analytics engine runs an "indexed" query (parquet + optional Lucene
delegation), the Rust side does roughly this in
`indexed_executor.rs::execute_indexed_with_context_inner`:

1. Decode the Substrait plan into a DataFusion `LogicalPlan`.
2. Pull the scan-level `WHERE` out of the plan (`extract_filter_expr`).
3. Turn that `Expr` into a `BoolNode` tree (`expr_to_bool_tree`) and **classify**
   it (`classify_filter` → `None` / `SingleCollector` / `Tree`).
4. From the tree, build three **artifacts**:
   - an **evaluator factory** — a closure that, per (segment × row-group chunk),
     produces the thing that decides which rows match (a `PredicateOnlyEvaluator`,
     a `SingleCollectorEvaluator` that calls Lucene via FFM, or a
     `BitmapTreeEvaluator`);
   - a **pushdown predicate** — the parquet-native residual handed to
     `ParquetSource::with_predicate` for decode-time row filtering;
   - the **predicate columns** — which physical columns the evaluator reads.
5. Stuff those into `IndexedTableConfig`, register one `IndexedTableProvider`
   under the table name, and execute.

`IndexedTableProvider::scan()` is then called by DataFusion **once per
`TableScan` in the plan**. It builds a `QueryShardExec` that uses the config's
single evaluator factory.

### The marker UDFs (`delegated_predicate`, `delegation_possible`)

A delegated `WHERE` like `where event_type = "auth_failure"` (on an indexed
keyword field) is rewritten on the Java/Calcite side into a marker UDF call
that survives the Substrait round-trip — e.g. `delegated_predicate(0)` where `0`
is an *annotation id*. `expr_to_bool_tree` recognizes those markers and turns
them into `BoolNode::Collector { annotation_id }` leaves; at execution the
evaluator calls back into Java (`create_provider(context_id, annotation_id)`) to
get the actual Lucene matches for that id. The UDF's own body panics by design —
it must never be *evaluated*, only *recognized*.

---

## The bug

At **single shard**, aggregates stay `mode=SINGLE` (no PARTIAL/FINAL split), so
there is no `ExchangeReducer` to cut a join/union into per-branch stages. The
**whole pipeline collapses into one data-node fragment**. For a self-join:

```
source=t | where event_type="A" | stats count() by k
| inner join ... [ source=t | where event_type="B" | stats count() by k ]
```

the fragment contains **two** `TableScan`s of `t`, and the decoded plan has two
`Filter` nodes — one with `delegated_predicate(0)` (event_type="A") above the
left scan, one with `delegated_predicate(1)` (event_type="B") above the right.

Step 2 above (`extract_filter_expr`) returns a **single** `Option<Expr>` — it
walks the plan depth-first and returns the *first* filter it finds. So the
whole-fragment evaluator was built from branch A's filter only, and the single
shared `IndexedTableProvider` applied **branch A's collector to both scans**.

Result: the right branch counted rows matching `A` instead of `B`. In
ComplexJoins Q1 that surfaced as `failed_attempts == suspicious_count` on every
row; row counts and values were simply wrong.

(Cross-table joins — two *different* indices — are unaffected here: the planner
splits them into separate per-index stages, so each branch is its own fragment
with its own filter. Their failures were an unrelated test-data issue.)

---

## The key realization

DataFusion **already** hands each scan its own filter.
`IndexedTableProvider::supports_filters_pushdown` returns `Exact`, so DataFusion
pushes each scan's adjacent `WHERE` predicate into **that** `scan()` call's
`filters` argument. We verified this on the real Substrait path — the two
`scan()` calls for a self-join receive:

```
scan 1 filters: [ delegation_possible(event_type = "authentication_failure", 0) ]
scan 2 filters: [ delegation_possible(event_type = "suspicious_activity",     1) ]
```

The old code **ignored `scan()`'s `filters` argument** (it used the pre-built
whole-fragment evaluator instead). The fix is simply to *use* it.

---

## The fix

### 1. Extract the artifact-building into a reusable function

`indexed_executor.rs` gained:

```rust
pub struct IndexedFilterArtifacts {
    pub evaluator_factory: EvaluatorFactory,
    pub pushdown_predicate: Option<Arc<dyn PhysicalExpr>>,
    pub predicate_columns: Vec<usize>,
}

pub fn build_filter_artifacts(
    filter_expr: Option<Expr>,          // a single scan's WHERE (or None)
    schema: &SchemaRef,
    state: &dyn Session,                // lowers leaves to PhysicalExprs (analyzer path)
    context_id: i64,
    classification_override: Option<FilterClass>,
    query_config: &DatafusionQueryConfig,
    store: &Arc<dyn ObjectStore>,
    io_handle: &tokio::runtime::Handle,
) -> Result<IndexedFilterArtifacts, DataFusionError>
```

This is the exact logic that used to live inline in
`execute_indexed_with_context_inner` (steps 2–4 above), now parameterized by a
single `filter_expr`. The session-setup path calls it once for the whole
fragment, unchanged in behavior.

> Note: `expr_to_bool_tree` / `convert_expr` changed their `state` parameter from
> `&SessionState` to `&dyn Session`. `SessionState` implements `Session`, and
> `Session::create_physical_expr` delegates to the same analyzer-driven method,
> so lowering behavior is identical. This lets `scan()` (which only has a
> `&dyn Session`) call the builder.

### 2. Let `scan()` rebuild artifacts from its own filter

`IndexedTableConfig` gained one optional field:

```rust
pub per_scan_builder: Option<
    Arc<dyn Fn(&[Expr], &dyn Session)
        -> Result<IndexedFilterArtifacts>
        + Send + Sync>
>,
```

`IndexedTableProvider::scan()` now does, at the top:

```rust
let per_scan_artifacts = match (&self.config.per_scan_builder, !filters.is_empty()) {
    (Some(builder), true) => Some(builder(filters, state)?),   // rebuild from THIS scan
    _ => None,                                                 // fall back to whole-fragment
};
```

and then threads the per-scan `evaluator_factory` / `pushdown_predicate` /
`predicate_columns` into the `QueryShardExec` it returns (each scan gets its own
`QueryShardExec`, so two scans of the same table no longer share state).

The `evaluator_factory` is now a field on `QueryShardExec` (set from the per-scan
artifacts, or the config's factory when there's no per-scan builder) instead of
being read from `config.evaluator_factory` at execution time.

### 3. Install the builder only when it's needed

In `execute_indexed_with_context_inner`, the `per_scan_builder` is populated
**only when the fragment scans some index more than once**:

```rust
let multi_scan = count_table_scans(&logical_plan) > 1;
let per_scan_builder = if !multi_scan { None } else { Some(Arc::new(move |filters, state| {
    let filter_expr = filters.iter().cloned().reduce(|a, f| a.and(f));
    build_filter_artifacts(filter_expr, &schema, state, context_id,
        /*classification_override=*/ None, &query_config, &store, &io_handle)
})) };
```

Two important details:

- **Single-scan fragments keep `None`** → `scan()` uses the whole-fragment
  artifacts exactly as before. Zero behavior change to the common path (HAVING,
  simple `WHERE`, single-table aggregations). This is deliberate: the
  whole-fragment build honors Java's authoritative `FilterTreeShape`
  classification override, which we don't want to second-guess on the path that
  already works.
- **Per-scan builds pass `classification_override = None`** → each scan
  classifies its **own** sub-tree with `classify_filter`. One fragment-level
  shape cannot describe two independently-shaped branch filters, so per-scan
  derivation is the correct choice here.

`count_table_scans` is a small recursive walk over the `LogicalPlan` added to
`substrait_to_tree.rs`.

---

## Why this also covers self-unions

The fix has **no notion of join vs. union**. It triggers on
`count_table_scans > 1` and reads each scan's pushed-down filter. A self-union
(`... | where A | stats | append [ ... | where B | stats ]`) collapses into the
exact same single-fragment-two-scans shape at single shard, and DataFusion
pushes each arm's filter into its own `scan()` the same way (verified:
`self_union_delivers_distinct_filters_per_scan`). So joins and unions are one
mechanism.

---

## What did NOT change

- No FFM signature changes, no new wire fields, no Java-side instruction
  changes. `createSessionContextForIndexedExecution` and `IndexedExecutionConfig`
  are untouched.
- The whole-fragment build path is byte-for-byte the same logic (it just lives in
  `build_filter_artifacts` now).
- Cross-table joins/unions (different indices) are split into per-index stages by
  the planner and never reach this code path; they are unaffected.

---

## Tests

| Test | Location | What it pins |
| --- | --- | --- |
| `self_join_delivers_distinct_filters_per_scan` | `table_provider.rs` | DataFusion delivers distinct per-scan filters for a self-join |
| `self_union_delivers_distinct_filters_per_scan` | `table_provider.rs` | …and for a self-union |
| `fuzz_self_join_per_scan_filter` / `_null_heavy` | `tests_e2e/fuzz/` | randomized self-join, result == oracle intersection, through the production `per_scan_builder` |
| `fuzz_self_union_per_scan_filter` / `_null_heavy` | `tests_e2e/fuzz/` | randomized self-union, result == oracle union |
| `delegated_self_join_routes_distinct_collectors` | `tests_e2e/fuzz/self_join.rs` | deterministic delegated-collector self-join (the ComplexJoins Q1 shape) |
| `SelfUnionDelegationIT` | `analytics-engine-rest` | end-to-end PPL self-union with delegated WHEREs per arm |

All six fail when the per-scan routing is reverted and pass with it — verified
by temporarily disabling the `(Some(builder), true)` arm and re-running.

To reproduce a fuzz failure deterministically:
`INDEXED_E2E_SEED=<hex> cargo test <test_name>`.
