# Implementing dynamic filters for `indexed_table`

> Companion to `dynamic-filters-investigation.md`. That doc explains the DataFusion
> mechanism; this one is the concrete implementation plan for **our** scan, whose pushdown
> path is **completely different** from `DataSourceExec`.
> File:line refs to our tree unless prefixed `df:` (then `~/Documents/dev/datafusion`).

---

## 0. The key realization: our pushdown is not DataFusion's pushdown

DataFusion's dynamic-filter story assumes the consumer is `DataSourceExec` and the filter lands
in its `predicate` field, consumed by `ParquetOpener` / `FilePruner`. **None of that applies to
us.** Two separate pushdown systems are in play:

1. **Logical-level `TableProvider::supports_filters_pushdown`** — we already return
   `Exact` for *every* filter (`table_provider.rs:170-181`). This deletes the outer
   `FilterExec` and routes the WHERE clause into our BoolNode tree / evaluator. This is the
   **static** filter path and is unrelated to dynamic filters.

2. **Physical-level `ExecutionPlan` pushdown hooks** — `gather_filters_for_pushdown` /
   `handle_child_pushdown_result`. This is the *only* channel a runtime dynamic filter
   (TopK / join) travels through. **We don't implement these**, so the default impl
   (`df:physical-plan/src/execution_plan.rs:581-680`) reports everything `unsupported` and the
   dynamic filter dies at the operator just above `QueryShardExec`.

**Crucial fact confirmed:** the indexed path uses DataFusion's *default* physical optimizer
rules minus `CombinePartialFinalAggregate` (`agg_mode.rs:29-37`,
`indexed_executor.rs:149`). The default set includes
`FilterPushdown::new_post_optimization()` (`df:physical-optimizer/src/optimizer.rs:240`) — the
**Post-phase** rule that pushes dynamic filters. So the machinery *runs* over our plan already;
the TopK above us *does* create a `DynamicFilterPhysicalExpr` and *tries* to push it to our
scan. It just hits our default (reject-all) hooks and stops.

**Therefore: the integration point is the physical `ExecutionPlan` pushdown hooks on
`QueryShardExec`, not `TableProvider` and not the substrait/BoolNode path.** We accept the
`Arc<DynamicFilterPhysicalExpr>` there, thread it into `IndexedExec` → `IndexedStream`
alongside the existing `pushdown_predicate`, and consume it RG-by-RG.

---

## 1. Architecture of the change

```
SortExec(fetch=k)                       ← producer: owns DynamicFilterPhysicalExpr, update()s it
   │   gather_filters_for_pushdown(Post) pushes the dyn filter as a self-filter to its child
   ▼
QueryShardExec                          ← (NEW) accept it in our pushdown hooks, store Arc
   │   execute(): clone Arc into each IndexedExec
   ▼
IndexedExec (per chunk)                 ← (NEW) carry dyn_filter: Option<Arc<dyn PhysicalExpr>>
   ▼
IndexedStream                           ← (NEW) subscribe once; re-snapshot per RG; prune/refine
   │
   ├─ per-RG: snapshot_generation changed? → snapshot() → PruningPredicate vs RG stats → skip RG
   └─ per-RG: hand snapshot to create_row_selection_stream(push=true)  OR  intersect candidate bitset
```

Nothing about the BoolNode tree, the evaluator, or `supports_filters_pushdown` changes.

---

## 2. Step-by-step implementation

### Step 1 — accept the dynamic filter on `QueryShardExec`

Implement the two hooks on `impl ExecutionPlan for QueryShardExec` (`table_provider.rs:347`).

```rust
fn gather_filters_for_pushdown(
    &self,
    phase: FilterPushdownPhase,
    parent_filters: Vec<Arc<dyn PhysicalExpr>>,
    _config: &ConfigOptions,
) -> Result<FilterDescription> {
    // We are a leaf (children() == []). Only accept in the Post phase — that is the
    // only phase where dynamic filters appear. In Pre, decline (our BoolNode tree
    // already owns static WHERE semantics; we don't want to double-handle).
    if phase != FilterPushdownPhase::Post {
        return Ok(FilterDescription::all_unsupported(&parent_filters, &self.children()));
    }
    // Mark a parent filter `Yes` only if every column it references exists in our
    // projected schema AND is a plain column/comparison we can evaluate against
    // parquet RG statistics. Everything else stays `No` so the parent keeps a
    // FilterExec as a safety net.
    let mut desc = ChildFilterDescription::all_unsupported(&parent_filters);
    for (i, f) in parent_filters.iter().enumerate() {
        if self.dynamic_filter_is_acceptable(f) {
            desc.mark_supported(i);          // pseudo — use the real PushedDown API
        }
    }
    Ok(FilterDescription::new_for_leaf(desc)) // pseudo — match the real constructor
}

fn handle_child_pushdown_result(
    &self,
    _phase: FilterPushdownPhase,
    child_pushdown_result: ChildPushdownResult,
    _config: &ConfigOptions,
) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
    // We're a leaf: the "accepted" filters come back as our own parent_filters.
    // Collect the ones we said Yes to, store them on a rebuilt QueryShardExec.
    let accepted: Vec<Arc<dyn PhysicalExpr>> = child_pushdown_result
        .parent_filters
        .iter()
        .filter(|p| matches!(p.all(), PushedDown::Yes))
        .map(|p| Arc::clone(&p.filter))
        .collect();
    if accepted.is_empty() {
        return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
    }
    let mut new_self = self.clone_shallow();        // QueryShardExec is not Clone today — add a ctor
    new_self.dynamic_filters = accepted;            // NEW field
    Ok(FilterPushdownPropagation::if_all(child_pushdown_result)
        .with_updated_node(Arc::new(new_self)))
}
```

> Verify the exact `FilterDescription` / `ChildFilterDescription` / `PushedDownPredicate`
> constructors against `df:physical-plan/src/filter_pushdown.rs:295-555` — the pseudo names
> above (`mark_supported`, `new_for_leaf`) must be replaced with the real API. The TopK
> consumer pattern to copy is `df:sorts/sort.rs:1336-1419`; a leaf-accepts pattern is what
> `DataSourceExec` does — grep `gather_filters_for_pushdown` in `datafusion/datasource/`.

New field on `QueryShardExec` (`table_provider.rs:309`):
```rust
/// Dynamic (and/or static residual) filters accepted via physical pushdown.
/// Each is typically an Arc<DynamicFilterPhysicalExpr>; read at runtime per RG.
dynamic_filters: Vec<Arc<dyn PhysicalExpr>>,
```

`dynamic_filter_is_acceptable(f)`: walk `f` (`PhysicalExpr::apply`), and accept iff it is
(or is a conjunction of) simple `col <op> literal` / dynamic-filter nodes whose columns are in
`self.projected_schema`. Reject anything referencing the panicking `index_filter(...)` UDF or
columns we don't read. **Conservatism is safe** — a `No` just means the parent keeps its
`FilterExec`, costing nothing but the chance to prune.

### Step 2 — thread it into `IndexedExec`

In `QueryShardExec::execute` (`table_provider.rs:385-451`), conjoin `dynamic_filters` and
clone the `Arc` into each `IndexedExec`:

```rust
let dyn_filter: Option<Arc<dyn PhysicalExpr>> =
    (!self.dynamic_filters.is_empty()).then(|| conjunction(self.dynamic_filters.clone()));
// ... in the IndexedExec { ... } literal:
dynamic_filter: dyn_filter.clone(),     // NEW field on IndexedExec (stream.rs:281)
```

The `Arc<DynamicFilterPhysicalExpr>` is shared — every chunk's `IndexedExec` sees the same
live inner state. (Column remap per chunk via `with_new_children` if schemas differ; usually
they don't here.)

### Step 3 — subscribe once in `IndexedStream`

Add to `IndexedStream` (`stream.rs:413`) and initialize in `new` (`stream.rs:504`):

```rust
dynamic_filter: Option<Arc<dyn PhysicalExpr>>,   // the conjoined accepted filter
df_tracking: DynamicFilterTracking,              // Static | AllComplete | Watching(tracker)
df_last_generation: u64,                         // for the cheap snapshot_generation() check
df_snapshot: Option<Arc<dyn PhysicalExpr>>,      // cached concrete predicate (current gen)
```

In `new`: `df_tracking = match &dynamic_filter { Some(p) => DynamicFilterTracking::classify(p),
None => Static }`. **One tracker per stream** — do not share across chunk streams; subscriptions
are per-consumer (see §5.1 of the investigation doc; relevant because our
`CoalescePartitionsExec` fan-out runs chunks concurrently).

### Step 4 — re-snapshot and prune per RG (Tier 1, the core win)

In `poll_inner`, in the `Poll::Ready(Ok(Some(prefetched)))` arm (`stream.rs:854`), *before*
building the row selection (`stream.rs:919`):

```rust
// Refresh the snapshot only when the filter actually moved (cheap atomic check).
if let Some(ref pred) = self.dynamic_filter {
    let gen = snapshot_generation(pred);            // df:physical-expr-common ...:982
    if self.df_snapshot.is_none() || gen != self.df_last_generation {
        self.df_last_generation = gen;
        self.df_snapshot = Some(snapshot_physical_expr(Arc::clone(pred))?); // flatten dyn -> concrete
    }
}

// RG-level prune: test the current snapshot against THIS RG's parquet statistics.
if let Some(ref snap) = self.df_snapshot {
    if let Some(pp) = build_pruning_predicate(snap, &self.full_schema /* or rg schema */, ...) {
        let rg_stats = /* PrunableStatistics for rg.index from self.metadata */;
        if pp.prune(&rg_stats)?.into_iter().all(|keep| !keep) {
            // RG provably cannot match the tightened filter — skip it.
            if let Some(ref c) = self.metrics.rg_skipped { c.add(1); }
            continue;   // do NOT open a parquet stream for this RG
        }
    }
}
```

This is the analogue of `FilePruner::should_prune` (`df:pruning/src/file_pruner.rs:127-170`)
but at **row-group** granularity, which `DataSourceExec` can't do mid-file. We already hold
`self.metadata: Arc<ParquetMetaData>` (`stream.rs:438`), so per-RG min/max stats are in hand —
build a `PrunableStatistics` for `rg.index` (mirror how the page pruner / `prune_rg` already
reads column stats in `page_pruner.rs`).

Guard the rebuild behind the generation check — `build_pruning_predicate` is expensive
(`df:file_pruner.rs:128`).

### Step 5 — apply the residual during decode (Tier 1b)

When we *do* process the RG, hand the same snapshot to parquet decode by conjoining it into the
predicate passed to `create_row_selection_stream(..., push)` (`stream.rs:985`). **Respect the
existing alignment gate**: only push when `min_skip_run == 1` (row-granular) and
`!evaluator.needs_row_mask()` and `!evaluator.forbid_parquet_pushdown()` — the conditions
already computed at `stream.rs:980-983`. Otherwise apply post-decode via the mask path, or skip
(never push a residual into a block-granular selection — indices misalign, see the comment at
`stream.rs:954-979`). Simplest correct first cut: **Tier 1 = RG-skip only; defer decode-time
residual** until RG-skip is proven.

### Step 6 — honor completion & config

- When `df_tracking` reports all filters complete (`tracker.changed()` returns + subscriptions
  drain), stop calling `snapshot_generation`; pin the final `df_snapshot`.
- Gate the whole feature behind a new `DatafusionQueryConfig` flag (e.g.
  `indexed_dynamic_filter_pushdown`, default false), mirroring `indexed_pushdown_filters` /
  `force_pushdown` (`stream.rs:442-453`). Read it once into a local `bool` in `IndexedStream`.

---

## 3. Tier 2 (follow-up): intersect the threshold into the Lucene candidate bitset

For a TopK on an **indexed** column, the snapshot is `col > threshold`. Instead of only pruning
whole RGs, translate the threshold into a doc-range / range query and **intersect with the
candidate `RoaringBitmap` before `build_row_selection_with_min_skip_run`** (`stream.rs:919`).
This shrinks `matched` per RG at the source. Requires a new evaluator hook, e.g.:

```rust
trait RowGroupBitsetSource {
    /// Tighten the candidate set for this RG using a runtime residual predicate.
    /// Default no-op; indexed evaluators that can range-scan implement it.
    fn refine_candidates(&self, rg: &RowGroupInfo, residual: &Arc<dyn PhysicalExpr>,
                         candidates: &mut RoaringBitmap) {}
}
```

This is strictly more powerful than vanilla DataFusion (which the blog notes can only prune
RGs/files without late materialization). Sequence it after Tier 1 lands.

---

## 4. Ordering / "is the data sorted?" — the part that decides if this even helps

Dynamic TopK filters only pay off when the **early** RGs contain the extreme values, so later
RGs get skipped. That needs the scan to visit RGs in (roughly) sort order. Two prerequisites:

1. **`SortExec` must survive above our scan with a `fetch`.** Confirm the indexed plan actually
   produces `SortExec { fetch: Some(k) }` for `ORDER BY ... LIMIT k` (it should — we use default
   logical+physical planning from substrait, `indexed_executor.rs:807-810`). If the sort is
   instead pre-satisfied/elided, no dynamic filter is created and there's nothing to consume.
2. **RG visitation order.** We iterate RGs in `row_groups` order per chunk
   (`stream.rs:194-200`). For a DESC-on-time query the useful order may be reverse. We don't
   need perfect order for correctness (the filter only ever *removes* non-qualifying rows), but
   the *speedup* scales with how early the extremes appear. This is a tuning concern, not a
   correctness one.

**Correctness invariant (non-negotiable):** a dynamic filter may only drop rows the query would
discard anyway (the producer guarantees monotonic tightening). If `build_pruning_predicate`
fails or stats are missing for an RG, **fall back to scanning it** — never skip on uncertainty.

---

## 4b. Correctness with complex boolean (Lucene⊕parquet-split) queries

**The dynamic filter references only the sort columns, never the WHERE clause** — so the
boolean-query split between Lucene (FFM bitsets) and parquet (residual / `on_batch_mask`
refinement) is *orthogonal* to dynamic-filter pruning. Proof for the worst case
`WHERE (lucene_match(a) OR parquet_pred(b)) ORDER BY ts DESC LIMIT k`:

- `threshold` = k-th largest `ts` among rows that passed WHERE **and were emitted**.
- RG pruning uses parquet RG statistics for `ts`, which span **all** rows in the RG — a
  *superset* of the candidate (WHERE-passing) rows.
- Skip an RG only if `RG.max(ts) <= threshold`. Then
  `max(ts | candidates) <= max(ts | all rows) <= threshold`, so **no candidate in that RG can
  reach top-k**. Safe.

This is **conservative**: using all-row stats over-estimates the candidate max, so we may *miss*
a skip but can **never wrongly drop a qualifying row** — independent of how WHERE was split.

**Consequence for acceptance (Step 1):** the accept/reject decision keys on **sort columns**,
not WHERE. Accept the dynamic filter iff *every column it references* is a real, readable
parquet column **with RG statistics**. Reject (fall back, no harm) when the sort is on a
Lucene-only field, a computed expression (e.g. a relevance score), or a column lacking stats.
We never inspect the WHERE filters, the BoolNode tree, or the `index_filter(...)` UDF.

**Cases to cover explicitly in tests:**
1. Sort col is a plain parquet column with stats → prune fires.
2. Sort col not in parquet (Lucene-only / computed) → filter rejected, results unchanged.
3. Sort col in parquet but an RG has no stats → that RG is scanned (never skipped on missing stats).
4. Multi-column `ORDER BY a, b` → lexicographic dynamic predicate; only prune when provably excluded.
5. Nulls in sort col → `PruningPredicate` must handle null stats; verify equality with non-pushdown path.
6. Complex boolean WHERE (OR across Lucene+parquet leaves) + sort → identical results, RGs still pruned.
7. The `supports_filters_pushdown = Exact` FilterExec-removal path still yields `SortExec{fetch}`
   above `QueryShardExec` (verify in the probe test).

## 4d. IMPLEMENTED — final design (both phases + config)

The feature is implemented and tested. Summary of what landed:

- **Acceptance:** `QueryShardExec::handle_child_pushdown_result` (`table_provider.rs`) accepts
  a `Post`-phase dynamic filter when every referenced column is a readable parquet column,
  gated by the `indexed_dynamic_filter_pushdown` config flag.
- **Two prune phases** (`stream.rs` + `dynamic_filter.rs`):
  - **Prefetch phase** — inside the prefetch `spawn_blocking` closure (`fetch_row_group`),
    *before* `evaluator.prefetch_rg`. Skips the Lucene/FFM eval. The snapshot used is the
    filter's tightening *so far* (prefetch runs ~1 RG ahead, so it's slightly looser — still
    correct since tightening is monotonic). Counter: `dynamic_filter_rg_pruned_at_prefetch`.
  - **Poll phase** — in `poll_inner` after the RG returns, before parquet decode. Backstop
    that catches RGs which became prunable only after further tightening between prefetch and
    processing. Counter: `dynamic_filter_rg_pruned_at_poll`.
- **`DynamicRgPruner`** (`dynamic_filter.rs`) — generation-gated snapshot cache + a single-RG
  `PruningStatistics`. `current_pruning_predicate()` yields a cloneable `RgPruningContext`
  (Send) that the prefetch closure moves across threads.
- **Config flag** — `DatafusionQueryConfig.indexed_dynamic_filter_pushdown` (default **true**),
  mirrored through the FFM wire struct and the Java cluster setting
  `datafusion.indexed.dynamic_filter_pushdown` (`DatafusionSettings`, `WireConfigSnapshot`,
  default true, dynamic + node-scoped).
- **Verified:** Rust 1024 tests pass; the TopK e2e prunes 3/4 RGs split `prefetch=2, poll=1`
  with identical results to the non-pushdown path; Java `WireConfigSnapshotTests` /
  `DatafusionSettingsTests` pass (wire size 80, offset 76).

## 4c. Where to run the prune — cost placement (Lucene vs parquet)

The Lucene prefetch (expensive FFM eval) for an RG fires in
`IndexReader::poll_next_row_group` → `start_prefetch` (`stream.rs:167-200`) **before**
candidates return to `IndexedStream`. So:

- **First cut — prune in `IndexedStream::poll_inner`** (Step 4): correct and simple, but the
  Lucene prefetch for the skipped RG has *already run*; we save only parquet decode.
- **Follow-up — prune before `start_prefetch`**: move the parquet-sort-col-stats-vs-threshold
  check into `IndexReader` (thread the sort-col stats + a handle to the current snapshot down),
  so a pruned RG also skips the Lucene eval. Bigger win for complex boolean queries where the
  FFM eval dominates. Sequence after the first cut works.

## 5. What to verify before/while coding (open questions)

1. **Exact leaf-accept API.** Read a real leaf consumer's `gather_filters_for_pushdown` /
   `handle_child_pushdown_result` in `datafusion/datasource/src/` to copy the precise
   `FilterDescription`/`PushedDown` construction — the §2 snippets are structurally right but
   use placeholder method names.
2. **Does `QueryShardExec` need to be reconstructable?** `handle_child_pushdown_result` returns
   an updated node, so we need a way to build a `QueryShardExec` with one extra field. It holds
   an `ExecutionPlanMetricsSet` (not `Clone`) — add a private ctor that re-uses
   `Arc`-shared fields and a fresh/clone metrics set as appropriate.
3. **`PrunableStatistics` per RG.** Confirm how to build it from `ParquetMetaData` for a single
   `rg.index` — the page pruner (`page_pruner.rs`) already reads per-RG column stats; reuse that.
4. **Interaction with `force_pushdown` / `needs_row_mask`.** The decode-time residual (Step 5)
   must obey the existing alignment rules at `stream.rs:980-983`; start with RG-skip only.
5. **Concurrency.** With the current `CoalescePartitionsExec` fan-out, each `IndexedStream` gets
   its own tracker (good). If we later move to the sequential `flatten` topology discussed
   separately, nothing here changes — the shared `Arc<Inner>` is topology-independent.

---

## 6. Minimal first PR (recommended scope)

1. `QueryShardExec`: implement the two pushdown hooks, accept only `Post`-phase dynamic filters
   over readable columns; add `dynamic_filters` field + private ctor.
2. Thread `Arc` → `IndexedExec` → `IndexedStream`; add tracker + generation-gated snapshot.
3. **RG-skip only** (Step 4). No decode-time residual, no bitset intersection yet.
4. New off-by-default config flag.
5. Metrics: reuse `rg_skipped`; add `dynamic_filter_rg_pruned` counter and a
   `dynamic_filter_snapshots` counter to prove the path fires.
6. Test: `ORDER BY indexed_col DESC LIMIT k` over a multi-RG segment; assert later RGs are
   skipped (counter > 0) and results are identical to the non-pushdown path.

Tier 2 (bitset intersection) and Step 5 (decode-time residual) are separate follow-ups.
