# Dynamic Filters in DataFusion — and how to apply them to `indexed_table`

> Investigation against the local DataFusion checkout at `~/Documents/dev/datafusion`
> (commit `3e006c99c`, DataFusion 53.x line). File:line references are to that tree.
> Audience: the OpenSearch analytics `indexed_table` scan (`IndexedExec` / `IndexedStream`).

---

## 0. TL;DR — what this is and why we'd want it

A **dynamic filter** is a `PhysicalExpr` whose *value* is not known at plan time but is
**filled in and tightened during execution** by some upstream operator. The canonical
producers:

- **`SortExec` with a `fetch` limit (TopK)** — as the top-K heap fills, it learns "I only
  care about rows where `sort_col > <current K-th value>`" and pushes that threshold down
  to the scan. The threshold *tightens* monotonically as more batches arrive.
- **`HashJoinExec`** — once the build side is hashed, it knows the exact set / min-max
  bounds of join-key values and pushes `probe_key IN (build keys)` (or a bounds predicate)
  down to the probe-side scan.

The scan (today: Parquet `DataSourceExec`) **subscribes** to that filter and re-checks it as
it goes, so it can **skip files / row groups / pages / rows** that can no longer contribute —
*without* the plan being rebuilt.

For `indexed_table`, the prize is the same: a TopK or join above our scan could hand us a
threshold that lets us **prune row groups and shrink the Lucene candidate set per RG at
runtime**, on top of the static index predicate we already evaluate.

---

## 1. The core type: `DynamicFilterPhysicalExpr`

**File:** `datafusion/physical-expr/src/expressions/dynamic_filters/mod.rs`

It is a `PhysicalExpr` that wraps a *mutable inner expression* behind a lock plus a
broadcast channel, with a monotonic **generation counter**.

```rust
// mod.rs:62-81
pub struct DynamicFilterPhysicalExpr {
    children: Vec<Arc<dyn PhysicalExpr>>,                 // original children (sort/join keys)
    remapped_children: Option<Vec<Arc<dyn PhysicalExpr>>>,// per-consumer column remap
    inner: Arc<RwLock<Inner>>,                            // the live, mutable expr
    state_watch: watch::Sender<FilterState>,              // tokio broadcast of changes
    data_type: Arc<RwLock<Option<DataType>>>,
    nullable: Arc<RwLock<Option<bool>>>,
}

// mod.rs:92-104
pub struct Inner {
    pub expression_id: u64,   // stable across updates
    pub generation: u64,      // ++ on every update()
    pub expr: Arc<dyn PhysicalExpr>,
    pub is_complete: bool,    // no further updates coming
}
```

### Producer API (the operator that *tightens* the filter)

```rust
// mod.rs:247-275 — push a new, tighter predicate
pub fn update(&self, new_expr: Arc<dyn PhysicalExpr>) -> Result<()>;
// mod.rs:281-291 — "I will never update again"
pub fn mark_complete(&self);
```

`update()` takes the write lock, bumps `generation`, swaps `expr`, drops the lock, then
broadcasts on the `watch` channel. `mark_complete()` lets consumers stop polling.

### Consumer API (the scan)

```rust
// mod.rs:236-239 — current concrete expr (with this consumer's column remap applied)
pub fn current(&self) -> Result<Arc<dyn PhysicalExpr>>;

// PhysicalExpr trait impls:
// mod.rs:538-541
fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> { Ok(Some(self.current()?)) }
// mod.rs:543-546
fn snapshot_generation(&self) -> u64 { self.inner.read().generation }
```

### Why `snapshot()` matters: flattening for pruning

Pruning code can't reason about an opaque dynamic node, so it **snapshots** the whole
predicate tree into a concrete expression first:

```rust
// physical-expr-common/src/physical_expr.rs:962-972
pub fn snapshot_physical_expr_opt(expr) -> Result<Transformed<...>> {
    expr.transform_up(|e| match e.snapshot()? {
        Some(s) => Ok(Transformed::yes(s)),   // dynamic node -> its current concrete expr
        None    => Ok(Transformed::no(e)),    // static node  -> unchanged
    })
}
```

And to cheaply tell whether *anything* changed without re-flattening, generations are XOR/
summed across the tree:

```rust
// physical-expr-common/src/physical_expr.rs:982-992
pub fn snapshot_generation(expr) -> u64 {           // 0 for a fully-static tree
    let mut g = 0u64;
    expr.apply(|e| { g = g.wrapping_add(e.snapshot_generation()); Continue });
    g
}
```

### Change detection without polling locks: the tracker

**File:** `datafusion/physical-expr/src/expressions/dynamic_filters/tracker.rs`

```rust
// tracker.rs:54-66
pub enum DynamicFilterTracking {
    Static,                        // no dynamic filters at all
    AllComplete,                   // all dynamic filters already finalized
    Watching(DynamicFilterTracker) // at least one can still move
}
// tracker.rs:71-95   classify(predicate) -> walks tree once, subscribes to incomplete filters
// tracker.rs:135-144 changed()         -> cheap atomic has_changed() per subscription
```

`changed()` is a `watch::Receiver::has_changed()` check (atomic, no lock in the common case)
and auto-drops subscriptions as filters complete. This is the steady-state hot-path check.

**Thread-safety model:** `parking_lot::RwLock` for `Inner` (brief read lock to clone the
`Arc<expr>`; brief write lock to swap on update). Cross-thread notification via a
`tokio::sync::watch` channel. Equality/hash use `Arc::as_ptr(&inner)` (pointer identity) so
the expr stays usable as a map key even after its inner value changes.

---

## 2. How the filter gets *wired into the plan*: the two-phase pushdown optimizer

**Files:** `datafusion/physical-optimizer/src/filter_pushdown.rs`,
`datafusion/physical-plan/src/filter_pushdown.rs`,
`datafusion/physical-plan/src/execution_plan.rs`

Pushdown happens in two passes, distinguished by a phase enum:

```rust
// physical-plan/src/filter_pushdown.rs:48-88
pub enum FilterPushdownPhase {
    Pre,   // static filters (WHERE col = 5) — before other rewrites
    Post,  // dynamic filters (TopK / join) — AFTER the tree is structurally stable
}
```

Dynamic filters are **only** pushed in the `Post` phase, because they hold `Arc` references
into specific plan nodes; pushing earlier would risk those references being invalidated by
later rewrites. After `Post`, only `with_new_children` runs, which preserves the shared
`Arc<Inner>`.

The two `ExecutionPlan` trait hooks that drive it:

```rust
// physical-plan/src/execution_plan.rs:581-680 (default impls shown)
fn gather_filters_for_pushdown(
    &self, phase: FilterPushdownPhase,
    parent_filters: Vec<Arc<dyn PhysicalExpr>>, config: &ConfigOptions,
) -> Result<FilterDescription>;            // "which parent filters go to which child,
                                           //  and what self-filters do I add?"

fn handle_child_pushdown_result(
    &self, phase: FilterPushdownPhase,
    child_pushdown_result: ChildPushdownResult, config: &ConfigOptions,
) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>>;  // upward pass: what did the
                                                                 // child accept? rebuild me.
```

`FilterDescription` / `ChildFilterDescription` carry, per child, the routed `parent_filters`
(each tagged `PushedDown::Yes/No`) plus any `self_filters` the node injects
(`physical-plan/src/filter_pushdown.rs:295-555`).

### Producer side — what TopK does

```rust
// physical-plan/src/sorts/sort.rs — SortExec
fn create_filter(&self) -> Arc<RwLock<TopKDynamicFilters>> {           // sort.rs:911-920
    let children = self.expr.iter().map(|s| Arc::clone(&s.expr)).collect();
    Arc::new(RwLock::new(TopKDynamicFilters::new(
        Arc::new(DynamicFilterPhysicalExpr::new(children, lit(true))), // initial = lit(true)
    )))
}
fn gather_filters_for_pushdown(&self, phase, parent_filters, config) {  // sort.rs:1336-1366
    if phase == Post && self.fetch.is_some()
        && config.optimizer.enable_topk_dynamic_filter_pushdown {
        child = child.with_self_filter(self.filter.read().expr());      // inject the dynamic expr
    }
    ...
}
```

At runtime `TopK::update_filter()` (`topk/mod.rs:352-421`) reads the heap max, builds
`col > threshold (OR tie-break)` and calls `filter.expr.update(pred)` each time the heap
tightens.

### Producer side — what HashJoin does

```rust
// joins/hash_join/exec.rs:1636-1651
if phase == Post && self.dynamic_filter.is_none()
    && self.allow_join_dynamic_filter_pushdown(config) {
    let df = Self::create_dynamic_filter(&self.on);    // DynamicFilterPhysicalExpr::new(right_keys, lit(true))
    right_child = right_child.with_self_filter(df);    // push to PROBE side
}
// exec.rs:1658-1687  handle_child_pushdown_result: if the probe child accepted it,
//                    downcast back to DynamicFilterPhysicalExpr and store the handle.
```

The build→probe runtime coordination (build hash table → compute membership/bounds →
`dynamic_filter.update(expr)` → `mark_complete()` → `Notify` waiters) lives in
`joins/hash_join/shared_bounds.rs:354-693`.

### Consumer side — what a leaf scan must do

A scan that wants to *accept* a dynamic filter implements the same two hooks:

1. `gather_filters_for_pushdown`: return a `ChildFilterDescription` that marks the incoming
   filters it can handle as supported (`from_child(...)`).
2. `handle_child_pushdown_result`: pull the accepted filters out of the result, **store them
   in a rebuilt copy of the node**, and return that node via
   `FilterPushdownPropagation::...with_updated_node(new_self)`.

The scan keeps the `Arc<dyn PhysicalExpr>` (which *is* the shared dynamic filter) and reads it
at execution time. Because the `Arc<Inner>` is shared with the producer, updates are seen with
no further plan surgery.

---

## 3. How the Parquet scan *consumes* it at runtime (the part we copy)

**Files:** `datafusion/datasource-parquet/src/opener/mod.rs`,
`datafusion/datasource-parquet/src/opener/early_stop.rs`,
`datafusion/pruning/src/file_pruner.rs`,
`datafusion/pruning/src/pruning_predicate.rs`

There are **two distinct consumption points**, at different granularities:

### (a) At file/row-group/page open — snapshot once

When the opener prepares a file, building a `PruningPredicate` flattens the predicate,
unraveling dynamic nodes to their *current* value:

```rust
// pruning/src/pruning_predicate.rs:460-468
// In particular this unravels any DynamicFilterPhysicalExpr by snapshotting them
let tf = snapshot_physical_expr_opt(expr)?;
```

That snapshot then feeds:
- **Row-group pruning** via `row_groups.prune_by_statistics(...)` (`opener/mod.rs:1069-1095`).
- **Page-index pruning** via `page_pruning_predicate.prune_plan_with_page_index_...`
  (`opener/mod.rs:1283-1302`).
- **Row-level late-materialization filter** (`RowFilterGenerator`, `opener/mod.rs:1354-1404`)
  — note this passes the *original* predicate (still containing the dynamic node) to arrow-rs's
  `RowFilter`, but its value is effectively pinned at this point in the file's life.

So the value used for a given file is **whatever the filter held when that file was opened**.

### (b) During the scan — re-check per batch via `FilePruner` + `EarlyStoppingStream`

This is the mechanism that lets a *tightening* filter abandon a file mid-scan.

```rust
// pruning/src/file_pruner.rs
pub struct FilePruner {
    predicate: Arc<dyn PhysicalExpr>,   // ORIGINAL (keeps the dynamic node)
    tracking: DynamicFilterTracking,    // Static | AllComplete | Watching(...)
    checked_once: bool,
    file_schema: SchemaRef,
    file_stats_pruning: PrunableStatistics,
    predicate_creation_errors: Count,
}

// file_pruner.rs:127-170 — the hot check
pub fn should_prune(&mut self) -> Result<bool> {
    let should_build = if self.checked_once {
        self.tracking.watcher().is_some_and(|w| w.changed())  // cheap atomic; only true on a real update
    } else { self.checked_once = true; true };
    if !should_build { return Ok(false); }

    let pp = build_pruning_predicate(Arc::clone(&self.predicate), &self.file_schema, ...);
    match pp.prune(&self.file_stats_pruning) {                 // file-level min/max stats
        Ok(values) if values.into_iter().all(|v| !v) => Ok(true),  // file can't match anymore -> prune
        _ => Ok(false),
    }
}
```

`FilePruner::try_new` returns `None` (no wrapper) when the file has no stats *and* the
predicate has no dynamic filter (`file_pruner.rs:88-116`). `is_watching()` is true only when
some dynamic filter can still move.

The per-batch driver:

```rust
// datasource-parquet/src/opener/early_stop.rs:35-108
struct EarlyStoppingStream<S> { file_pruner: FilePruner, inner: S, done: bool, ... }
impl Stream for EarlyStoppingStream {
    fn poll_next(...) {
        // for each batch from inner:
        if self.file_pruner.should_prune()? {   // dynamic filter moved -> can we abandon the file?
            self.done = true; Poll::Ready(None)  // stop reading this file entirely
        } else { Poll::Ready(Some(batch)) }
    }
}
// only wrapped when file_pruner.is_watching() — opener/mod.rs:1437-1447
```

### Granularity summary (important for our design)

| Decision | When the dynamic value is read | Re-tightens mid-file? |
|---|---|---|
| File prune at open | once, at file open (snapshot) | no |
| Row-group prune | once, at file open (snapshot) | no |
| Page prune | once, at file open (snapshot) | no |
| Row filter (decode) | value pinned at file open | no |
| **`FilePruner::should_prune` (early stop)** | **per batch, only when `changed()`** | **yes — but only file-level abandon** |

So upstream DataFusion's runtime re-tightening is **coarse**: within a file it can only decide
"abandon the whole remaining file," not "re-prune individual row groups." Finer-grained
re-tightening within a file is exactly the gap an indexed scan could improve on.

---

## 4. Applying this to `indexed_table`

Our scan is `QueryShardExec` → (per chunk) `IndexedExec` → `IndexedStream`, which drives an
evaluator RG-by-RG and reads parquet through `parquet_bridge`. Two integration tiers:

### Tier 1 — accept a snapshot, prune at RG boundaries (low risk, high value)

This mirrors `FilePruner` but at *row-group* granularity, which we already iterate.

1. **Accept the filter (plan wiring).** Implement `gather_filters_for_pushdown` /
   `handle_child_pushdown_result` on `QueryShardExec` (and thread the accepted
   `Arc<dyn PhysicalExpr>` into each `IndexedExec`, alongside the existing
   `predicate: Option<Arc<dyn PhysicalExpr>>`). Mark only filters over **sort/join key
   columns that exist in our schema** as `PushedDown::Yes`; everything else `No`.
   - Critical: respond in the **`Post`** phase only. Returning `Yes` in `Pre` for a dynamic
     filter is wrong.

2. **Subscribe once.** When `IndexedStream` is constructed, build a
   `DynamicFilterTracking::classify(&predicate_including_dynamic)`. Store the
   `Watching(tracker)` (or note `Static`/`AllComplete`).

3. **Re-snapshot at each RG.** In `poll_inner`, right before/after
   `poll_next_row_group` (`stream.rs:853`), if `tracker.changed()` (or
   `snapshot_generation(&pred)` differs from last seen), call
   `snapshot_physical_expr(&pred)` to get the current concrete predicate, and:
   - Build/refresh a `PruningPredicate` and test it against **this RG's** parquet
     statistics (we already hold `Arc<ParquetMetaData>` in `IndexedStream`). If the RG
     can't match, skip it (increment `rg_skipped`) — same as a `prefetch_rg == None`.
   - This gives us what DataFusion can't: **per-RG** runtime tightening, not just
     whole-file abandon.

4. **Hand the residual to parquet decode.** The snapshotted concrete predicate can also be
   passed into `create_row_selection_stream(..., push=true)` so arrow-rs applies it during
   decode for the current RG — reusing the existing `push` plumbing
   (`stream.rs:980-985`). Re-snapshot per RG so each RG's stream gets the latest threshold.

### Tier 2 — intersect with the Lucene candidate bitset (bigger win, more work)

For a TopK on an indexed column, the dynamic predicate is `col > threshold`. If that column
is indexed, we can translate the threshold into a doc-range / range query and **intersect it
with the candidate `RoaringBitmap` before building the RowSelection** (in or just after
`prefetch_rg`). That shrinks `matched` rows per RG at the source, not just at decode. This
requires the evaluator to expose a "tighten with this residual predicate" hook and is a
follow-up once Tier 1 is proven.

### Where the value lands

- **TopK / ORDER BY ... LIMIT k** over an indexed segment: later RGs get pruned or
  row-selected away once the heap fills — big reads avoided.
- **Joins** where our indexed table is the probe side: build-side keys become a runtime
  range/membership filter on our scan.

---

## 5. Gotchas / decisions for our implementation

1. **Sequential vs concurrent execution.** Dynamic re-snapshotting assumes a stream that
   makes forward progress and periodically re-reads the filter. Our current
   `CoalescePartitionsExec` fan-out (see `table_provider.rs:464-471`) runs segment chunks
   concurrently into shared metric atomics — fine for correctness of dynamic filters (the
   `Arc<Inner>` is shared and thread-safe), but each `IndexedStream` must own its own
   `DynamicFilterTracker` (subscriptions are per-consumer). Don't share a tracker across
   chunk streams.

2. **`needs_row_mask` / pushdown alignment.** We already gate parquet pushdown on
   `min_skip_run` and `evaluator.needs_row_mask()` (`stream.rs:980-983`). A dynamically
   snapshotted residual must obey the **same alignment rules** — if we hand it to parquet
   while also building a `current_mask` over block-granular selection, indices misalign.
   Safest first cut: apply the dynamic residual only in the **row-granular
   (`min_skip_run == 1`) pushdown path**, or post-decode via the evaluator, never both.

3. **Snapshot cost.** `build_pruning_predicate` is "expensive" (DataFusion's own comment,
   `file_pruner.rs:128`). Guard every rebuild behind `tracker.changed()` /
   `snapshot_generation` — never rebuild per batch unconditionally.

4. **Completeness.** Honor `is_complete`: once the producer calls `mark_complete()`, stop
   polling and pin the final snapshot. `DynamicFilterTracker` drops completed subscriptions
   automatically, so steady-state cost goes to ~zero.

5. **Config gating.** DataFusion gates these behind
   `optimizer.enable_topk_dynamic_filter_pushdown` (and a join equivalent). Add an analogous
   `DatafusionQueryConfig` flag so we can A/B and disable on regressions, consistent with how
   `indexed_pushdown_filters` / `force_pushdown` are handled today.

6. **Correctness invariant.** A dynamic filter pushdown must only ever **remove rows the
   query would discard anyway** (the producer guarantees monotonic tightening). If we ever
   can't prove a RG is fully excluded, fall back to scanning it — never drop a candidate on
   an uncertain snapshot.

---

## 5b. The TopK story end-to-end (from the DataFusion blog, 2025-09-10)

Source: <https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/>. This is the
canonical worked example and matches the code traced above.

**Motivating query** — the "last K rows" pattern:

```sql
SELECT * FROM records ORDER BY start_timestamp DESC LIMIT 1000;
-- ClickBench Q23: SELECT * FROM hits WHERE "URL" LIKE '%google%' ORDER BY "EventTime" LIMIT 10;
```

`TopK` keeps only `K` rows in a heap instead of fully sorting — but historically had **no
early termination**: it still read the whole table to prove nothing bigger existed. Dynamic
filters fix exactly that.

**Threshold construction & tightening.** With `DESC LIMIT 3`, once the heap holds 3 rows the
smallest of them is the threshold; nothing `<=` it can enter the top 3, so the filter is
`start_timestamp > '<heap-min>'`. It starts life as `lit(true)` (the placeholder), and the
TopK operator **monotonically tightens** it as better values arrive — each tightening is an
`update()` that bumps `generation`.

**Pushdown is transparent.** The filter is just an `Arc<dyn PhysicalExpr>` sitting in the
`predicate` field of `DataSourceExec`. `EXPLAIN` before execution shows `true`; after, it
shows the tightened predicate (e.g. `EventTime < 1372713773.0`). Existing scan pushdown logic
needs no special-casing — that's the whole point of hiding mutation behind a normal
`PhysicalExpr`.

**Two trait methods make it work** (the same ones traced in §1):
- `snapshot_generation()` — cheaply detect that the filter tree changed, so the scan knows
  when to re-evaluate against file / row-group statistics. This is what enables **early
  termination of a file mid-scan**.
- `snapshot()` — collapse the dynamic node to its current static value for stats-pruning /
  serialization.

**Crucial granularity caveat (directly relevant to us):**

> "With Late Materialization, filters apply during the scan; **without it, dynamic filters can
> only prune row groups or entire files.**"

In other words, vanilla Parquet gets *within-file row-level* benefit only through late
materialization (the row filter during decode); otherwise the runtime tightening is limited to
row-group/file skipping. **`indexed_table` is positioned better than this baseline**: because
we already iterate RG-by-RG *and* hold a Lucene candidate bitset, we can apply a tightened
threshold at RG granularity (Tier 1) and at row granularity via bitset intersection (Tier 2)
without depending on arrow-rs late materialization semantics.

**Why not the simpler "sorted-data" approach.** The blog contrasts with IOx's
`ProgressiveEvalExec`, which requires *already-sorted* input. Real datasets are usually only
*roughly* sorted (insert order ≈ timestamp order), so DataFusion chose the general dynamic-
filter mechanism. Our segments are likewise only roughly ordered, so the same reasoning
applies — we want the general mechanism, not an assumption of sorted RGs.

**Performance evidence (motivation for doing this at all):**

| Scenario | Before | After | Speedup |
|---|---|---|---|
| Logfire `ORDER BY ts DESC LIMIT` | — | — | >10× |
| ClickBench Q23, 1 core (filter + late mat.) | 32.04s | 1.42s | ~22× |
| ClickBench Q23, 12 cores | 5.04s | 0.602s | ~8× |
| Hash join (DF 49 → 50 + both opts) | 2.5s | 0.1s | ~25× |

The big wins land when the top values appear in the **first** files/RGs read (insert order
roughly matches sort order) — the rest can then be skipped wholesale. That is the common shape
for time-ordered OpenSearch indices, which is why this is worth pursuing for `indexed_table`.

## 6. Key file:line index (DataFusion)

| Concern | Location |
|---|---|
| `DynamicFilterPhysicalExpr` struct | `physical-expr/src/expressions/dynamic_filters/mod.rs:62-104` |
| `update()` / `mark_complete()` | `…/dynamic_filters/mod.rs:247-291` |
| `current()` / `snapshot()` / `snapshot_generation()` | `…/dynamic_filters/mod.rs:236-239, 538-546` |
| `with_new_children` shares inner Arc | `…/dynamic_filters/mod.rs:459-472` |
| Tracker: classify / changed | `…/dynamic_filters/tracker.rs:54-95, 135-144` |
| `snapshot_physical_expr_opt` / `snapshot_generation` helpers | `physical-expr-common/src/physical_expr.rs:962-992` |
| `snapshot()` / `snapshot_generation()` trait defaults | `physical-expr-common/src/physical_expr.rs:413-436` |
| `FilterPushdownPhase` (Pre/Post) | `physical-plan/src/filter_pushdown.rs:48-88` |
| `FilterDescription` / `PushedDown` | `physical-plan/src/filter_pushdown.rs:295-555` |
| `gather_filters_for_pushdown` / `handle_child_pushdown_result` | `physical-plan/src/execution_plan.rs:581-680` |
| Pushdown optimizer driver | `physical-optimizer/src/filter_pushdown.rs:440-597` |
| TopK create/push/update filter | `physical-plan/src/topk/mod.rs:352-421`; `sorts/sort.rs:911-920, 1336-1419` |
| HashJoin create/accept filter | `joins/hash_join/exec.rs:1636-1687`; `shared_bounds.rs:354-693` |
| Snapshot at file open (pruning) | `pruning/src/pruning_predicate.rs:460-468` |
| `FilePruner` struct + `should_prune` | `pruning/src/file_pruner.rs:88-170` |
| Row-group / page / row-filter use | `datasource-parquet/src/opener/mod.rs:1069-1095, 1283-1302, 1354-1404` |
| `EarlyStoppingStream` per-batch recheck | `datasource-parquet/src/opener/early_stop.rs:35-108`; wired at `opener/mod.rs:1437-1447` |
