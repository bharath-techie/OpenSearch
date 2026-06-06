# Porting DataFusion #21351 ("Dynamic work scheduling in FileStream") to the indexed table

> How DataFusion's cross-partition work-stealing works, why it cannot be inherited
> by upgrading, and a concrete plan to reimplement the same idea on **our** scan
> (`QueryShardExec` → `IndexedExec` → `IndexedStream`).
>
> Companion to `dynamic-filters-indexed-table-impl.md` — that one tightens *which
> rows* survive; this one rebalances *which partition does the work*. They compose.
>
> File:line refs are to our tree unless prefixed `df:` (= `~/Documents/dev/datafusion`,
> currently checked out at `main` past the merge of #21351 + #21956).

---

## 0. TL;DR — the two upfront facts

1. **You cannot get this by upgrading.** PR #21351 modifies `FileStream`,
   `FileScanConfig`, and `DataSourceExec` (`df:datasource/src/file_stream/*`,
   `df:datasource/src/source.rs`). **Our indexed scan never touches any of them** —
   it is a parallel, hand-rolled `ExecutionPlan` (`QueryShardExec`) that talks to
   parquet directly. Upgrading DataFusion gives work-stealing to *DataSource-based*
   scans for free, but our scan reimplements the read path, so we reimplement the
   pattern regardless of version. **This is a pattern port, not a dependency flip.**

2. **The upgrade to 54.0 is mostly orthogonal — but worth doing for one reason.**
   Work-stealing only pays off if the *most promising* work is at the front of the
   shared queue. PR **#21956** ("globally reorder files and row groups by statistics
   for TopK", merged right after #21351 — `df:102da39c7`) adds
   `FileSource::reorder_files` (`df:datasource/src/file.rs:288`) and
   `reorder_files_by_min_statistics` (`df:datasource-parquet/src/sort.rs:144`), which
   is what `SharedWorkSource::from_config` calls before filling the queue
   (`work_source.rs:99`). We don't consume `FileSource`, so we'd port the *ordering
   idea* too — but having the reference in-tree is why bumping the checkout helps.
   **The current workspace pins DataFusion `=53.1.0`, Arrow `=58.2.0`**
   (`sandbox/libs/dataformat-native/rust/Cargo.toml:19-33`); the local DF checkout is
   `53.1.0` + unreleased main commits. See §6 on the upgrade.

---

## 1. What #21351 actually does (read from the merged source, not the PR text)

**The problem.** A `DataSourceExec` over N files is planned into P partitions, and the
file→partition mapping is **frozen at planning time** (`FileScanConfig.file_groups`,
one group per partition). If partition A's files turn out cheap (dynamic filter prunes
them, or they're small) and partition B's are expensive, A finishes and **sits idle**
while B grinds. Worse with dynamic filtering, which is exactly when imbalance spikes.

**The fix: a shared queue siblings steal from.** Instead of each `FileStream` owning a
fixed `VecDeque<PartitionedFile>`, reorderable scans share **one** queue across all
sibling streams of a single execution. Whichever stream goes idle first pops the next
file. Mapping becomes dynamic; the slow partition stops being a tail.

### 1.1 The pieces (all in the local checkout)

**`WorkSource`** (`df:datasource/src/file_stream/work_source.rs:31`) — per-stream handle:

```rust
pub(super) enum WorkSource {
    Local(VecDeque<PartitionedFile>),  // order-sensitive: keep private
    Shared(SharedWorkSource),          // reorderable: steal from the pool
}
fn pop_front(&mut self) -> Option<PartitionedFile>   // local pop  OR  shared steal
fn skipped_on_limit(&self) -> usize                  // Local: files.len(); Shared: 0
```

**`SharedWorkSource`** (`work_source.rs:67`) — the pool itself, `Clone` (shares an `Arc`):

```rust
pub(crate) struct SharedWorkSource { inner: Arc<SharedWorkSourceInner> }
struct SharedWorkSourceInner { files: parking_lot::Mutex<VecDeque<PartitionedFile>> }

fn from_config(config: &FileScanConfig) -> Self {        // build once per execution
    let files = config.file_groups.iter().flat_map(FileGroup::iter).cloned().collect();
    let files = config.file_source.reorder_files(files);  // <-- #21956: best work first
    Self::new(files)
}
fn pop_front(&self) -> Option<PartitionedFile> {          // the steal
    self.inner.files.lock().pop_front()
}
```

**The synchronization is deliberately boring:** a single
`parking_lot::Mutex<VecDeque<…>>`. No atomics, no channels, no lock-free deque. Pops
are O(1) under a short critical section; contention is negligible because the queue is
touched once per *file*, not per *row* or *batch*.

### 1.2 How the shared state reaches each stream

A `OnceLock` on the exec node, lazily initialized on first `execute()`, then handed to
every partition via a new `open_with_args` entry point
(`df:datasource/src/source.rs`):

```rust
// DataSourceExec
execution_state: Arc<OnceLock<Option<Arc<dyn Any + Send + Sync>>>>,   // :326

fn execute(&self, partition, context) -> ... {
    let shared_state = self.execution_state
        .get_or_init(|| self.data_source.create_sibling_state())       // :393 — built ONCE
        .clone();
    let args = OpenArgs::new(partition, ctx).with_shared_state(shared_state);
    self.data_source.open_with_args(args)                              // :396-398
}

// DataSource trait (default no-op so every other source is unaffected)
fn create_sibling_state(&self) -> Option<Arc<dyn Any + Send + Sync>> { None }  // :248
fn open_with_args(&self, args) -> ... { self.open(args.partition, args.context) } // :256
```

`FileScanConfig::create_sibling_state` returns `Some(Arc::new(SharedWorkSource::from_config(self)))`
**unless** `preserve_order || partitioned_by_file_group` — i.e. order-sensitive scans
opt out and keep `WorkSource::Local`. `open_with_args` downcasts the `Arc<dyn Any>`
back to `SharedWorkSource` and feeds the builder.

### 1.3 The three invariants that make it correct

1. **The queue is built exactly once per execution** (the `OnceLock`), so all siblings
   share *one* `Arc<Mutex<VecDeque>>`, not P copies.
2. **Only reorderable scans share.** If output order matters (`preserve_order`), stealing
   would scramble it → the scan opts out. **This is the load-bearing safety gate.**
3. **Limit accounting de-double-counts.** When an early `LIMIT` stops a stream, a `Local`
   source reports its leftover files as "skipped", but a `Shared` source reports `0` —
   otherwise P siblings each claim the *same* shared leftovers (`skipped_on_limit`).

---

## 2. Why our scan can't inherit it — and what maps onto what

Our execution model (confirmed in `table_provider.rs` / `stream.rs` / `partitioning.rs`):

```
QueryShardExec                              ← Partitioning::UnknownPartitioning(assignments.len())
  assignments: Vec<PartitionAssignment>     ← FROZEN at scan() time (partitioning.rs:50)
  execute(partition):                       ← table_provider.rs:456
     assignment = assignments[partition]    ← this partition's fixed RG set
     for chunk in assignment.chunks:        ← one IndexedExec per (segment, RG-subset)
        IndexedExec { row_groups, ... }     ← stream.rs:326
          .execute() -> IndexedStream       ← stream.rs:463; iterates its RGs sequentially
     chain streams with futures::flatten    ← table_provider.rs:549 (sequential, no union)
```

The structural parallel to DataFusion:

| DataFusion (#21351)                     | Indexed table (this port)                                  |
|-----------------------------------------|------------------------------------------------------------|
| `PartitionedFile` (unit of work)        | **`SegmentChunk`** or a single RG (`partitioning.rs:31`)   |
| `FileScanConfig.file_groups` (frozen)   | `QueryShardExec.assignments` (frozen, `partitioning.rs:50`)|
| `FileStream` (1 per partition)          | the per-`execute(partition)` stream chain (`tp.rs:483`)    |
| `WorkSource::Local / Shared`            | **NEW** `IndexedWorkSource` enum                            |
| `SharedWorkSource` (`Arc<Mutex<deque>>`)| **NEW** `SharedChunkQueue` (`Arc<Mutex<VecDeque<WorkItem>>>`)|
| `DataSourceExec.execution_state OnceLock` | **NEW** `QueryShardExec.shared_queue: OnceLock<…>`       |
| `create_sibling_state` / `open_with_args` | inline in `QueryShardExec::execute` (we own both ends)   |
| `preserve_order` opt-out                | **our existing order constraint** — see §4, the crux       |
| `reorder_files` (#21956)                | reorder chunks by stat before enqueue (Tier 2)             |

**Key simplification: we don't need the trait dance.** DataFusion needs
`create_sibling_state` / `open_with_args` / `with_new_state` because `DataSource` is a
public extension trait with many impls that must stay source-compatible. **We own
`QueryShardExec` outright** — we can put the `OnceLock<Option<SharedChunkQueue>>`
directly on the struct and read it at the top of `execute()`. No new trait surface.

---

## 3. The design

### 3.1 Work item & queue

```rust
// partitioning.rs (or a new work_source.rs alongside it)

/// One stealable unit. A SegmentChunk already carries (segment_idx, doc range,
/// RG indices) — reuse it verbatim as the unit of work.
type WorkItem = SegmentChunk;

#[derive(Clone, Debug)]
pub(crate) struct SharedChunkQueue {
    inner: Arc<Mutex<VecDeque<WorkItem>>>,   // std::sync::Mutex is fine; or parking_lot
}

impl SharedChunkQueue {
    fn from_assignments(assignments: &[PartitionAssignment]) -> Self {
        // Flatten EVERY partition's chunks into one queue.
        let items = assignments.iter().flat_map(|a| a.chunks.iter().cloned());
        Self { inner: Arc::new(Mutex::new(items.collect())) }
        // Tier 2: reorder by per-chunk min/max stat here (see §5).
    }
    fn pop(&self) -> Option<WorkItem> { self.inner.lock().unwrap().pop_front() }
}

enum IndexedWorkSource {
    Local(VecDeque<WorkItem>),   // this partition's own chunks, in order
    Shared(SharedChunkQueue),    // steal from the global pool
}
```

`std::sync::Mutex` is adequate (we lock once per chunk, and a chunk is many RGs of
work); `parking_lot` only if we want to match upstream exactly or drop poisoning.

### 3.2 Build the queue once, on `QueryShardExec`

```rust
// table_provider.rs — new field
shared_queue: OnceLock<Option<SharedChunkQueue>>,   // None when stealing is disabled

// at the TOP of execute(), before the per-chunk loop:
let shared = self.shared_queue.get_or_init(|| {
    (self.work_stealing_enabled() && self.is_reorderable())
        .then(|| SharedChunkQueue::from_assignments(&self.assignments))
});

let work: IndexedWorkSource = match shared {
    Some(q) => IndexedWorkSource::Shared(q.clone()),               // share the Arc
    None     => IndexedWorkSource::Local(                          // today's behavior
        self.assignments[partition].chunks.iter().cloned().collect()
    ),
};
```

`OnceLock` guarantees one queue across all `execute(partition)` calls of this node.
`QueryShardExec` is cheaply rebuilt elsewhere already (the dynamic-filter port adds a
ctor), so adding `OnceLock` — which is not `Clone` but is `Default` — needs a fresh
empty one wherever the node is reconstructed (mirror `DataSourceExec::reset_state`,
`df:source.rs:536`).

### 3.3 Drain via the work source instead of a fixed chunk loop

The current loop walks `assignment.chunks` (`tp.rs:485`). Replace the *driver* so it
pulls from `work`:

```rust
let mut streams = Vec::new();
while let Some(chunk) = work.pop_front() {     // Local: own chunks; Shared: steal
    let segment = &self.config.segments[chunk.segment_idx];
    // ... build IndexedExec exactly as today (tp.rs:514-535) ...
    streams.push(exec.execute(0, Arc::clone(&context))?);
}
// chaining unchanged (tp.rs:539-552)
```

**Subtlety — eager vs. lazy.** Today the loop builds *all* of a partition's streams
eagerly, then chains them. With stealing, a partition shouldn't drain the whole shared
queue up front (that just re-freezes the assignment!). Two options:

- **3.3a Lazy (correct, recommended).** Don't pop in a `while` loop at `execute` time.
  Instead build a stream that pops **one chunk at a time, lazily**: an outer
  `futures::stream::unfold(work, |w| async { w.pop_front().map(build_and_exec) })`
  flattened into the batch stream. A sibling only takes its *next* chunk when it has
  finished the current one — that is what produces real load-balancing.
- **3.3b Eager (wrong for stealing).** Popping all chunks in `execute` reproduces the
  static split. Only acceptable as a no-op fallback when stealing is disabled.

The lazy driver is the heart of the port. Sketch:

```rust
let driver = futures::stream::unfold(
    (work, self.clone_for_exec(), context),
    |(mut work, exec_tmpl, ctx)| async move {
        let chunk = work.pop_front()?;                 // steal point
        let stream = exec_tmpl.build_indexed_exec(chunk, &ctx)?.execute(0, ctx.clone())?;
        Some((stream, (work, exec_tmpl, ctx)))
    },
).flatten();                                           // Stream<Stream<Batch>> -> Stream<Batch>
Ok(Box::pin(RecordBatchStreamAdapter::new(self.projected_schema.clone(), driver)))
```

This keeps **one outstanding chunk per partition** — the exact "single outstanding I/O
per partition" property #21351 preserves (it explicitly did *not* add multi-I/O).

---

## 4. The crux: are we even allowed to steal? (ordering & dynamic-filter interplay)

This is where the port lives or dies, mirroring #21351's `preserve_order` gate.

**4.1 When order must be preserved → DO NOT share.** If the plan above us relies on the
scan emitting chunks/segments in a particular order (e.g. a `SortPreservingMerge`, or
any consumer that assumes per-partition ordering), stealing scrambles emission order
across partitions. **Gate: only enable `Shared` when the scan's output ordering is
unconstrained** — the analogue of `create_sibling_state` returning `None` under
`preserve_order`. Concretely: check whether `QueryShardExec`'s `PlanProperties` /
equivalence properties advertise an output ordering, and whether the parent is order-
sensitive. When in doubt, **stay `Local`** (today's behavior, always correct).

**4.2 Interaction with the dynamic-filter port (the doc next door).** These two
features are *complementary and were co-designed upstream* — work-stealing matters
**most** precisely when dynamic filtering makes partitions wildly uneven. But note a
real interaction:

- The dynamic filter (`DynamicRgPruner`, per `IndexedStream`) tightens as the TopK heap
  fills. If chunks are stolen **out of assignment order**, the pruning threshold a
  stealing stream sees still applies correctly (the filter only ever *removes*
  non-qualifying rows — §4 of the dynamic-filter doc proves this is order-independent).
  So **stealing never breaks dynamic-filter correctness.**
- But stealing can *blunt* it: the win from a DESC-TopK comes from visiting high-value
  RGs **early** so the threshold rises fast. If a stealing sibling grabs a low-value
  chunk first, it does wasted work the filter would later have pruned. **Mitigation =
  Tier 2 reorder (§5):** put high-value chunks at the front of the shared queue, so
  stealing and dynamic-filter pruning pull in the same direction.

**4.3 The hard case: ORDER BY + LIMIT (the most valuable case) is also order-sensitive.**
A `SortExec{fetch=k}` above us re-sorts anyway, so the scan's *emission* order doesn't
need preserving — the sort fixes it. **So TopK queries are both the biggest
beneficiary AND safe to steal**, as long as the consumer is the `SortExec` (which
re-orders) and not a `SortPreservingMerge` (which assumes pre-sorted inputs). Verify
which topology our indexed TopK plans produce — the dynamic-filter probe test
(`dynamic_filter_probe.rs`) already asserts `SortExec{fetch}` lands above
`QueryShardExec`, so we likely get the safe one. **Confirm this in the probe before
enabling by default.**

---

## 5. Tiers (ship incrementally)

**Tier 1 — shared queue, FIFO, gated off by default.**
`SharedChunkQueue` + `IndexedWorkSource` + lazy `unfold` driver + `OnceLock` on
`QueryShardExec` + the order-safety gate (§4.1). Chunks enqueued in assignment order
(no reordering yet). New config flag `indexed_work_stealing` (default **false**),
mirroring `indexed_dynamic_filter_pushdown` through `DatafusionQueryConfig`,
`WireConfigSnapshot`, and a `datafusion.indexed.work_stealing` cluster setting. Metric:
`work_stolen_chunks` (count of chunks a partition processed beyond its own assignment).
This alone removes the idle-tail on imbalanced scans.

**Tier 2 — reorder the queue by statistics (PR #21956, at SEGMENT/"file" granularity).**

This is a faithful port of `reorder_files_by_min_statistics`
(`df:datasource-parquet/src/sort.rs:144`) — **file-level only, NOT per-row-group**
(matching the explicit scope of this request). In our world a DataFusion "file" = a
**segment** (`SegmentFileInfo`), so we reorder the *segments* (and therefore the chunks
they own) in the shared queue; we do **not** reorder RGs within a segment.

**#21956's exact algorithm (read from the merged source):**
1. `extract_topk_sort_info` — take the **leading** sort expression; proceed only if it's
   a plain `Column`. Yields `(col_name, descending)`.
2. `file_min_value` — each file's key is its **`min(col)`** column statistic (note: `min`
   even for DESC — see below).
3. Sort: `descending ? cmp.reverse() : cmp`. **Files missing stats sort to the *end*** so
   present-stats files run first.
4. No-op when: no sort order, lead expr isn't a `Column`, or the column isn't in schema.

**Why `min` for both directions** (the subtle, load-bearing detail): file `i`'s `min` is a
lower bound on every RG inside it, so ordering files by `min` is a consistent *prefix* of
the order a per-RG reorder would produce — keeping the file layer and (any future) RG
layer convergent for the TopK dynamic filter. We copy this verbatim: **key off the
segment's `min` stat for the sort column**, direction from the request.

**Where we get `(col_name, descending)` — the one real plumbing gap.** Our `scan()`
(`table_provider.rs:186`) is **not** handed a `LexOrdering`, and `QueryShardExec` is a
leaf below the `SortExec` — so unlike DataFusion (whose optimizer threads
`sort_order_for_reorder` into the parquet source) **we have no sort-order hook.** But we
don't need a new one: the **accepted dynamic filter already references the lead sort
column** (the entire premise of the dynamic-filter port, `docs/dynamic-filters-…-impl.md`
§4b). So:
  - **`col_name`** = the column referenced by the accepted `dynamic_filters`
    (`table_provider.rs:338`) — reuse `dynamic_filter_is_acceptable`'s column walk.
  - **`descending`** = inferred from the filter's comparison operator: a DESC-TopK pushes
    `col > threshold` (keep high values) ⇒ `descending = true`; `col < threshold` ⇒ ASC.
  - **`min(col)` per segment** = read from `segment.metadata: Arc<ParquetMetaData>` (we
    already hold it, `table_provider.rs:524`); take the column chunk `min` across the
    segment's RGs. Missing → sort segment to the end.

**Consequence:** Tier 2 only reorders when a dynamic filter was accepted (i.e.
`indexed_dynamic_filter_pushdown` on + a TopK present). That's exactly the case where
reorder helps, so the coupling is a feature, not a limitation. When no dynamic filter is
present, the queue stays in assignment order (Tier 1 FIFO) — still correct, just unsorted.

**Build site:** `SharedChunkQueue::from_assignments` becomes
`from_assignments_reordered(&assignments, &segments, reorder_key)` where `reorder_key:
Option<(usize /*col_idx*/, bool /*descending*/)>`. Sort the flattened `WorkItem`s by their
segment's `min`-stat before `collect()` into the deque. `None` key ⇒ no sort (Tier 1).

**Tier 3 — sub-chunk (per-RG) stealing.** Today a `WorkItem` is a whole `SegmentChunk`
(many RGs). If a single chunk dominates runtime, make the queue hold individual RGs
(`(segment_idx, rg_index)`), so siblings steal at RG granularity. RGs are already the
atomic unit inside `IndexReader` (`stream.rs`), so this is mostly a queue-granularity
change — but it complicates per-segment evaluator construction (one evaluator per
segment today, `tp.rs:504`), so defer until Tier 1+2 prove the win. (Upstream
explicitly deferred "splitting files into smaller units" too.)

---

## 6. The DataFusion 54.0 upgrade

**Does this port require it? No.** Everything in §3 is buildable against the current
`=53.1.0` pin — we add our own queue; we don't call any new DF API. The dynamic-filter
port already shipped on 53.1.

**Should we upgrade anyway? Yes, but as a separate, sequenced change** — and mind the
blast radius:

- **What 54 buys this feature:** the `FileSource::reorder_files` reference (#21956) and
  the work-source code itself as a living example to copy from. Neither is a *runtime*
  dependency for us — they're reference implementations. So the upgrade is a
  *nice-to-have for fidelity*, not a *blocker*.
- **What the upgrade costs:** DF 54 bumps Arrow (53.x pairs with arrow 58; 54.x targets
  a newer arrow line). Our workspace pins **exact** versions across three crates
  (`sandbox/libs/dataformat-native/rust/Cargo.toml:19-33`) **and** the parquet-data-format
  module, plus the Java FFM layer that mirrors arrow's C-data ABI. An arrow major bump
  ripples into every `arrow-*` pin, `parquet`, and any FFI struct layout. **That's a
  bigger, riskier change than this feature** and should not be bundled with it.
- **Recommendation:** **Port the pattern on 53.1 first** (Tiers 1–2), verify the win,
  *then* do the DF54/arrow upgrade as its own PR. If you'd rather upgrade first, treat
  it as a standalone task — re-pin all `arrow-*`/`parquet`/`datafusion-*` crates
  together, rebuild the FFM bindings, and run the full Rust + Java suite before touching
  work-stealing. Confirm the exact 54.0 ↔ arrow version matrix at upgrade time.

---

## 7. Minimal first PR (recommended scope)

1. `SharedChunkQueue` (`Arc<Mutex<VecDeque<SegmentChunk>>>`) + `IndexedWorkSource` enum
   (new `work_source.rs` next to `partitioning.rs`).
2. `OnceLock<Option<SharedChunkQueue>>` field on `QueryShardExec`; build once in
   `execute()`; fresh empty one wherever the node is reconstructed.
3. **Order-safety gate** (§4.1): only `Shared` when output ordering is unconstrained;
   else `Local`. Default the whole feature **off** behind `indexed_work_stealing`.
4. Lazy `unfold`-based driver (§3.3a) replacing the eager chunk loop, **only** on the
   `Shared` path; keep the eager loop verbatim for `Local`.
5. Metric `work_stolen_chunks`.
6. Tests:
   - Two partitions, lopsided chunk costs (one segment with a slow `MatchAllCollector`,
     one trivial) → assert the fast partition steals (`work_stolen_chunks > 0`) and the
     full result set is identical to the `Local` path.
   - Order-sensitive plan (output ordering advertised) → assert we stay `Local`
     (`work_stolen_chunks == 0`) and results/order are unchanged.
   - `indexed_work_stealing=false` → byte-identical to today.

Tier 2 (stat reorder) and Tier 3 (per-RG stealing) are separate follow-ups. The DF54
upgrade is a separate PR entirely (§6).

---

## 8. Open questions to resolve while coding

1. **Output-ordering detection.** What's the precise predicate for "safe to steal"?
   Inspect `QueryShardExec` `PlanProperties.eq_properties.output_ordering()` — is it
   ever `Some` for us today? If never, the gate is trivially "always safe except when a
   parent is `SortPreservingMerge`." Confirm against the probe test plan shape.
2. **Node reconstruction + `OnceLock`.** Enumerate every place `QueryShardExec` is
   rebuilt (`with_new_children`, the dynamic-filter `handle_child_pushdown_result`
   ctor). Each must start with a fresh `OnceLock` so a re-planned node doesn't inherit a
   drained queue (mirror `reset_state`, `df:source.rs:536`).
3. **Evaluator lifetime under stealing.** Today one evaluator is built per chunk via
   `evaluator_factory` *inside* the per-partition loop (`tp.rs:504`). With the lazy
   driver, evaluator construction moves into the `unfold` closure — confirm the factory
   is cheap/idempotent to call from whichever partition steals the chunk, and that
   `stream_metrics` attribution still makes sense when a chunk is processed by a
   "foreign" partition (probably fine — metrics are summed across the plan, §sum_metric
   in the e2e tests).
4. **Limit accounting.** We don't have DF's `skipped_on_limit` concept because our LIMIT
   handling differs — confirm there's no analogous double-count when a `LIMIT` short-
   circuits the chained stream while the shared queue still has items.
5. **Interaction with the sequential `flatten` topology.** Today chunks within a
   partition are chained sequentially (`tp.rs:549`). The lazy driver preserves that
   (one outstanding chunk per partition) — just confirm `flatten`/`unfold` polling
   doesn't accidentally hold the mutex across an `.await`.
