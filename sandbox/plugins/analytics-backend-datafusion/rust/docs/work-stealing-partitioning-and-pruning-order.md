# Work-stealing: partitioning model & the prune-after-partition ordering

> Companion to `dynamic-work-stealing-indexed-table.md` (the port mechanics) and
> `dynamic-filters-indexed-table-impl.md` (the dynamic filter). This doc answers
> the question that *justifies* the whole feature: **when does pruning happen
> relative to partitioning, and why does that create the imbalance work-stealing
> fixes?** It also pins down how our partitioning actually works, how it compares
> to DataFusion's, and which correctness cases the current tests cover.
>
> All file:line refs are to our tree unless prefixed `df:` (= the local DataFusion
> checkout at `~/Documents/dev/datafusion`, currently `branch-54`).

---

## 0. TL;DR

1. **No data-column stats pruning happens before partitioning** — not in our
   scan, and not in DataFusion either. Files/segments are split into partitions
   **blind to predicate selectivity**, by row-count (us) or byte-size (DF).
2. **All predicate pruning (page-index, bloom, row-group stats, dynamic TopK
   filter) happens per-row-group at *execution* time**, *after* the partition
   assignment is already frozen.
3. Therefore one partition can have ~all of its RGs pruned to near-zero work
   while a sibling has none pruned. The planner could not have known — the
   information that prunes a RG only exists at execution. **Work-stealing is the
   rebalancer for pruning decisions the planner couldn't make.**
4. The one real exception in DataFusion is **Hive partition-column** pruning
   (directory layout, e.g. `/date=…/`), which *can* drop whole files at planning.
   That is a path-metadata feature, **not** data-column min/max stats, and we have
   no equivalent (single shard, no Hive layout).

---

## 1. How partitioning works in our scan

### 1.1 The unit and the split

`compute_assignments` (`partitioning.rs:50`) is the whole partitioning logic. It:

1. Flattens **every** RG of **every** segment into one list, in segment order
   (`partitioning.rs:61`). Each `RGEntry` carries only `(segment_idx, rg_index,
   first_row, num_rows)`.
2. Computes `rows_per_partition = ceil(total_rows / target_partitions)`
   (`partitioning.rs:79`).
3. Walks the flat RG list and cuts a new partition each time the accumulated row
   count crosses `rows_per_partition` (`partitioning.rs:115`), **always at an RG
   boundary**.

A partition is a `PartitionAssignment { chunks: Vec<SegmentChunk> }`. A
`SegmentChunk` is `(segment_idx, doc_min, doc_max, row_group_indices)` — a
contiguous RG subset *within one segment*. A partition spanning a segment boundary
gets one chunk per segment it touches; a segment large enough to exceed
`rows_per_partition` is split into multiple chunks across multiple partitions.

```
segments:  [ seg0: 8 RGs ........ ][ seg1: 3 RGs ][ seg2: 12 RGs ............ ]
flatten →  rg rg rg rg rg rg rg rg | rg rg rg     | rg rg rg rg rg rg rg rg rg rg rg rg
cut by row count into target_partitions, at RG boundaries:
  P0: [seg0 rg0..4]                      (chunk A)
  P1: [seg0 rg4..8][seg1 rg0..1]         (chunk B, chunk C)   ← P1 spans 2 segments
  P2: [seg1 rg1..3][seg2 rg0..3]         (chunk D, chunk E)
  P3: [seg2 rg3..12]                     (chunk F)
```

### 1.2 The ONLY input is row count

`compute_assignments` sees `num_rows` and nothing else. It does **not** see:
- the query predicate,
- any column min/max statistics,
- bloom filters or page indexes,
- the dynamic TopK filter (which doesn't exist yet at plan time — it's a `true`
  placeholder until rows flow).

Grep confirms it: `partitioning.rs` references only `num_rows`/`first_row`; the
words `prune`, `filter`, `stats`, `dynamic` do not appear. So the split is
**predicate-blind, balanced on raw rows.**

### 1.3 When it runs: plan time, frozen

`scan()` calls `compute_assignments` once (`table_provider.rs:277`) and stores the
result on `QueryShardExec.assignments`. From that point the file→partition mapping
is **frozen** — `execute(partition)` (`table_provider.rs:461`) just looks up
`assignments[partition]`. This is the static mapping the work-stealing port
replaces with a shared queue (see the port doc §2–§3).

### 1.4 Where the segment list itself comes from

`build_segments` (`segment_info.rs:49`) turns the shard's parquet files into
`SegmentFileInfo`s, pushing **every** RG of every file (`segment_info.rs:87-95`).
There is **no stats-based file or RG dropping** here — it reads metadata, records
RG row counts, unions the schema, done. The file *list* is supplied by the Java
caller through `df_create_reader` (`ffm.rs:232`); that's a shard-membership /
catalog decision, not a per-query predicate prune.

> **Net:** from the file set entering the scan, through `build_segments`, through
> `compute_assignments`, **nothing is pruned by the query predicate.** The full
> RG universe is partitioned by row count.

---

## 2. Where pruning actually happens: execution, per-RG

Every predicate-based prune fires inside the per-RG streaming loop, *after*
partitioning, *during* execution:

| Prune | Site | Granularity | When |
|---|---|---|---|
| Page-index stats | `single_collector.rs::prefetch_rg` → `page_pruner.prune_rg` (`single_collector.rs:262`) | page ranges within an RG | execution, per RG |
| Bloom filter | `single_collector.rs:294` (`bloom_prune_rg`) | whole RG | execution, per RG |
| Dynamic TopK filter | `stream.rs::fetch_row_group` → `ctx.rg_provably_excluded` (`stream.rs:188`) | whole RG, before the Lucene eval | execution, per RG, threshold tightening as the heap fills |

A pruned RG returns `Ok(None)`/`PrefetchOutcome::Pruned` and contributes no work.
Crucially the **dynamic filter** only becomes concrete *after* rows start flowing
and the TopK heap fills — so its pruning power literally does not exist at plan
time. (This is also why Tier-2 queue reorder is a one-shot done lazily mid-scan
once the filter goes concrete — port doc §5.)

### 2.1 Why this produces imbalance

Partition assignment was balanced on **raw row count**. Pruning then removes a
*different* fraction of rows from each partition, decided by data the planner never
had:

```
plan time (balanced by rows):     P0=1.0M  P1=1.0M  P2=1.0M  P3=1.0M
execution, after per-RG pruning:  P0=0.05M P1=1.0M  P2=0.1M  P3=0.95M
                                      ↑ 95% pruned          ↑ ~0% pruned
```

Under the **static** mapping, P0 and P2 finish fast and **idle** while P1/P3 grind
the tail. Under **work-stealing**, P0/P2 drain the shared queue, so wall-clock is
bounded by total surviving work / N, not by the unluckiest partition. The win
scales with how *uneven* pruning is across partitions — and dynamic filtering is
precisely the thing that makes it wildly uneven.

---

## 3. How DataFusion does it (verified against `branch-54`)

The question "does DF prune files by stats *during* partitioning, since it has the
stats?" — answered from source:

1. **Planning, file groups built** by `ListingTable`. The optimizer may *reorder*
   and *split* files by stats (`split_groups_by_statistics`,
   `df:file_groups.rs`), but does **not drop** them for data-column predicates.
2. **Planning, repartition rule → `FileScanConfig::repartitioned`**
   (`df:file_scan_config/mod.rs:678`): the partitioning step. Calls
   `FileGroupPartitioner` to byte-range-split files into `target_partitions` groups
   using **file size**, not predicate stats. No predicate-based file dropping.
3. **Execution, `ParquetOpener::open`** (`df:datasource-parquet/src/opener/mod.rs`):
   per file, at scan time, builds `RowGroupAccessPlanFilter` and calls
   `prune_by_statistics` (`:910`), `prune_by_bloom_filters` (`:1057`), then page
   pruning (`:1114`). **This is the only place data-column predicate pruning
   happens** — per-RG, inside the already-partitioned, already-executing stream.

So DF's structure is the same as ours: **partition blind to predicate
selectivity; prune per-RG at execution.** Its work-stealing port (#21351) exists
for the same reason ours does — and #21956 reorders the shared queue by stats for
exactly the dynamic-filter-imbalance case.

### 3.1 The one exception: Hive partition-column pruning

DataFusion *can* eliminate whole files at planning, but **only** for **Hive
partition columns** (directory layout like `/date=2024-01-01/`). Each file's
partition-column min=max=the-partition-value is known from the *path* without
reading the file, so `file_groups.rs:378` lets the optimizer "prune entire file
groups based on partition bounds." This is **path-metadata** pruning, not
data-column min/max stats, and it happens before the repartition step.

**We have no equivalent:** a single OpenSearch shard's segments aren't laid out in
Hive partition directories, and our segment list is a catalog membership decision
on the Java side. So for us there is simply *no* planning-time file drop of any
kind — the full RG universe always reaches `compute_assignments`.

---

## 4. Mapping table (us ↔ DataFusion)

| Concept | DataFusion | Our indexed scan |
|---|---|---|
| Stealable unit | `PartitionedFile` (whole file *or* a byte-range/RG slice) | `SegmentChunk` (whole segment *or* an RG subset) |
| Partition split basis | file **byte size** (`FileGroupPartitioner`) | **row count** (`compute_assignments`) |
| Split happens at | planning (repartition rule) | planning (`scan()`) |
| Sub-file splitting | planner pre-slices into more `PartitionedFile`s | `compute_assignments` pre-slices a segment into chunks |
| Predicate pruning | execution, per-RG (`ParquetOpener`) | execution, per-RG (`prefetch_rg` / `fetch_row_group`) |
| Planning-time file drop | only Hive partition columns | none |
| Shared queue | `SharedWorkSource: Arc<Mutex<VecDeque<PartitionedFile>>>` | `SharedChunkQueue: Arc<Mutex<VecDeque<WorkItem>>>` |
| Stat reorder | `reorder_files` (#21956), file granularity | `reorder_remaining`, segment granularity (Tier 2) |
| Per-RG stealing | deferred upstream | Tier 3, deferred |

The granularities match: **steal at the (possibly-sub-file) work-item level; do
finer-than-file splitting in the planner before the queue exists.** Neither system
steals individual RGs out of a work item today.

---

## 5. The correctness model (why stealing is safe)

### 5.1 The unit of work is a *collector*, not a segment or RG

For the delegated (Lucene) path, the production `evaluator_factory`
(`indexed_executor.rs` ~`:693`) builds **one collector per chunk** via
`FfmSegmentCollector::create(context_id, provider_key, writer_generation,
chunk.doc_min, chunk.doc_max)`. On the Java side `createCollector`
(`LuceneFilterDelegationHandle.java:188`) calls `weight.scorer(leaf)` — a **fresh
`Scorer`** — and stores it on a **per-`collectorKey` `ScorerHandle`** with its own
`currentDoc` cursor (`scorersByCollectorKey`, a `ConcurrentHashMap`).

Consequence: a segment split into two chunks → two `createCollector` calls → two
distinct `collectorKey`s → two distinct `Scorer`s with independent cursors → **no
shared mutable state.** Two chunks of the same segment are two *different*
collectors and are safe to advance on different threads.

### 5.2 The one hard constraint

A **single** collector must only ever be advanced by **one** thread — the Lucene
`Scorer` iterator (`collectDocs`, `LuceneFilterDelegationHandle.java:214`) is not
reentrant; its `currentDoc` is mutated without synchronization. (This is the same
class of hazard that bit the multi-RG decoder when it armed RG n+1's prefetch
before awaiting RG n — a *within-stream* bug, orthogonal to stealing.)

### 5.3 How the port satisfies it — by prevention, not detection

The shared queue hands out **whole chunks** (`work.pop()`, `table_provider.rs:566`)
and each popped chunk is built into one `IndexedExec` advanced serially by one
task. So:

- One chunk = one collector = one task. The one-thread-per-collector constraint
  holds **by construction**.
- The race "two partitions acquire the same chunk" is **prevented at the source**:
  `SharedChunkQueue::pop` is a single `Arc<Mutex<…>>::lock().pop_front()`
  (`work_source.rs:119`) — one atomic critical section, no peek-then-pop, so two
  racers get *distinct* items or `None`. It is impossible for both to get the same
  chunk. The `OnceLock` on `QueryShardExec` (`table_provider.rs:485`) guarantees
  all partitions share **one** queue instance, not P copies.

> Load-bearing invariants a future change must not break (flagged for review):
> (1) `pop()` stays a single locked op — never split into peek+pop;
> (2) the queue stays behind the `OnceLock`, and every node-reconstruction site
> (`with_new_children`, the dynamic-filter ctor) starts with a **fresh empty**
> cell so a re-planned node never inherits a drained/duplicated queue
> (mirror `df:source.rs::reset_state`).

### 5.4 Ordering safety

`QueryShardExec` advertises `EquivalenceProperties::new(..)` with **no output
ordering** (`table_provider.rs:281`), so DataFusion never assumes emission order;
any `ORDER BY` is a `SortExec` above us that re-sorts regardless of chunk
completion order. Reordering/stealing chunks therefore can never produce a wrong
answer — it only changes *when* work happens. (Analogue of DF's `preserve_order`
opt-out, always satisfied for us.)

---

## 6. Test coverage matrix

Two e2e suites, in `indexed_table/tests_e2e/`:

### 6.1 `work_stealing.rs` (cherry-picked with the port)
- 1-RG-per-segment fixtures; thread-safe match-all collector.
- Asserts: result == static path; `work_stolen_chunks > 0` when chunks >
  partitions; `== 0` when flag off; result is partition-count-invariant; idle
  partition steals from a slow sibling.
- **Gap it leaves:** a segment is never split (1 RG each), and a match-all
  collector can't detect a mis-scoped/dropped/duplicated chunk or a collector race.

### 6.2 `work_stealing_collector.rs` (added to close that gap)
Drives the **real `SingleCollectorEvaluator`** (delegation path), with:

- **Selective, content-addressed collector** (`MatchRule`: `abs_doc % m == r`) —
  matches by **absolute doc id**, not chunk bounds, like a real `Scorer`. So a
  mis-scoped / dropped / double-counted chunk changes the **row set**. Verified vs
  an *independent* `expected_rows()` ground truth **and** vs the static path.
- **`total_scanned`** atomic (summed across collectors) must == total rows ⇒ no
  range scanned twice or skipped.
- **`ChunkDispatchGuard`** — shared `Mutex<HashSet<ChunkId>>`; the factory
  `register`s each `(segment, doc_min, doc_max)` once. A second dispatch of the
  same chunk (two partitions on one collector — concurrent *or* sequential) →
  immediate panic.
- **`SerialGuardCollector`** — per-instance in-flight atomic panics if two threads
  enter the **same** collector concurrently; `max_concurrent` proves **distinct**
  collectors *do* run at once (the safe, intended case — so the cross-thread path
  is genuinely exercised, not just asserted).

Cases:
| Test | Fixture | Partition counts | Asserts |
|---|---|---|---|
| `single_segment_split_selective_delegation_no_collector_race` | 1 seg, 40 rows / 8 RGs, slow | 1,2,3,4 | segment splits into N chunks; rows == ground truth; `total_scanned==40`; ≥2 concurrent collectors of the *same* segment |
| `multi_segment_varied_sizes_selective_delegation_no_collector_race` | 4 segs of different sizes, slow middle | 1,2,3,5 | rows == ground truth; each doc scanned once; path engaged (`stolen>0`) |
| `delegation_correct_across_selectivities` | both fixtures | 1,2,3,4 | correct for rules from match-all → ~empty |
| `repeated_runs_never_trip_the_collector_guard` | 1 seg, lopsided | 4 | 20× stress; rows + `total_scanned` stable |
| `serial_guard_actually_trips_on_concurrent_same_collector` | unit | — | meta-test: the in-flight guard is **not** a no-op |
| `chunk_dispatch_guard_rejects_duplicate_dispatch` | unit, `#[should_panic]` | — | meta-test: the dispatch guard is **not** a no-op |

### 6.3 Fault injection (the tests were proven to fail)
The guards were verified live by breaking the production queue and confirming red,
then reverting:
- **Each chunk emitted twice** in `SharedChunkQueue::from_assignments` → 4 e2e
  tests FAILED with *"chunk … dispatched to a collector twice"*. ✅ dispatch guard
  is wired into the real path.
- **Last chunk dropped** from the queue → 4 e2e tests FAILED with *"work-stealing
  produced wrong rows"*, even at np=1; the dispatch guard correctly stayed quiet.
  ✅ the two guards detect independent bug classes.

All 9 work-stealing tests pass on revert; full `indexed_table` suite 433 pass.

### 6.4 Coverage gaps still open
- **Multi-collector boolean tree under stealing** (`match() AND match()`, the
  `TreeBitsetSource` path) is exercised by `boolean_algebra.rs` but not *under
  work-stealing* — worth a case if Tier-1 ships with delegation trees.
- **Dynamic-filter (TopK) + stealing interaction** end-to-end: Tier-2 reorder
  has unit coverage (`work_source.rs` tests) but no e2e that asserts the reorder
  fires mid-scan under a real TopK and improves pruning. (Lands with the dynamic
  filter wiring.)
- **On-node wall-clock A/B** with real delegation imbalance (the `#[ignore]`d
  benchmark is synthetic-sleep only).

---

## 7. Build / run

```bash
# Rust dylib (debug, fast iteration):
cd /Users/gbh/Documents/dev/OpenSearch/sandbox/libs/dataformat-native/rust
cargo build -p opensearch-datafusion

# The work-stealing tests:
cargo test -p opensearch-datafusion --lib work_stealing
```
Deploy/A-B on the archive node: see `route-pure-parquet-perf-handoff.md`.

> Note: the cherry-pick was authored against DF 53.1.0; `main` is DF 54.0.0. The
> only drift was `dynamic_filter_reorder_key`'s tree-node closure — DF 54 yields
> `&Arc<dyn PhysicalExpr>` and exposes `downcast_ref` directly (no `.as_any()`).
> Fixed in `table_provider.rs`.

---

## 8. Still TODO (feature-level)
- Java `WireConfigSnapshot` + `DatafusionSettings` wiring for the
  `datafusion.indexed.work_stealing` cluster setting (Rust config field +
  `from_assignments` build gate already exist; the wire offset / setting is not
  yet plumbed).
- Tier-2 e2e + on-node benchmark (§6.4).
- Tier-3 (per-RG stealing) — deferred, as upstream deferred sub-file units.
