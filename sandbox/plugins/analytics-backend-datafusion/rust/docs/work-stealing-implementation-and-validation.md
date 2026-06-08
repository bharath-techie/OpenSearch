# Cross-partition work-stealing: implementation, correctness & validation

> The end-to-end record of the indexed-scan work-stealing feature: what it is, how
> it's wired from the Rust scan up to the OpenSearch cluster setting, why it's
> correct, how it's tested, and the on-node ClickBench A/B results.
>
> Companion docs:
> - `dynamic-work-stealing-indexed-table.md` — the original port design (DataFusion
>   #21351 + #21956) and the tiered plan.
> - `work-stealing-partitioning-and-pruning-order.md` — why the feature exists
>   (prune-after-partition imbalance) and how partitioning maps to DataFusion.
> - `work-stealing-ab-results/` — raw benchmark data + per-run FINDINGS.

---

## 1. What it does (one paragraph)

A `QueryShardExec` over N partitions normally freezes the chunk→partition mapping at
plan time (`compute_assignments`, by row count, predicate-blind). But all predicate
pruning (page-index, bloom, row-group stats, and the runtime TopK dynamic filter)
happens **per-row-group at execution, after that mapping is frozen** — so one
partition can have ~all its work pruned away while a sibling grinds the tail.
Work-stealing replaces the static per-partition drain with **one shared queue of
row-group chunks that all sibling partition streams steal from at runtime**. Whichever
partition goes idle first pops the next chunk. Off → each partition drains only its
own assignment (byte-identical to before). It's a faithful port of DataFusion's
`SharedWorkSource` (#21351), at chunk granularity, with an optional one-shot
stat-reorder of the queue once the TopK filter goes concrete (#21956).

---

## 2. Architecture & control flow

```
QueryShardExec.execute(partition)                       table_provider.rs
  │
  ├─ shared_queue: OnceLock<Option<SharedChunkQueue>>    built once per execution
  │     get_or_init: config.indexed_work_stealing
  │       ? Some(SharedChunkQueue::from_assignments(..))  (flatten ALL partitions' chunks)
  │       : None
  │
  ├─ OFF (None):  IndexedWorkSource::Local(own chunks)   eager loop, chain_streams
  │                 → byte-identical to pre-feature behaviour
  │
  └─ ON (Some(q)): IndexedWorkSource::Shared(q.clone())  lazy unfold driver:
        loop {                                            one outstanding chunk / partition
          (tier-2) if filter concrete: q.reorder_remaining(..) once
          item = work.pop()              ← SharedChunkQueue::pop = Arc<Mutex<VecDeque>>::lock().pop_front()
          if item not in own_chunks: work_stolen_chunks += 1   ← faithful steal metric
          build_chunk_stream(item) → IndexedExec.execute(0)
        }.flatten()
```

Key files:
| Concern | Location |
|---|---|
| Shared queue + work source | `indexed_table/work_source.rs` (`SharedChunkQueue`, `IndexedWorkSource`, `WorkItem`, `ReorderKey`) |
| Build queue / lazy driver / steal metric | `indexed_table/table_provider.rs` `execute()` |
| Tier-2 stat reorder | `work_source.rs::reorder_remaining` + `table_provider.rs::dynamic_filter_reorder_key` / `segment_min_key` |
| Metric definition | `indexed_table/metrics.rs` `work_stolen_chunks` |
| Partition assignment (unchanged) | `indexed_table/partitioning.rs::compute_assignments` |

---

## 3. The unit of work is a *collector*, not a segment or row group

This is the load-bearing correctness fact and the framing that drove the design.

- A chunk is one `(segment, doc_range)` slice. The production `evaluator_factory`
  (`indexed_executor.rs`) builds **one collector per chunk** via
  `FfmSegmentCollector::create(context_id, provider_key, writer_generation,
  chunk.doc_min, chunk.doc_max)`.
- Java `LuceneFilterDelegationHandle.createCollector` calls `weight.scorer(leaf)` — a
  **fresh `Scorer`** — and stores it on a **per-`collectorKey` `ScorerHandle`** with
  its own `currentDoc` cursor (`scorersByCollectorKey`, a `ConcurrentHashMap`).
- ⇒ A segment split into two chunks → two `createCollector` calls → two distinct
  `collectorKey`s → two distinct `Scorer`s with independent cursors → **no shared
  mutable state.** Two chunks of one segment are two *different* collectors and are
  safe to advance on different threads.
- **The one hard constraint:** a *single* collector must only ever be advanced by one
  thread (the Lucene `Scorer` iterator and its `currentDoc` are non-reentrant). The
  shared queue satisfies this **by construction**: it hands out *whole chunks*, and
  each popped chunk becomes one `IndexedExec` advanced serially by one task. One chunk
  = one collector = one thread.
- **Race prevention, not detection:** `SharedChunkQueue::pop` is a single
  `Arc<Mutex<VecDeque>>::lock().pop_front()` — one atomic critical section, no
  peek-then-pop, so two racing partitions get *distinct* items or `None`; it is
  impossible for both to acquire the same chunk. The `OnceLock` guarantees all
  partitions share one queue instance, not P copies.
- **Ordering safety:** `QueryShardExec` advertises no output ordering, so DataFusion
  never assumes emission order; any `ORDER BY` is a `SortExec` above the scan that
  re-sorts regardless of chunk-completion order. Reordering/stealing chunks can never
  change a well-defined result.

> Invariants a future change must not break: (1) `pop()` stays a single locked op
> (never peek+pop); (2) the queue stays behind `OnceLock`, and every node-
> reconstruction site starts with a fresh empty cell.

---

## 4. Tests (Rust, hermetic)

`indexed_table/tests_e2e/work_stealing.rs` (from the port) + `work_stealing_collector.rs`
(added to close the gaps). Both drive the real `SingleCollectorEvaluator` delegation
path. Highlights of `work_stealing_collector.rs`:

- **Selective, content-addressed collector** (`MatchRule`: `abs_doc % m == r`) — matches
  by absolute doc id, NOT chunk bounds, like a real `Scorer`. So a mis-scoped / dropped
  / double-counted chunk changes the **row set**, checked vs an independent
  `expected_rows()` ground truth AND vs the static path.
- **`total_scanned`** atomic == total rows ⇒ no range scanned twice or skipped.
- **`ChunkDispatchGuard`** — shared `Mutex<HashSet<ChunkId>>`; panics if the same
  `(segment, doc_min, doc_max)` is dispatched twice (two partitions on one collector,
  concurrent OR sequential).
- **`SerialGuardCollector`** — per-instance in-flight atomic panics if two threads enter
  the SAME collector concurrently; `max_concurrent` proves DISTINCT collectors DO run at
  once (the safe, intended case is genuinely exercised).
- Cases: single-segment-split (np 1–4), multi-segment varied sizes (np 1,2,3,5),
  selectivity sweep (match-all → ~empty), 20× stress, + 2 meta-tests proving each guard
  is not a no-op, + `work_stolen_chunks_counts_only_true_cross_partition_steals`.

**Fault injection (proved the tests fail on a real break, then reverted):**
- chunk emitted twice in `from_assignments` → dispatch guard fired in 4 e2e tests.
- last chunk dropped → row-set + `total_scanned` mismatched (dispatch guard correctly
  silent) — proving the guards detect independent bug classes.

Status: full Rust lib suite **1073 pass** with work-stealing ON by default; Java
`WireConfigSnapshotTests` + `DatafusionSettingsTests` pass.

---

## 5. Cluster-setting wiring (Java ↔ Rust wire)

`datafusion.indexed.work_stealing` — boolean, NodeScope + **Dynamic**, default **ON**.

| Layer | File | Default |
|---|---|---|
| Cluster setting | `DatafusionSettings.java` (`INDEXED_WORK_STEALING`, in `ALL_SETTINGS`, both ctors, listener) | true |
| Java builder | `WireConfigSnapshot.java` (field/getter/builder-copy; `writeTo` offset **80**) | true |
| Rust fallback (backs test builder) | `datafusion_query_config.rs::fallback()` | true |

All three must stay in lockstep. **Wire layout:** Rust `WireDatafusionQueryConfig` has
`indexed_work_stealing` (i32) at **offset 80**; struct size **88** (84 bytes fields + 4
`repr(C)` tail padding). Verified by a layout probe. `WireConfigSnapshot.BYTE_SIZE` was
bumped 80→88 to match (the port had left it at 80 — a latent wire mismatch where Rust
read offset 80 past the Java-allocated segment; wiring the setting fixed it).

Being `Dynamic` means the A/B is a **runtime toggle**, no rebuild/restart between arms.

> Scheduling note: ON is all-or-nothing — there is no "own work first." Every partition
> shares the one queue and pops front, one chunk at a time. OFF = each partition drains
> its own static set. Strictly either/or (matches DF's `Local | Shared` WorkSource).

---

## 6. On-node ClickBench A/B

**Setup:** archive node, ~99,997,497 clickbench docs. Multi-partition via
`search.concurrent_segment_search.mode=auto` + `search.concurrent.max_slice_count=N`
→ `target_partitions = min(N, cores)` (box has 10 cores). Same deployed build; only the
dynamic `datafusion.indexed.work_stealing` toggled between OFF and ON arms. Per query:
1 cold (cache cleared) + 2 warm; perf = best-of-2-warm. Runner:
`run_work_stealing_ab.sh [N]` → results in `work-stealing-ab-results/p<N>/`.

### 6.1 Correctness (both 4 and 8 partitions): NO regression

- Every query with a **deterministic total order** matched **byte-exact** OFF vs ON, at
  both partition counts.
- The only differences are `head`/LIMIT queries over a **non-total order**. At 4p: q18,
  q25, q32, q33, q40 (5). At 8p: q18, q22, q25, q32, q33, q39, q40, q41, q42 (9).
  Verified rigorously each time:
  - The **sort-key column sequence is byte-identical OFF vs ON** (e.g. 8p q41 col0 =
    `[27,27,26,26,25,25,25,24,24,24]` in both arms) — the sort is correct and stable;
    only *which tied rows* fill the equal-key slots at the LIMIT cut differs.
  - q18 has **no sort** (any 10 groups valid); q22 returns the **same 10 rows reordered**;
    q25/q32/q33/q39/q40/q41/q42 differ only within a single sort-key tie-band (all `c=1`,
    `=2`, `=15`, `=27`, …). PPL defines no order among ties under LIMIT.
  - ON isn't even stable run-to-run (verified on q18). Inherent to LIMIT-without-total-
    sort, NOT introduced by work-stealing — the static path is deterministic only by
    accident of fixed partition order. A real `ORDER BY` re-sorts above the scan, unaffected.
  - **More mismatches at 8p (9) than 4p (5)** is expected: more partitions → more
    emission-order variance → more tie-bands reshuffled. Not a regression.
- q14 (`dc(UserID) ... sort -u | head`) errors in BOTH arms identically (~120–150s
  timeout, pre-existing distinct-count issue, unrelated).

### 6.2 Performance

| Partitions | OFF total warm | ON total warm | ON/OFF | real correctness regressions |
|---|---|---|---|---|
| 4 | 108.71s | 106.02s | **0.975x** (ON ~2.5% faster) | 0 |
| 8 | 85.84s | 82.00s | **0.955x** (ON ~4.5% faster) | 0 |

- Per-query deltas are mostly within run-to-run noise, but the **aggregate win grows
  with partition count** (2.5% → 4.5%) — consistent with the mechanism: more sibling
  partitions = more chance one idles while others still have work, so more to rebalance.
- 8p standout warm wins: q35 0.47x (-3.97s), q34 0.66x (-2.30s), q15 0.38x (-1.43s),
  q29 -1.68s, q33 0.82x, q36 0.64x. 8p regressions are small/medium queries dominated by
  per-query overhead + noise (q19 +1.64s, q6 +1.10s, q24 +3.79s on a ~17s query).
- 8 partitions is faster than 4 in absolute terms (more parallelism); work-stealing's
  relative benefit increases on top of that.
- Per-run detail: `work-stealing-ab-results/p4/FINDINGS.md`, `p8/FINDINGS.md`.

### 6.3 Interpretation

ClickBench on this archive is **not strongly lopsided per partition**, so work-stealing's
rebalancing has limited headroom — the small-but-growing win (2.5% @ 4p → 4.5% @ 8p) is
the *expected* and acceptable result for a default-on feature: it must not regress
balanced scans (it doesn't), and it pays off more as concurrency rises. The largest wins
will come under **uneven per-partition cost** — heavy dynamic filtering / skewed segment
sizes — a separate targeted benchmark (TODO).

---

## 7. Status & TODO

Done: port cherry-picked + DF54 build fix; collector-correctness tests + fault
injection; cluster-setting wiring (3 layers + wire); faithful steal metric; default ON;
full Rust + Java test suites green; on-node 43-query A/B at 4 and 8 partitions (this doc).

TODO:
- Lopsided-workload benchmark to demonstrate the real perf upside.
- Tier-2 (TopK stat-reorder) e2e under a live dynamic filter.
- Tier-3 (per-RG stealing) — deferred, as upstream deferred sub-file units.
- Investigate the q14 `dc()` ~123s timeout and the q-with-join `byte array offset
  overflow` panic (`arrow-array generic_bytes_builder.rs:87`) — both pre-existing,
  both arms, unrelated to work-stealing, but they muddy wall-clock totals.

---

## 8. Reproducing the A/B

```bash
# node must be running a build whose jar recognizes datafusion.indexed.work_stealing
# (the runner preflight aborts with guidance if not).
cd /Users/gbh/Documents/dev/OpenSearch
bash sandbox/plugins/analytics-backend-datafusion/rust/docs/run_work_stealing_ab.sh 4   # → p4/
bash sandbox/plugins/analytics-backend-datafusion/rust/docs/run_work_stealing_ab.sh 8   # → p8/
```
The runner forces multi-partition concurrency, runs both arms by toggling the dynamic
setting, writes `bench_off.json` / `bench_on.json` / `COMPARISON.md`, and restores
concurrency settings at the end.

### Environment gotchas encountered (archive setup, not the code)
- The 3.8.0-ARCHIVE home crashes on startup (Guice `StreamTransportService not
  @Nullable`, with cascade "transport handlers for action cluster:admin/opensearch/ppl
  already registered" — the @Nullable is primary, ppl-dup is downstream noise). The
  working config is the 3.7.0-ARCHIVE home; the 23G clickbench data lives in
  3.8.0-ARCHIVE/data. Run from the 3.7.0 home with `-E path.data=.../3.8.0-ARCHIVE/data`.
- 3.8.0-ARCHIVE jvm.options lacked `-Djava.library.path`; add it pointing at the release
  dylib dir (`.../dataformat-native/rust/target/release`).
- A stale `archived.datafusion.indexed.route_pure_parquet_through_indexed` persistent
  setting blocks all settings updates; clear with `"archived.*": null` in the PUT.
