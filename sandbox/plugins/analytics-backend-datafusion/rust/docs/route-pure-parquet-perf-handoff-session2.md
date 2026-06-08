# Route-pure-parquet-through-indexed: perf handoff (session 2)

> Continues `route-pure-parquet-perf-handoff.md`. Goal unchanged: make pure-parquet
> queries run through the indexed (`QueryShardExec`, codename "mustang") pipeline at
> parity with the vanilla `ListingTable`/`DataSourceExec` path, so the two scan
> implementations can collapse into one.
>
> This session: built a full ClickBench A/B harness, ran the whole suite vanilla-vs-indexed
> on **release**, and root-caused the remaining gaps with EXPLAIN-ANALYZE plans.

> ⚠️ READ THIS FIRST — METHODOLOGY GOTCHAS THAT PRODUCED WRONG ANSWERS THIS SESSION.
> Re-verify before trusting any perf claim.
> 1. **transient > persistent settings.** A leftover *transient* `route_pure_parquet_through_indexed=false`
>    silently overrode the harness's *persistent* toggling → an ENTIRE 43-query run executed
>    vanilla-vs-vanilla and looked "byte-exact, perf-equal". Always confirm the *effective*
>    plan is `QueryShardExec` (indexed) vs `DataSourceExec` (vanilla) by reading the
>    QUERY_PROFILE dump, not by trusting the flag.
> 2. **Warmup-order confound.** A min_skip_run sweep that ran configs cold→warm in order
>    showed a perfect monotonic "improvement" that was pure warmup. Re-running in reverse
>    gave the mirror image. Interleave configs and take medians; never trust a monotonic
>    trend that tracks execution order.
> 3. **Summed-inner metrics.** `QueryShardExec` sums the per-RG inner parquet `MetricsSet`s
>    (~110 of them). So `time_elapsed_*`, `metadata_load_time`, `row_pushdown_eval_time`,
>    `predicate_cache_*` etc. are SUMS, not comparable to vanilla's single-DataSourceExec
>    value. Only the indexed-OWN single accumulators (defined in `indexed_table/metrics.rs`)
>    are clean: `elapsed_compute`, `parquet_poll_time`, `on_batch_mask_time`,
>    `filter_record_batch_time`, `index_query_time`, `coalesce_time`, `build_mask_time`,
>    `prefetch_wait_time`.
> 4. **Per-query e2e wall is warmup-dominated** at this node (same config ranged 0.13–0.33s
>    by order). Lean on the single-accumulator profile metrics; use interleaved medians if
>    you must report wall.

---

## 0. Environment / current state (verified at end of session)

- **Node:** running, on the **release** dylib (`-Djava.library.path=.../target/release`,
  built Jun 8 11:49). `jvm.options` lib path was changed debug→release this session.
- **Index:** `clickbench`, yellow, 99,997,497 docs.
- **Settings currently set** (RESET THESE if you want defaults):
  - persistent: `route_pure_parquet_through_indexed=true`, `search.concurrent_segment_search.mode=none`,
    `memory_pool_limit_bytes=34359738368`, `search.concurrent.max_slice_count=2`
  - transient: `indexed_pushdown_filters=false`, `parquet_pushdown_filters=false`,
    `min_skip_run_default=1024`, `min_target_partitions=1`
  - ⚠️ transient does NOT survive a full node restart; persistent does.
- **Uncommitted changes** (all from session 1, still uncommitted, compile clean in release):
  routing gate (`ffm.rs`), config field (`datafusion_query_config.rs`), perf fixes A/B/C
  (`predicate_evaluator.rs`, `eval/mod.rs`, `stream.rs`, `single_collector.rs`),
  TEMP profile instrumentation (`api.rs::stream_close` — STILL must be removed before landing),
  Java wire/settings (`WireConfigSnapshot.java`, `DatafusionSettings.java` + tests).
- **New this session (untracked):** `rust/docs/` including the harness — see §1.
- REVERIFY all of these might be stale

---

## 1. The ClickBench A/B harness (USE THIS)

`rust/docs/cb_harness.py` + `rust/docs/cb_queries.json` (43 ClickBench PPL queries, name→SQL).

- Two passes, flipping `route_pure_parquet_through_indexed` OFF (vanilla) ↔ ON (indexed) per query:
  - `perf(iters=3)`: per query/flag → clear caches → cold run → median of 3 hot. Prints
    `OFF cold | OFF hot | ON cold | ON hot | hot Δ%`.
  - `correctness()`: scalar aggregates compared directly; raw-row queries rewritten with a
    total-order tiebreaker; group-by+head-N checked as (a) deterministic top-N and
    (b) truncation-immune total group-count.
- **Hardened this session:** `set_flag()` now nulls the transient override AND reads back the
  effective value, asserting it matches (so gotcha #1 fails loud instead of silently). It does
  NOT auto-reset the flag when done.
- Single-partition A/B requires BOTH `concurrent_segment_search.mode=none` AND
  `min_target_partitions=1` (mode=none alone is overridden by the partition floor).
- Run perf-only fast: `cd rust/docs && python3 -c "import cb_harness as h; h.perf(iters=3)"`.
- Outputs `/tmp/cb_perf.json`, `/tmp/cb_correctness.json`.

---

## 2. Headline results (RELEASE, single-partition)

### Correctness: clean
All 43 byte-exact vanilla-vs-indexed where they ran (verified on debug; q28 failure reproduces
on release). Non-clean cases:
- **q28 — indexed-only 500** ("Failed to start streaming fragment on [clickbench][0]"); vanilla
  succeeds. REAL indexed bug, reproduces on release. Not yet root-caused.
- q14 — 500 on BOTH paths (pre-existing `byte array offset overflow`), not indexed-specific.
- q29 — malformed in source workload (missing `where`), skipped.
- q18 — harness couldn't parse group keys → unverified (not failed).

### Perf (indexed vs vanilla, pushdown OFF on both — the recommended default)
- **Most aggregation queries are AT PARITY** (±6%): q01,q03,q05,q06,q08,q09,q10,q11,q12,q13,
  q15–q19,q21–q23,q26,q30–q36. The debug-mode regressions (e.g. q08 +20%) were debug-build
  inflation of Rust compute; they vanish on release (q08 release ≈ −5%/parity).
- **Structural regressions (real on release):**
  - **q07 `min/max(EventDate)`: ~+1750% (13ms→235ms).** Vanilla answers min/max from parquet
    column statistics without scanning; indexed scans all rows. Missing stats short-circuit.
    HIGHEST-VALUE FIX.
  - **EventTime-sorted raw-row+limit: q25 +364%, q27 +137%, q24 +108%.** `... | sort EventTime | head N`.
    q26 (`sort SearchPhrase`) is at parity → specific to the EventTime sort path. Raw-row
    (no aggregation pipeline-breaker), so per-batch streaming cost shows.
  - Small selective queries (q37–q42, q20): +10–50% but tiny absolute (30–240ms) — fixed
    per-query indexed overhead dominates when the query is small.

---

## 3. Pushdown (RowFilter) — when it helps, and why OFF is the right default

`indexed_pushdown_filters` drives the indexed path; `parquet_pushdown_filters` drives vanilla
(names are backwards — see session-1 handoff §2). Default both false.

- **Pushdown ON helps ONLY at extreme selectivity AND with expensive non-predicate output cols.**
  Winners: q22 (sel 0.002%), q23 (0.0076%), q24 (0.037%) — all `like(URL/Title,'%...%')` with
  heavy `SearchPhrase` output. Everything with selectivity ≥1.1% loses, sometimes badly
  (q02 +162%, q25 +591%, q42 +234%).
- The clean discriminator is **predicate row-selectivity**, NOT the candidate RowSelection
  run-length (q02 and q22 have identical coarse selections, opposite outcomes). `scan_efficiency_ratio`
  (= bytes_scanned/file_size, an IO/prize-size proxy) separated winners (>1300%) from losers
  (<680%) in this suite but is a proxy, not causal, and is post-hoc (not available at plan time).
- **Real gate (proposed):** push only when `min_skip_run == 1` (row-granular = selective candidate),
  AND/OR estimate `(predicate selectivity < ~0.1%) AND (output-col compressed bytes large)`.
  See §5 lever.

### min_skip_run is NOT useless (it's correct)
`min_skip_run=1024` (block-granular, chosen when candidate selectivity ≥ 3%) coalesces small
gaps so parquet bulk-decodes surviving pages instead of a fragmented row-by-row gather. It's
what gives non-selective aggregations vanilla-parity. Increasing it does NOT reduce poll time
(tested both sweep directions — flat; the apparent gain was warmup, gotcha #2). Pushdown is
designed to ride on `min_skip_run==1`; the bug is that for `PredicateOnlyEvaluator` the
`needs_row_mask()==false` change removed the alignment guard, so pushdown currently fires even
in block-granular mode (q02), which is the wrong combination.

---

## 4. Mechanism deep-dives (all verified against plans + arrow-rs/datafusion source in ~/Documents/dev)

### IO model (CORRECTED — my first claim was wrong)
- arrow-rs `ParquetRecordBatchStream` (the `Stream` impl) is a SINGLE-outstanding-request
  fetch-then-decode state machine. It does NOT background-prefetch the next RG's column data.
  DataFusion's opener uses this plain Stream and never calls `next_row_group` (the opt-in
  prefetch API). So **vanilla does NOT get free cross-RG IO read-ahead.** Both paths fetch-then-decode.
- Indexed prefetch (`stream.rs::start_prefetch` → `spawn_blocking`) overlaps only the next RG's
  CANDIDATE bitmap computation, not its parquet IO. RGs are read strictly sequentially.

### Predicate cache (the pushdown-ON tax)
- `max_predicate_cache_size`: DataFusion default = `None` → falls back to arrow-rs reader default
  **100 MB**. OpenSearch plugin never sets it → both paths use 100 MB. `0` = no caching.
- The cache holds decoded PREDICATE-column values between filter-eval and output-generation so a
  predicate column that's ALSO an output column isn't decoded twice. `cache_projection` =
  predicate ∩ output controls what's cached.
- **q02 (`count()`, predicate col not projected):** vanilla caches 0 (correct); indexed caches
  59.6M records (waste) → indexed decode 58ms vs vanilla 53ms... wait, the big one: with pushdown
  on, q02 indexed `parquet_poll`/processing ≈ 88–94ms vs vanilla ≈ 53–58ms. The cache materialization
  (~24ms) + per-RG setup (~11ms) is the gap. The RowFilter eval itself is identical (~11ms both).
- **q08 (`by AdvEngineID`, predicate col IS projected):** BOTH cache 59.6M (legit — avoids
  re-decode), so they're near-parity with pushdown on (proc 86 vs 89ms). The cache is only WASTE
  when the predicate column isn't in the output.
- Fix direction: set `max_predicate_cache_size=0` (or empty `cache_projection`) on the indexed
  per-RG `ParquetSource` ONLY when the predicate column isn't projected. NOT a blanket disable
  (would hurt q08-type queries by forcing a re-decode).

### Metadata/file caching (VERIFIED correctly wired — nothing to add)
- Footer `ParquetMetaData` is cached in the GLOBAL `FileMetadataCache` (shared cross-query):
  `indexed_executor.rs` builds a per-query RuntimeEnv but carries over
  `runtime.runtime_env.cache_manager.get_file_metadata_cache()` + limit (default 50MB).
  `build_segments` → `load_parquet_metadata` consults it via `DFParquetMetadata::fetch_metadata()`.
  Per-RG streams get the `Arc<ParquetMetaData>` directly (`CachedMetadataReader::get_metadata`),
  no re-fetch.
- DataFusion's `CachedParquetFileReaderFactory` caches the SAME thing (metadata only, not data
  bytes). The indexed `CachedMetadataReaderFactory` is the equivalent. Neither caches data; OS
  page cache covers local reads.
- So `metadata_load_time` (3–4ms) and `time_elapsed_opening` (4–7ms) on the indexed path are NOT
  cache misses — they're `ArrowReaderMetadata::load_async` (Arrow-schema derivation) + array-reader
  construction run ONCE PER RG (×110). No cache addresses this; only fewer streams do.

### CrossRtStream is irrelevant for aggregations
It wraps the top-level plan output (one mpsc hop, cap 1–2) between the CPU DedicatedExecutor and
the IO runtime — Rust-internal, NOT the Java/Rust (FFM) boundary. For aggregation queries only the
final result rows cross it (18 for q08). Symmetric with vanilla (which wraps identically). Strike
it from suspects for group-by shapes.

---

## 5. The remaining time split (q08, RELEASE, pushdown OFF — the canonical at-parity-but-not case)

Indexed `QueryShardExec elapsed_compute = 57.5ms` vs vanilla `decode 27.5 + FilterExec 15.25 + AggPartial 3.25 ≈ 42.7ms`. **Gap ≈ +15ms CPU**, two levers (decode is at parity: 29.4 vs 27.5ms):

| work | vanilla | indexed | Δ | lever |
|---|---|---|---|---|
| decode | 27.49 | 29.43 (`parquet_poll_time`) | +1.9 | — parity |
| residual filter eval+apply | 15.25 (`FilterExec`) | 22.65 (`on_batch_mask 19.22` + `filter_record_batch 3.43`) | **+7.4** | **LEVER 1** |
| candidate compute | — | 2.15 (`index_query_time`) | +2.2 | lever 2 |
| per-RG open + metadata | ~1.0 | 4.33 + 3.31 | **+6.7** | **LEVER 2** |
| coalesce | — | 0.48 | +0.5 | minor |
| aggregate | 3.25 | 3.26 | — | — |

**Lever 1 — residual filter (`on_batch_mask`), ~+7ms, contained, NO architecture change.**
Indexed does it in two passes (build boolean mask in `on_batch_mask`, then apply in
`filter_record_batch`) vs vanilla's fused vectorized `FilterExec`. Same predicate, same 7.15K
batches. Suspect: `evaluate_residual` (`eval/eval_helpers.rs`) re-maps/re-plans the `PhysicalExpr`
to the batch schema on every call. Fix: compile/cache the residual `PhysicalExpr` once per query
and/or fuse eval+apply (or delegate to a real vectorized filter). NEXT STEP — was about to read
`evaluate_residual`/`on_batch_mask` to confirm per-batch re-planning.

**Lever 2 — per-RG construction, ~+9ms (opening + metadata + index_query), ARCHITECTURAL.**
110 per-RG `DataSourceExec`/`ArrowReaderMetadata`/array-reader builds vs vanilla's ~per-file. Only
removed by building ONE stream over all candidate RGs (session-1 §5 "big lever"). Bigger change;
also recovers nothing on decode (already parity) but removes setup + would simplify the predicate-cache
and CrossRtStream story.

---

## 6. Ranked next steps
1. **Lever 1: residual `on_batch_mask` eval.** Read `evaluate_residual`/`PredicateOnlyEvaluator::on_batch_mask`,
   confirm per-batch expr re-planning, cache the compiled expr, fuse eval+apply. ~+7ms, contained.
2. **q07 stats short-circuit for min/max** (and bounds aggregates). Biggest single suite win (~47×→).
3. **Gate pushdown on `min_skip_run==1`** (`stream.rs` ~990: `let push = base_push && min_skip_run==1 && !forbid`)
   — kills the non-selective pushdown regressions while keeping the selective winners. Re-run §3 suite to confirm.
4. **q28 indexed-only 500** ("Failed to start streaming fragment") — correctness/robustness bug.
5. **EventTime-sort cluster (q24/25/27)** — investigate why EventTime sort specifically (vs q26 SearchPhrase parity).
6. **Lever 2: one DataSourceExec over candidate RGs** — architectural, removes per-RG setup (~9ms) and
   the predicate-cache-when-unprojected waste.
7. **Before landing:** remove the TEMP `api.rs::stream_close` profile instrumentation + the `displayable` use.

## 7. File map (additions to session-1 handoff §6)
| What | Where |
|---|---|
| ClickBench harness + queries | `rust/docs/cb_harness.py`, `rust/docs/cb_queries.json` |
| pushdown gate (min_skip_run / needs_row_mask) | `stream.rs` ~985–995 (`let push = ...`) |
| min_skip_run selection | `stream.rs` ~880 (`let min_skip_run = match force_strategy ...`) |
| per-RG stream build + RowFilter + access plan | `indexed_table/parquet_bridge.rs::create_stream_with_access_plan` |
| metadata cache wiring | `indexed_executor.rs` (~108–130 RuntimeEnvBuilder), `segment_info.rs::build_segments`, `parquet_bridge.rs::load_parquet_metadata` |
| residual eval (LEVER 1) | `indexed_table/eval/eval_helpers.rs::evaluate_residual`, `eval/predicate_evaluator.rs::on_batch_mask` |
| arrow-rs async reader (no prefetch) | `~/Documents/dev/arrow-rs/parquet/src/arrow/async_reader/mod.rs::poll_next_inner` |
| predicate cache size config | `~/Documents/dev/datafusion/.../config.rs:930`, arrow-rs `arrow_reader/mod.rs:184` (100MB) |
