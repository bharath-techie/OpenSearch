# Route-pure-parquet-through-indexed: perf handoff

> Goal: make pure-parquet queries (no Lucene-delegated filters) run through the
> indexed `QueryShardExec` pipeline with perf equal to the vanilla `ListingTable`
> / `DataSourceExec` path, so the two scan implementations can collapse into one.
>
> Status: routing works and is **byte-exact correct** vs vanilla. Three perf fixes
> landed (uncommitted). Indexed is still ~1.7× slower than vanilla on the q08 shape.
> Root cause of the remaining gap is NOT yet conclusively measured — see §5.

> WARNING TO FUTURE READER (and author): this investigation produced THREE wrong
> conclusions before they were caught. Do not trust a perf claim in this area
> until you've checked it against §3 (metric provenance) and §2 (test gotchas).
> Re-verify before acting.

---

## 0. Changes made this session (ALL UNCOMMITTED on `main`)

### Rust (`sandbox/plugins/analytics-backend-datafusion/rust/src`)
1. **Routing gate** — `ffm.rs` `use_indexed`: added
   `|| (query_config.route_pure_parquet_through_indexed && !has_row_id)`.
   `!has_row_id` guard: a row-id query that isn't Lucene-delegated carries
   `indexed_config=None`, so the indexed path would NOT emit shard-global row-ids.
2. **Config field** — `datafusion_query_config.rs`: `route_pure_parquet_through_indexed`
   (wire offset 76) + `indexed_pushdown_filters` already existed (offset 36).
3. **Perf fix A** — `indexed_table/eval/predicate_evaluator.rs`:
   `PredicateOnlyEvaluator::needs_row_mask() -> false`. The stream's `current_mask`
   is never consumed for this evaluator (`finalize_batch` applies the `on_batch_mask`
   residual exclusively), so building it was waste. Mirrors `TreeBitsetSource`.
4. **Perf fix B** — same file: `prefetch_rg` sets `mask_buffer: None` (the packed-bits
   buffer is dead once needs_row_mask=false).
5. **Perf fix C** — `indexed_table/eval/mod.rs` added `PrefetchedRg.selection_runs:
   Option<Vec<(usize,usize)>>`; `predicate_evaluator.rs` populates it from
   `compute_page_ranges`; `indexed_table/stream.rs` (~line 902) builds the parquet
   `RowSelection` from those runs (O(#runs)) instead of
   `build_row_selection_with_min_skip_run` walking the full candidate bitmap
   bit-by-bit (O(#set bits)). All other `PrefetchedRg` constructors set `None`.
6. **TEMP INSTRUMENTATION — MUST REMOVE BEFORE LANDING** — `api.rs::stream_close`:
   logs `DisplayableExecutionPlan::with_metrics(plan).indent(true)` as
   `QUERY_PROFILE plan+metrics:` via `native_bridge_common::log_info!`. Both vanilla
   and indexed store `physical_plan` on the `QueryStreamHandle`, so this dumps the
   per-operator EXPLAIN-ANALYZE for either path. Remove it (and the `displayable`
   use) when done profiling.

### Java (`src/main/java/org/opensearch/be/datafusion`)
- `WireConfigSnapshot.java`: `BYTE_SIZE` 76→80; new `routePureParquetThroughIndexed`
  (offset 76); `indexedPushdownFilters` (offset 36) changed from **hardcoded 1** to a
  builder field (default true).
- `DatafusionSettings.java`: two new dynamic cluster settings (defaults preserve old
  behaviour):
  - `datafusion.indexed.route_pure_parquet_through_indexed` (default false)
  - `datafusion.indexed.indexed_pushdown_filters` (default true)
- Tests updated: `WireConfigSnapshotTests` (BYTE_SIZE 80, offset 76/36),
  `DatafusionSettingsTests` (ALL_SETTINGS.size 29).

---

## 1. How to build

### Rust dylib (the .dylib the node loads)
```bash
# MUST cd into this dir. Do NOT use --manifest-path from elsewhere:
# cargo discovers .cargo/config.toml from the CWD upward, and that file carries
# `rustflags = ["--cfg","tokio_unstable",...]`. Building via --manifest-path from
# the repo root drops the cfg and fails with BOGUS E0599 errors in stats.rs
# (worker_poll_count etc. — those methods ARE cfg(tokio_unstable)-gated, not missing).
cd /Users/gbh/Documents/dev/OpenSearch/sandbox/libs/dataformat-native/rust
cargo build            # debug → target/debug/libopensearch_native.dylib
# (release: cargo build --release → target/release/...)
```
The crate `opensearch-datafusion` is a workspace member of dataformat-native, so a
plain `cargo build` here recompiles it. ~15-90s.

### Java plugin jar (only when Java/wire/settings changed)
```bash
cd /Users/gbh/Documents/dev/OpenSearch
./gradlew :sandbox:plugins:analytics-backend-datafusion:jar -Dsandbox.enabled=true \
  -x cargoTest -x test -x javadoc -x missingJavadoc
# → sandbox/plugins/analytics-backend-datafusion/build/distributions/analytics-backend-datafusion-3.7.0-SNAPSHOT.jar
```

### Deploy to the archive node
- **dylib**: the archive's `-Djava.library.path` points at `.../dataformat-native/rust/target/debug`,
  so a rebuilt dylib is picked up on next node restart — no copy.
- **jar**: copy into the archive plugins dir:
  ```bash
  cp .../build/distributions/analytics-backend-datafusion-3.7.0-SNAPSHOT.jar \
     /Users/gbh/Documents/data-fol/3.7.0-ARCHIVE/plugins/analytics-backend-datafusion/
  ```
- **CROSS-PLUGIN ABI DRIFT**: the archive's OTHER plugin jars (analytics-engine, etc.)
  are from an older build. If you rebuild only the datafusion jar from current source
  and it references a newer symbol (this session hit
  `NoSuchFieldError: AggregateFunction ... REDUCE_EVAL_OP`), the node crashes at query
  time. Fix: rebuild ALL plugins as a consistent set and replace the WHOLE plugins
  folder. The full set is produced by `./gradlew run -Dsandbox.enabled=true -Pinstalled
  Plugins=[...] ...` (the user's run line) — it installs them under
  `build/testclusters/runTask-0/distro/3.7.0-ARCHIVE/plugins/` BEFORE the node starts
  (the run itself then dies on java.library.path=release if you only built debug — that's
  fine, the jars are already assembled). Copy that whole `plugins/` dir into the archive.

---

## 2. How to test (and the gotchas that produced wrong results)

### Run / restart the node
```bash
kill $(pgrep -f Dopensearch); sleep 3        # stop
cd /Users/gbh/Documents/data-fol/3.7.0-ARCHIVE && ./bin/opensearch > /tmp/os-archive.log 2>&1 &
# wait red→yellow (~30s for 100M docs); query when:
curl -s "localhost:9200/_cat/indices/clickbench?h=health,docs.count"   # -> "yellow 99997497"
```
Logs (incl. the QUERY_PROFILE dump): `…/3.7.0-ARCHIVE/logs/opensearch.log`.
Only `native_bridge_common::log_info!`/`log_debug!` reach the log; plain `log::*` is dropped.

### Query
```bash
curl -s -X POST localhost:9200/_plugins/_ppl -H 'Content-Type: application/json' \
  -d '{"query":"source = clickbench | where AdvEngineID!=0 | stats count() by AdvEngineID | sort - `count()`"}'
```
Cold = clear caches first; hot = immediate repeat:
```bash
curl -s -X POST localhost:9200/clickbench/_cache/clear
```

### Settings (and which path each drives — the names are MISLEADING)
| cluster setting | wire offset | drives | default |
|---|---|---|---|
| `datafusion.indexed.route_pure_parquet_through_indexed` | 76 | the routing gate (vanilla→indexed) | false |
| `datafusion.indexed.parquet_pushdown_filters` | 32 | **VANILLA** path pushdown (`execution.parquet.pushdown_filters`) | false |
| `datafusion.indexed.indexed_pushdown_filters` | 36 | **INDEXED** path pushdown (IndexedStream) | true |
| `datafusion.min_target_partitions` | (separate native call) | floor on target_partitions | (node default ~2) |
| `search.concurrent_segment_search.mode` | — | `none` ⇒ wire target_partitions=1 | — |

**GOTCHA 1 — pushdown setting names are backwards from intuition.** Despite the
`indexed.` prefix, `parquet_pushdown_filters` drives VANILLA; `indexed_pushdown_filters`
drives indexed. A probe loop that left `parquet_pushdown_filters=true` PERSISTENT made an
early profile show vanilla using RowFilter pushdown by default (it does NOT — default false).

**GOTCHA 2 — single-partition is NOT just `mode=none`.** `mode=none` sets the wire
`target_partitions=1`, but `datafusion.min_target_partitions` (default ~2, applied via a
SEPARATE `NativeBridge.setMinTargetPartitions` path) is a FLOOR that overrides it. With the
default floor, vanilla scans `file_groups={2 groups}` (2 parallel decode threads) while
indexed scans `partitions=1`. That asymmetry alone made indexed look ~2× worse than it is.
**For a true single-thread A/B, set `datafusion.min_target_partitions=1` too**, and confirm
in the plan: vanilla `file_groups={1 group}`, indexed `partitions=1`.

**Always reset settings to defaults after testing** (set to null).

---

## 3. What each metric means — AND its provenance (this is where I went wrong)

The QUERY_PROFILE dump renders the plan with `DisplayableExecutionPlan::with_metrics`,
which **aggregates metrics by NAME across the subtree**. For `QueryShardExec` this matters
enormously because of how `QueryShardExec::metrics()` works (table_provider.rs:372): it
takes its own `ExecutionPlanMetricsSet` AND **pushes every inner per-row-group parquet
`MetricsSet` into the combined set** (one inner DataSourceExec is built PER ROW GROUP, ~110
of them for q08). So:

### (a) SUMMED-INNER metrics — origin = DataFusion's ParquetSource/FileStream, summed ×#RGs
These names come from parquet, are emitted by each per-RG inner DataSourceExec, and the
display SUMS them. **NOT comparable to vanilla's single-DataSourceExec value.**
- `time_elapsed_scanning_until_data`, `time_elapsed_scanning_total`,
  `time_elapsed_processing`, `time_elapsed_opening`, `metadata_load_time`,
  `parquet_read_time`, `row_pushdown_eval_time`, `page_index_*`, `statistics_eval_time`,
  `bloom_filter_eval_time`, `pushdown_rows_*`.
- (`output_rows`/`output_batches`/`output_bytes` are explicitly SKIPPED in the merge,
  table_provider.rs:378, to avoid double counting.)
- `bytes_scanned` sums to the correct total (it's additive by nature).

  ⚠️ MY MISTAKE: I read indexed `time_elapsed_scanning_until_data=173ms` vs vanilla `18ms`
  and concluded "indexed serializes IO 110×, time-to-first-data is 10× worse." WRONG —
  173ms was the SUM of 110 inner until-data values; vanilla's 18ms is ONE value. Not
  comparable. The indexed stream's OWN `prefetch_wait_time` was only ~1.2ms, i.e. it is
  NOT meaningfully IO-stalled.

### (b) INDEXED-OWN metrics — single accumulators (defined metrics.rs, accrued stream.rs)
These ARE single per-query accumulators and are the ones to reason about:
- `elapsed_compute` — wraps the ENTIRE `poll_inner` (stream.rs ~710). CPU spent in the
  stream's poll loop: candidate→RowSelection build, PositionMap, coalescer drain, mask
  application, plus the synchronous parts of pulling parquet batches. Catch-all.
- `parquet_poll_time` (stream.rs:766) — time in `current_stream.poll_next()`, i.e. pulling
  decoded batches from the current RG's parquet stream (IO wait + decode for that RG).
- `on_batch_mask_time` — evaluator's residual predicate applied post-decode (the
  `PredicateOnlyEvaluator` path when pushdown is off).
- `build_mask_time` — building `current_mask` (≈0 now: needs_row_mask=false).
- `index_query_time` — `eval_nanos` from `prefetch_rg` (candidate/page-range computation).
  Was ~295ms before fix C (bitmap_to_packed_bits); ~5ms after.
- `prefetch_wait_time` / `prefetch_wait_count` — time the poll thread parked waiting on the
  background `prefetch_rg` task. ~1.2ms for q08 ⇒ prefetch is NOT the bottleneck.
- `coalesce_time` — BatchCoalescer `push_batch` only (drain via `next_completed_batch` is
  NOT timed; it falls into `elapsed_compute`).
- `position_map_identity/bitmap/runs`, `min_skip_run_*`, `rows_matched`, `rg_*` — counters.

### Vanilla side (single DataSourceExec + separate FilterExec)
- `DataSourceExec`: `time_elapsed_scanning_total` (whole scan, pipelined), `output_rows`
  (rows surfaced post page-prune), `bytes_scanned`, `pushdown_rows_*` (0 unless
  parquet_pushdown_filters=true). `elapsed_compute` ~0 (work is inside FileStream timers).
- `FilterExec`: `elapsed_compute` = the residual predicate cost, `selectivity`.

---

## 4. VERIFIED facts for q08 (`where AdvEngineID!=0 | stats count() by AdvEngineID`)

Conditions: 99,997,497 docs, single partition (mode=none AND min_target_partitions=1).
AdvEngineID is 0 in ~99.4% of rows, spread across all RGs ⇒ no RG/page pruning helps much;
page-index prunes 99.96M→58.15M; the selective work is row-level.

- **Correctness: EXACT** match vanilla vs indexed, pushdown on AND off (18 rows). ✅
- Both paths: `bytes_scanned=1.14M` (identical), decode 58.15M rows, same page pruning.
- End-to-end hot median: **vanilla ~253ms, indexed ~434ms (push off) / ~483ms (push on)**
  ⇒ ~1.7×.  (At the WRONG 2-partition setting it was 166 vs 725 ≈ 4.4× — ignore that.)
- Indexed decode is EQUAL to vanilla: indexed `parquet_poll_time≈94ms` ≈ vanilla
  processing `≈91ms`.
- Indexed is NOT IO-stalled: `prefetch_wait_time≈1.2ms`.
- Indexed `elapsed_compute≈191ms` (push off) breaks down roughly as:
  `parquet_poll 94ms` + `on_batch_mask 82ms` + ~15ms loop overhead.
- The residual filter costs MORE on indexed (`on_batch_mask≈82ms`, post-decode pass) than
  on vanilla (`FilterExec elapsed_compute≈52ms`, vectorized) — same predicate.
- **Pushdown ON HURTS this shape**: indexed decode 94→234ms (row-level predicate eval over
  a non-selective filter inside the decoder). Keep `indexed_pushdown_filters=false` for
  non-selective predicates.

### Perf fixes' measured effect (indexed scan `elapsed_compute`)
738ms (before any fix) → 510ms (needs_row_mask=false, fix A) → ~196ms (fix C, page-range
RowSelection). build_mask_time 258→0; index_query_time 295→5. All while staying byte-exact.

---

## 5. The remaining gap — what's CONFIRMED vs OPEN

### Confirmed (code-read)
- Indexed builds a FRESH `DataSourceExec` + `TaskContext::default()` PER ROW GROUP
  (`parquet_bridge.rs::create_stream_with_access_plan`, called once per RG from the poll
  loop), drains it, then moves on. Vanilla builds ONE DataSourceExec over all RGs.
- The IndexReader prefetch (`stream.rs::start_prefetch` → `spawn_blocking` →
  `evaluator.prefetch_rg`) overlaps ONLY the candidate/bitmap computation of the next RG.
  It does NOT open/read the next RG's parquet stream ahead of time.
- Indexed has machinery vanilla lacks: `BatchCoalescer` re-chunking parquet batches, a
  `CrossRtStream` per-batch hop (tokio mpsc channel, cap 1-2; cross_rt_stream.rs) between
  the CPU executor and IO runtime, and `on_batch_mask`/PositionMap per batch.

### OPEN — NOT yet conclusively measured (do this next, carefully)
The user's framing: **vanilla = one ParquetSource auto-streaming all RGs continuously
(reader-level read-ahead across RG boundaries); indexed = manual per-RG drive +
re-coalesce + cross-runtime hop.** That difference is real in code. What is NOT yet
proven is HOW MUCH it costs, because every "scanning"/"processing"/"until_data" parquet
metric on QueryShardExec is summed-inner (§3a) and I have NOT found a clean apples-to-apples
wall-clock split. Open questions to answer with PROPER instrumentation (single accumulators,
not summed-inner):
1. Is `parquet_poll_time≈94ms` (≈vanilla decode) the floor, with the extra ~180ms being
   on_batch_mask(82) + coalescer-drain + CrossRtStream send/recv + per-RG DataSourceExec
   setup? Add discrete single-accumulator timers around: coalescer `next_completed_batch`,
   the CrossRtStream channel send/recv, and per-RG `create_row_selection_stream`.
2. Does the per-batch CrossRtStream channel (cap 1-2) throttle the pipeline? Measure
   send-block time.
3. Would batching multiple RGs into ONE DataSourceExec (instead of one-per-RG) recover the
   reader-level read-ahead and amortize setup?

### Candidate next fixes (in rough priority)
1. **on_batch_mask vs FilterExec (82 vs 52ms).** Why is the post-decode residual ~1.6×
   the vectorized FilterExec for the same expr? `evaluate_residual` (eval_helpers.rs)
   remaps the expr to the batch each call — check for per-batch re-planning overhead.
2. **One DataSourceExec spanning all candidate RGs** instead of per-RG, to get
   parquet-rs cross-RG read-ahead and drop 110× setup. Biggest architectural lever; matches
   the "auto vs manual aggregation across RGs" intuition.
3. **CrossRtStream batching** — larger channel / batch coalescing before the hop.
4. Confirm whether the BatchCoalescer is even needed for FilterClass::None (vanilla
   doesn't re-coalesce; aggregate consumes whatever batch sizes the scan emits).

---

## 6. File map
| What | Where |
|---|---|
| routing gate | `src/ffm.rs` `use_indexed` |
| config + wire decode | `src/datafusion_query_config.rs` |
| PredicateOnlyEvaluator (fixes A,B,C) | `src/indexed_table/eval/predicate_evaluator.rs` |
| PrefetchedRg.selection_runs | `src/indexed_table/eval/mod.rs` |
| stream poll loop / RowSelection / coalescer / masks | `src/indexed_table/stream.rs` |
| per-RG parquet stream build | `src/indexed_table/parquet_bridge.rs` |
| QueryShardExec.metrics() merge (summed-inner!) | `src/indexed_table/table_provider.rs:372` |
| metric definitions | `src/indexed_table/metrics.rs` |
| TEMP profile dump (REMOVE) | `src/api.rs::stream_close` |
| Java wire | `src/main/java/.../WireConfigSnapshot.java` |
| Java settings | `src/main/java/.../DatafusionSettings.java` |
| cross-runtime hop | `src/cross_rt_stream.rs` |

Related: `scan-timing-investigation.md`, `route-pure-parquet-through-indexed.md`,
`scan-latency-waterfall.md`.
