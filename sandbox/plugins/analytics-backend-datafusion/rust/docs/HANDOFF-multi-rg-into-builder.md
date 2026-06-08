# HANDOFF — one-decoder RG-by-RG indexed scan via backported arrow-rs `into_builder`

Date: 2026-06-11. Branch: `switch-indexed` (OpenSearch repo). Status: **compiles,
correctness-tested (all e2e diff tests green), NOT yet benchmarked end-to-end.**

────────────────────────────────────────────────────────────────────────
## UPDATE 2026-06-11 (session 2 — integration verification)
────────────────────────────────────────────────────────────────────────
- Ran the correctness gate: all 5 `diff_*` tests + the full `indexed_table`
  suite (now 429 tests) pass on the rewritten `decoder_stream` path. Debug build
  + full dylib both link clean.
- **FOUND + FIXED a real integration bug (projection over a reordered file).**
  `decoder_stream::run` built its `ProjectionMask` with
  `ProjectionMask::roots(parquet_schema, args.projection)`, but `args.projection`
  is indexed in TABLE-schema space while `roots` wants PHYSICAL parquet root
  indices. When `infer_schema` reorders columns (e.g. alphabetical) AND the query
  projects a strict subset, the multi-RG path decoded the WRONG physical columns.
  The per-RG path is immune because `ParquetSource::new(full_schema)` +
  `with_projection_indices` maps by name. SELECT * was unaffected
  (`ProjectionMask::all()`). Fix: translate table→physical root indices by field
  name in `run()` (uses the previously-dead `full_schema` field). Drift columns
  absent from the file are skipped (refine_batch's name-based fixup covers them).
- Added 2 differential tests in `tests_e2e/schema_drift.rs`
  (`diff_reordered_subset_projection_single_col` / `_two_cols`) that write a file
  whose physical order ≠ table order and project a subset. CONFIRMED they FAIL
  without the fix (multi-RG returns wrong/empty columns) and PASS with it.
- Cleanup: removed the unused `ObjectStoreExt` import and the dead `emit_row_ids`
  field from `DecoderStreamArgs` (the real gate is `row_id_output_index`, mirroring
  IndexedStream). Only remaining decoder_stream warning is the `with_page_index`
  deprecation, which matches existing codebase convention (page_pruner.rs etc.).
- Verified the prefetch OVERLAP is structurally correct: `advance_to_next_rg`
  calls `arm_next_prefetch()` for RG n+1 BEFORE awaiting RG n's handle and
  returning its decoder, so n+1's Lucene runs on a blocking task during n's decode.
- STILL TODO: Task #7 (cluster setting, still env-var).
  NOT committed — left open for review per user instruction.

### UPDATE 2026-06-11 (session 2b — live-node A/B + a SECOND real bug)
- **FOUND + FIXED a second, worse bug: the FFM/Lucene collector was invoked
  CONCURRENTLY.** `advance_to_next_rg` armed RG n+1's prefetch BEFORE awaiting
  RG n's handle, so at stream start RG0 + RG1's `collect_packed_u64_bitset` ran
  simultaneously on the same per-query Lucene handle (NOT reentrant). On the live
  node, every delegation/`match()` query with the flag ON died with Java
  `ArrayIndexOutOfBoundsException: Index 63 out of bounds for length 32` /
  `numBytes must be >= 0, got -257` → `collectDocs(...) failed: -1` (buffer
  corruption). The e2e diff tests use a thread-SAFE MockCollector, so they passed
  while the real path was broken — DO NOT trust mock-only tests for FFM behavior.
  Fix: await current prefetch, THEN arm next (overlap-with-decode preserved, never
  two collects at once). New regression test `streaming_at_scale.rs::
  multi_rg_never_collects_concurrently` (SerialGuardCollector) catches it.
- **REAL benchmark numbers (release, archive clickbench ~100M, median-of-8 warm,
  flag OFF=per-RG vs ON=multi-RG), byte-identical results both ways:**
  * q08 (pure-parquet, AdvEngineID!=0):            298ms → 137ms  **~2.2x**
  * deleg_lucene  (match(Referer,"http"), ~66%):  1357ms → 1250ms  ~1.09x
  * deleg_residual(match + AdvEngineID!=0):         999ms →  882ms  ~1.13x
  The PRIOR-MEASURED-NUMBERS section below is from the DELETED all-up-front
  design — ignore it. The pure-parquet win is real and large; delegation is
  dominated by the Lucene collect itself, so amortizing decoder setup helps less.
  Both delegation queries were the ones that EXPOSED bug 2 — verified correct
  (byte-identical to per-RG) and crash-free after the fix.

────────────────────────────────────────────────────────────────────────
## TL;DR — where we are
────────────────────────────────────────────────────────────────────────
- Goal: make the indexed parquet scan build the parquet decoder ONCE per segment
  chunk (not once per row group) to kill the per-RG decoder-setup tax
  (~107ms `parquet_first_poll_time` on q08, the dominant cost), WITHOUT regressing
  collector queries.
- Mechanism: backported arrow-rs `ParquetPushDecoder::into_builder` (PR #9968 + its
  prereq #9804) onto local arrow 58.3.0, pinned via `[patch.crates-io]`. DataFusion
  54.0.0 consumes it unchanged (verified: builds clean, additive API only).
- New module `src/indexed_table/decoder_stream.rs` drives a `ParquetPushDecoder`
  directly (bypasses `DataSourceExec`), one RG at a time, reusing the parsed
  `ArrowReaderMetadata` (schema parse done ONCE), with Lucene eval overlapped with
  decode via per-RG `spawn_blocking` prefetch.
- **It compiles** (`cargo build -p opensearch-datafusion` → Finished, 0 errors).
- NOT yet done: run differential e2e tests (`diff_*`), wire the flag as a cluster
  setting (still env-var), end-to-end q08 A/B on the archive node.

────────────────────────────────────────────────────────────────────────
## THE USER'S CONSTRAINTS / MUST-DOs (do not violate)
────────────────────────────────────────────────────────────────────────
1. **RG-BY-RG, ONE LUCENE PER RG, NO UP-FRONT STAGING.** The single hardest
   constraint. The decode must stay row-group-by-row-group. Each RG's Lucene/FFM
   candidate eval (`evaluator.prefetch_rg`) runs PER RG and is OVERLAPPED with the
   previous RG's decode (the existing prefetch model). DO NOT pre-evaluate all RGs'
   Lucene before decoding — that "all-up-front" design was explicitly REJECTED by
   the user because it serializes every collector call before the first row and
   destroys overlap. ("no upfront everything is not an option", "one rg at a time
   is only possible".)
2. **Parquet decoder = all RGs amortized; Lucene = per RG.** The user's exact
   framing: "why are we confusing lucene and parquet. parquet can still be all RGs,
   but lucene we can feed per RG since we can change everything including row
   selection per RG." So: amortize the parquet/arrow decoder SETUP across RGs, but
   feed each RG's Lucene-derived RowSelection separately.
3. **Forks are OK** but minimize blast radius. We chose Route 2 (backport the 2
   upstream commits onto local 58.3.0) over Route 1 (bump whole workspace to DF
   main + arrow 59). DF 54 must keep working unchanged.
4. **Switch must be a CLUSTER SETTING wired via DF config** (Java
   `WireDatafusionQueryConfig` + cluster-setting registration), NOT the env var.
   Currently still env-var (`OPENSEARCH_INDEXED_MULTI_RG_DECODE`) — Task #7, TODO.
5. **Byte-exact correctness.** Output must be identical to the per-RG path. Proven
   by the `diff_*` differential e2e tests (run same trees both ways, assert equal).
6. **Gate to non-dynamic-filter queries.** TopK/join keep the per-RG path (a single
   long-lived scan can't drop RGs mid-flight as a dynamic filter tightens). Already
   gated in `IndexedExec::execute`: `indexed_multi_rg_decode && dynamic_filter.is_none()`.
7. Build discipline: ALWAYS `cd /Users/gbh/Documents/dev/OpenSearch/sandbox/libs/
   dataformat-native/rust && cargo build` (needs `tokio_unstable` cfg from
   `.cargo/config.toml`; never `--manifest-path` from elsewhere). `cargo test`
   (dev) NOT `--release` (release LTO takes 40min+ to compile the test crate).

────────────────────────────────────────────────────────────────────────
## BUILD & PATHS (read this before touching anything)
────────────────────────────────────────────────────────────────────────
### Key paths
- OpenSearch repo root:        `/Users/gbh/Documents/dev/OpenSearch`
- Rust workspace ROOT (build here): `/Users/gbh/Documents/dev/OpenSearch/sandbox/libs/dataformat-native/rust`
  (this is the cargo workspace; its `Cargo.toml` has `[workspace]` + the
  `[patch.crates-io]` arrow pin + the build profiles + `.cargo/config.toml`.)
- The crate we edit:           `/Users/gbh/Documents/dev/OpenSearch/sandbox/plugins/analytics-backend-datafusion/rust`
  (member `opensearch-datafusion`; `src/indexed_table/` is the code in play.)
- Local arrow-rs (patched):    `/Users/gbh/Documents/dev/arrow-rs` (branch `58.3.0-into-builder`)
- Local datafusion (read-only reference, DO NOT build): `/Users/gbh/Documents/dev/datafusion`
- Archive node:                `/Users/gbh/Documents/data-fol/3.7.0-ARCHIVE`
- Produced dylib (loaded by node): `.../dataformat-native/rust/target/{debug,release}/libopensearch_native.dylib`
- ClickBench parquet for the spike: `/Users/gbh/Downloads/hits_0.parquet`

### How to build — ALWAYS cd into the workspace root first
```
cd /Users/gbh/Documents/dev/OpenSearch/sandbox/libs/dataformat-native/rust
cargo build -p opensearch-datafusion           # just the crate, fastest compile check
cargo build                                    # full dylib (libopensearch_native) for the node
```
CRITICAL: you MUST `cd` into that dir. cargo reads `.cargo/config.toml` from cwd
upward — it carries `rustflags = ["--cfg","tokio_unstable", ...]`. Running with
`--manifest-path` from elsewhere SILENTLY DROPS `tokio_unstable` and fails with
bogus E0599 errors in `stats.rs` (`worker_poll_count` etc. — those are
`#[cfg(tokio_unstable)]`-gated, NOT version drift).

### USE DEBUG BUILDS FOR ITERATION (release LTO is brutally slow here)
- `[profile.release]` has `lto=true, codegen-units=1` → compiling the crate takes
  ~3-4min and the TEST crate ~40min+. DO NOT use `--release` while iterating.
- `[profile.dev]` has `lto=false, codegen-units=16` → crate compiles in ~10-15s
  incremental. Use debug for ALL dev/test cycles:
  ```
  cargo build -p opensearch-datafusion          # debug, fast
  cargo test  -p opensearch-datafusion --lib indexed_table::tests_e2e::boolean_algebra::diff_
  cargo test  -p opensearch-datafusion           # full crate tests, debug
  ```
- Correctness is profile-independent → debug is sufficient to validate the diff_*
  tests and all logic. Only build `--release` when you need real perf numbers.
- To run the node against a DEBUG dylib for quick functional checks: point
  jvm.options at the debug dir (the node currently uses release):
  ```
  # in 3.7.0-ARCHIVE/config/jvm.options, the -Djava.library.path line:
  #   .../dataformat-native/rust/target/debug   (debug, fast rebuilds)
  #   .../dataformat-native/rust/target/release (release, for perf A/B)
  ```
  This session switched it to `target/release`; flip to `target/debug` for fast
  functional iteration, back to `target/release` for the q08 benchmark.
  NOTE: a debug dylib is ~10-50x slower at runtime — fine for correctness on a
  small query, useless for perf numbers. Build `cargo build --release` (no `-p`,
  full dylib) only for the actual benchmark.

### Spike example (standalone, no node)
```
cd /Users/gbh/Documents/dev/OpenSearch/sandbox/libs/dataformat-native/rust
cargo run --release -p opensearch-datafusion --example multi_rg_spike -- <parquet> <runs> <rg_size>
```
(Lives at `.../analytics-backend-datafusion/rust/examples/multi_rg_spike.rs`.)

### Restart the node after a dylib rebuild
```
pkill -f 'Dopensearch'; sleep 3
cd /Users/gbh/Documents/data-fol/3.7.0-ARCHIVE && ./bin/opensearch
# with the experimental flag (until Task #7 makes it a cluster setting):
OPENSEARCH_INDEXED_MULTI_RG_DECODE=1 ./bin/opensearch
```

### The arrow patch (already in place — don't re-do unless rebasing)
`[patch.crates-io]` in the workspace `Cargo.toml` points all arrow-rs crates at
`/Users/gbh/Documents/dev/arrow-rs/<crate>`. That checkout must stay on branch
`58.3.0-into-builder`. If you `git checkout` arrow-rs elsewhere, the OpenSearch
build breaks. To verify the pin resolves: `cargo metadata --format-version 1 >/dev/null`.

────────────────────────────────────────────────────────────────────────
## RELEVANT PRs / REFS
────────────────────────────────────────────────────────────────────────
- **arrow-rs #9968** "Add ParquetPushDecoder::into_builder ..." merged into arrow
  main 2026-05-20 (commit `7c6eb2cbd`), shipped in RELEASED arrow **59.0.0**.
  Adds `into_builder()`, `is_at_row_group_boundary()`, `row_groups_remaining()`,
  `try_next_reader()`. Superseded the older `swap_strategy`/`StrategySwap` design.
- **arrow-rs #9804** "separate push decoder frontier state from row-group decoding"
  (commit `48fa8a7a4`) — prereq for #9968 (adds RowGroupFrontier/RowBudget).
- **datafusion #22237** (adriangb, `AdaptiveParquetStream`) — the CONSUMER reference
  for driving one decoder RG-by-RG. Head `e3f7f3e0c8133a4fed30ed21d76826d920e88ea2`,
  branch `pr4-adaptive-parquet-scan`. Its loop: `try_next_reader` per RG +
  `maybe_swap_strategy` at each boundary. We don't need its adaptive-pushdown cost
  model — just the one-decoder-per-RG-boundary shape.
- **datafusion #22289** — extracted `PushDecoderStreamState` (the `unfold`-based
  driver pattern we mirror). #22407 — runtime RG early-stop (we already have the
  equivalent in `dynamic_filter::DynamicRgPruner`; not in scope here).
- pydantic/arrow-rs branch `adaptive-strategy-swap` — staging ground for #9968;
  ignore now that it's upstream/released.

Reference source copies pulled this session live in /tmp: adrian_opener.rs,
adrian_source.rs, adrian_selectivity.rs, adrian_row_filter.rs, pyd_mod2.rs,
pyd_remaining.rs, pyd_rb_mod.rs.

────────────────────────────────────────────────────────────────────────
## WHAT WAS CHANGED (files)
────────────────────────────────────────────────────────────────────────
### arrow-rs (`~/Documents/dev/arrow-rs`)
- New branch `58.3.0-into-builder` off the `58.3.0` tag.
- Cherry-picked `48fa8a7a4` (#9804) then `7c6eb2cbd` (#9968). BOTH clean, no
  conflicts. `cargo build -p parquet --features arrow` → Finished.
- `into_builder`/`is_at_row_group_boundary`/`row_groups_remaining` now present in
  `parquet/src/arrow/push_decoder/mod.rs`.

### Workspace pin (`sandbox/libs/dataformat-native/rust/Cargo.toml`)
- Added `[patch.crates-io]` pinning ALL arrow-rs workspace crates (arrow, arrow-*,
  parquet — 15 crates) to local paths `/Users/gbh/Documents/dev/arrow-rs/<crate>`.
  Needed so the patched parquet's arrow deps resolve to one consistent 58.3.0 tree.
- Verified: `cargo build -p opensearch-datafusion` Finished in ~2m48s (cold),
  0 errors — **DF 54 + patched arrow compile together.**

### `src/indexed_table/decoder_stream.rs` (NEW, ~520 lines) — the consolidated path
- `DecoderStreamArgs` — inputs from `IndexedExec::execute`.
- `build_decoder_stream(args) -> SendableRecordBatchStream` — wraps `run()` in
  `RecordBatchStreamAdapter`.
- `run()` — derives `ArrowReaderMetadata::try_new` ONCE (page index OFF), builds a
  `ProjectionMask`, then `futures::stream::unfold(DriverState, ...)` (dependency-free,
  mirrors DF's `PushDecoderStreamState::into_stream`).
- `DriverState::step()` — the async driver:
  * seeds a `LimitedBatchCoalescer` + arms first prefetch + `advance_to_next_rg`.
  * drains coalescer → drives `decoder.try_decode()`:
    - `NeedsData(ranges)` → `store.get_ranges(...).await` → `push_ranges`.
    - `Data(batch)` → `refine_batch` (= finalize_batch logic) → coalescer.
    - `Finished` → `advance_to_next_rg()` (await pending prefetch, build next decoder).
- `arm_next_prefetch()` — `spawn_blocking(evaluator.prefetch_rg(rg+1))` so RG n+1's
  Lucene overlaps RG n's decode. Skips RGs outside doc_range.
- `advance_to_next_rg()` — awaits the pending prefetch, arms the FOLLOWING one
  (overlap), `plan_rg` → builds a fresh decoder for that ONE rg via
  `build_decoder(rg_index, selection)` = `ParquetPushDecoderBuilder::new_with_metadata
  (arrow_meta.clone()).with_projection(...).with_batch_size(...).with_row_groups(
  [rg_index]).with_row_selection(sel).build()`. Skips empty (no-candidate) RGs.
- `plan_rg` → calls shared `build_rg_plan` (see below) → `(RowSelection, RgState)`.
- `refine_batch` → MIRRORS `IndexedStream::finalize_batch` exactly (on_batch_mask,
  candidate mask slice, row-id injection, projection fixup). Per-RG `batch_offset`/
  `mask_offset` live in `RgState`.

### `src/indexed_table/stream.rs` (per-RG path — restored to clean baseline)
- DELETED the old all-up-front consolidated code: `StagedRg` struct, `poll_build_staged`,
  `load_staged_rg`, the `consolidated`/`staged_*` fields, the finalize_batch boundary
  block, the poll_inner consolidated branch.
- EXTRACTED `build_rg_plan` from an `IndexedStream` method into a `pub(super)` FREE
  FUNCTION (line ~96) taking explicit params (force_strategy, min_skip_run_default,
  threshold, &evaluator, &metrics, rg, candidates, mask_buffer, selection_runs).
  Shared by BOTH the per-RG path and `decoder_stream` so they can't drift.
- `IndexedExec::execute` now branches: if `indexed_multi_rg_decode &&
  dynamic_filter.is_none()` → `decoder_stream::build_decoder_stream(...)`; else the
  existing `IndexedStream`. `evaluator` is taken from the mutex BEFORE the branch.
- `RowGroupInfo`, `FilterStrategy` are already `pub`. `build_rg_plan` is `pub(super)`.

### `src/indexed_table/mod.rs`
- Added `pub mod decoder_stream;`

### `src/indexed_table/parquet_bridge.rs`
- REMOVED `create_multi_rg_selection_stream` (was used only by deleted poll_build_staged).
- KEPT `create_multi_rg_full_scan_stream` (used by the spike example `examples/multi_rg_spike.rs`).

### `src/datafusion_query_config.rs`
- `indexed_multi_rg_decode: bool` field, currently from env var
  `OPENSEARCH_INDEXED_MULTI_RG_DECODE` via `multi_rg_decode_from_env()`. Builder
  method `.indexed_multi_rg_decode(bool)` for tests. **TASK #7: replace env with
  a real cluster setting wired through `WireDatafusionQueryConfig` + Java
  MemoryLayout + cluster-setting registration (datafusion.indexed.multi_rg_decode).**

### `src/indexed_table/tests_e2e/{mod.rs,boolean_algebra.rs}`
- `run_tree_multi_rg` / `run_tree_and_plan_cfg(tree, multi_rg)` harness param +
  `.indexed_multi_rg_decode(multi_rg)` in the qc builder.
- 5 `diff_*` differential tests (collector, predicate-only, AND, OR-nested-AND,
  NOT-mixed) asserting per-RG vs multi-RG output is byte-identical. NOTE: these were
  written against the OLD all-up-front path; they should still be valid for the new
  path (same `build_rg_plan`, same finalize logic) but HAVE NOT been re-run since the
  rewrite. RUN THEM FIRST.

### Version revert (separate, done this session)
- Branch reverted from 3.8.0 to 3.7.0: `Version.java` CURRENT=V_3_7_0 (removed
  V_3_7_1/V_3_8_0), `.ci/bwcVersions` (removed 3.7.0 line), version.properties +
  libs.versions.toml already at 3.7.0. (reactor 3.8.6 is an unrelated dep, left alone.)

────────────────────────────────────────────────────────────────────────
## IMMEDIATE NEXT STEPS (in order)
────────────────────────────────────────────────────────────────────────
1. **Run differential tests** (correctness gate):
   `cd sandbox/libs/dataformat-native/rust && cargo test -p opensearch-datafusion --lib \
     indexed_table::tests_e2e::boolean_algebra::diff_`
   They force `indexed_multi_rg_decode=true` via the builder. If any fail, the new
   decoder_stream's refine_batch/plan_rg diverges from the per-RG path — debug that
   FIRST. Also run the full `indexed_table` suite for regressions.
2. **Clean up warnings** in decoder_stream.rs (unused imports: check `build_mask`,
   `Duration`, `RowSelector`, `full_rg_selection` helper may be dead now).
3. **Verify the overlap actually happens** — add/inspect a temp log or metric: RG
   n+1 prefetch should be in-flight (spawn_blocking) while RG n decodes. The
   `arm_next_prefetch` is called at the START of `advance_to_next_rg` for the
   FOLLOWING rg — confirm timing is right (prefetch armed before we return the
   current decoder, so it runs during decode).
4. **Wire the cluster setting (Task #7)** — replace env var with
   `datafusion.indexed.multi_rg_decode`: add wire field + Java MemoryLayout +
   register cluster setting (mirror `route_pure_parquet_through_indexed`, which the
   archive's fresh jars already recognize). Remove `multi_rg_decode_from_env`.
5. **End-to-end q08 A/B on the archive node** (see harness below). Compare per-RG
   (flag off) vs multi-RG (flag on), both with route_pure_parquet ON, single-partition.
   Confirm byte-identical RESULT_ROWS + measure the `parquet_first_poll_time` drop.

────────────────────────────────────────────────────────────────────────
## KNOWN RISKS / OPEN QUESTIONS for the new session
────────────────────────────────────────────────────────────────────────
- **into_builder is NOT actually used yet.** The current decoder_stream REBUILDS the
  decoder fresh from a cloned `arrow_meta` per RG (`build_decoder`), rather than
  `into_builder()`. This is intentional and CORRECT (into_builder errors on Finished;
  after one RG the decoder IS Finished). Rebuilding from the shared `arrow_meta`
  reuses the parsed `fields` (the expensive schema parse) — that's where the
  amortization comes from. The only thing NOT carried is buffered bytes (re-fetched,
  but those are bytes we need anyway). VERIFY this still beats per-RG: the per-RG
  tax was DataSourceExec + TaskContext + `ArrowReaderMetadata::load` (metadata
  re-derive) per RG; we skip ALL of that. If benchmarks show it's NOT faster, the
  fallback is to genuinely use `into_builder` by building ONE decoder over all RGs
  and feeding per-RG selection at boundaries (needs the head-RG-selection-override
  trick — see multi-rg-fork-design.md §3).
- **Metrics parity**: decoder_stream doesn't populate `inner_parquet_metrics` /
  `bytes_scanned` / page-prune counters (those came from DataSourceExec's
  ParquetFileMetrics). EXPLAIN ANALYZE will show fewer scan metrics on this path.
  Acceptable for now; note it.
- **prefetch_rg dynamic-filter prune**: the per-RG IndexReader path passes a
  dynamic_prune_ctx into prefetch. decoder_stream does NOT (gated to no-dynamic-filter
  anyway, so fine). Don't add dynamic filters to this path.
- The `diff_*` tests use `force_strategy(BooleanMask)` + `force_pushdown(false)`.
  Confirm decoder_stream honors force_strategy via build_rg_plan (it does — same fn).

────────────────────────────────────────────────────────────────────────
## ARCHIVE-NODE BENCHMARK HARNESS (working, from this session)
────────────────────────────────────────────────────────────────────────
- Node: `/Users/gbh/Documents/data-fol/3.7.0-ARCHIVE`, `./bin/opensearch`. Loads the
  release dylib via `-Djava.library.path=.../dataformat-native/rust/target/release`
  (jvm.options ALREADY switched debug→release this session).
- The archive's Java jars are FRESH (user updated them) and recognize
  `datafusion.indexed.route_pure_parquet_through_indexed` (older jars did NOT — that
  cost us a bogus vanilla-vs-vanilla A/B earlier).
- Env var passthrough VERIFIED: `OPENSEARCH_INDEXED_MULTI_RG_DECODE=1 ./bin/opensearch`
  reaches the Rust dylib (JVM inherits 78 env vars; confirmed via `ps eww <pid>`).
- q08 = `source = clickbench | where AdvEngineID!=0 | stats count() by AdvEngineID | sort - \`count()\``
- Required settings BOTH runs: `search.concurrent_segment_search.mode=none`,
  `datafusion.min_target_partitions=1`, `datafusion.indexed.route_pure_parquet_through_indexed=true`.
  (q08 is pure-parquet → only routes through indexed path when that flag is ON.)
- Driver script (median over 8 iters, cache-cleared): `/tmp/q08_median.sh`.
- A/B procedure: stop node → start WITHOUT env (per-RG baseline) → run → stop →
  start WITH `OPENSEARCH_INDEXED_MULTI_RG_DECODE=1` (multi-RG) → run → compare.
- The QUERY_PROFILE plan+metrics is logged to `3.7.0-ARCHIVE/logs/...` via
  RustLoggerBridge — grep `parquet_first_poll_count` (per-RG=110, multi=should be
  ~20 segments or per-RG-count depending on how decoder_stream reports) and
  `time_elapsed_*`. RESULT_ROWS must be byte-identical (18 rows for q08).

### PRIOR MEASURED NUMBERS (for reference; the OLD all-up-front multi-RG path)
- per-RG q08 median 229ms; old all-up-front multi-RG 162ms (~1.4x). The win was
  amortizing `parquet_first_poll_time` 107.6ms→0.4ms. SAME mechanism the new path
  targets — expect similar or better, AND no collector-query regression.

────────────────────────────────────────────────────────────────────────
## RELATED MEMORY NOTES (auto-loaded; read for full history)
────────────────────────────────────────────────────────────────────────
- `multi-rg-decoder-consolidation-spike` — the spike + old all-up-front results +
  the corrected mechanism (decoder setup amortization, NOT IO pipelining).
- `indexed-path-slow-on-nonselective-predicate`, `indexed-q08-residual-lever`,
  `scan-timing-investigation` — q08 root-cause history.
- `build-target-dataformat-native`, `run-archive-node-for-benchmarks` — build/run.
- Design docs: `docs/multi-rg-fork-design.md` (full API reading + Route 1 vs 2),
  `docs/multi-rg-consolidation-design.md` (the ABANDONED all-up-front design).
