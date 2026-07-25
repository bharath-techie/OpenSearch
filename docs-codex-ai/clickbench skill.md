---
name: clickbench-local
description: Build the analytics-backend-datafusion native lib + run a local OpenSearch sandbox cluster against a ClickBench index, then A/B perf + correctness test with ab_perf_stats.sh. Use when iterating on the DataFusion/parquet backend (page-index cache, scoped page index, query perf) and you want to measure latency/stats deltas or check correctness against a baseline.
---

# ClickBench local build + test

This project's analytics backend is Rust (DataFusion) loaded as a native lib by a Java
OpenSearch plugin. Local iteration = rebuild the Rust lib → (re)launch the sandbox
cluster → run PPL queries against a `clickbench` index → compare with `ab_perf_stats.sh`.

Repo root: `/Users/gbh/Documents/dev/OpenSearch`. Run commands from there unless noted.

## 1. Build the native Rust lib

The cargo workspace root is `sandbox/libs/dataformat-native/rust` (the
`analytics-backend-datafusion` crate is a member of it). Build from there:

```bash
cd sandbox/libs/dataformat-native/rust
cargo build              # debug → target/debug/  (fast iteration; pairs with -PrustDebug)
# cargo build --release  # optimized → target/release/ (for real perf numbers)
```

- The cluster loads the `.dylib`/`.so` via `-Djava.library.path=.../rust/target/debug`
  (see launch command below). **Debug build → `target/debug`, release → `target/release`;
  the `java.library.path` must point at whichever you built.**
- For **perf measurements always use `--release`** and point `java.library.path` at
  `target/release` — debug DataFusion is many times slower and the numbers are meaningless.
- Rust unit tests: `cargo test --lib <module>` (e.g. `cargo test --lib cache::page_index`).
  These are the fast inner loop — prefer them over cluster runs when the logic is unit-testable.

## 2. Launch the local sandbox cluster

Sandbox modules are gated behind `-Dsandbox.enabled=true` (see `sandbox/build.gradle`,
`settings.gradle`). Launch with all analytics plugins installed and the experimental
flags on. `--preserve-data --data-dir` keeps the `clickbench` index across restarts so
you don't reload 100M rows every run.

```bash
./gradlew run -Dsandbox.enabled=true \
  -PinstalledPlugins="['arrow-base','arrow-flight-rpc','analytics-engine','composite-engine','parquet-data-format','analytics-backend-datafusion','analytics-backend-lucene','org.opensearch.plugin:opensearch-job-scheduler:3.8.0.0-SNAPSHOT','org.opensearch.plugin:opensearch-sql-plugin:3.8.0.0-SNAPSHOT']" \
  -Dtests.jvm.argline="-Djava.library.path=$PWD/sandbox/libs/dataformat-native/rust/target/debug -Dopensearch.experimental.feature.pluggable.dataformat.enabled=true -Xms8g -Xmx8g -Ddatafusion.memory_pool_limit_bytes=34359738368 -Ddatafusion.spill_memory_limit_bytes=17179869184 -Dopensearch.experimental.feature.transport.stream.enabled=true -Ddatafusion.reduce.input_mode=streaming" \
  -x javadoc -x test -x missingJavadoc \
  --preserve-data --data-dir /Users/gbh/Documents/data
```

Notes:
- Default REST port is **9200** (this is the `MAIN` node the harness expects).
- `--preserve-data` + `--data-dir`: persists the index. Drop these (or delete the data
  dir) for a clean slate.
- For a **release** run, change `target/debug` → `target/release` and build with `--release`.
- Concurrency / partition knobs for perf runs (set per-index or via `-D...`): match
  `search.concurrent.max_slice_count` and `datafusion.min_target_partitions` to vCPU count,
  `search.concurrent_segment_search.mode: all`. Do NOT change `datafusion.reduce.target_partitions`
  — leave it default so both delegation and non-delegation query paths are exercised.

### A/B (two clusters)
`ab_perf_stats.sh` compares a **baseline** node (`MAIN_NODE`, default `:9200`) against a
**contender** node (`CACHE_NODE`, default `:9302`). Run two clusters — e.g. baseline =
`main` checkout on 9200, contender = this branch on 9302 (separate checkout / data dir /
`httpPort`). Two separate clusters work fine; the harness only needs both URLs reachable.

## 3. The clickbench index

- Index name: `clickbench` (override with `INDEX=...`).
- Composite parquet format (same settings the ITs use):
  `index.pluggable.dataformat.enabled: true`, `index.pluggable.dataformat: composite`,
  `index.composite.primary_data_format: parquet`,
  `index.composite.secondary_data_formats: lucene`.
- Queried via PPL at `POST /_plugins/_ppl` with body `{"query": "source=clickbench | ..."}`.
- If the index isn't present yet, create it with the composite settings above and bulk-load
  the ClickBench `hits` dataset. With `--preserve-data` it survives restarts, so this is a
  one-time cost per data dir.

## 4. A/B perf + correctness: `ab_perf_stats.sh`

Lives at repo root. Runs ClickBench PPL queries against MAIN and CACHE, per query:
`clear cache → stats snapshot → cold run → snapshot → warm run → snapshot`, capturing
per-query latency + stats deltas and an order-independent correctness diff.

```bash
# default: arms "match_nonsel plain", both nodes
./ab_perf_stats.sh

# pick the arm and a subset of queries
ARMS="match_nonsel" ONLY="q1 q8 q20" ./ab_perf_stats.sh
ARMS="plain" SKIP="q18 q23" ./ab_perf_stats.sh
```

Env overrides: `MAIN_NODE` (`:9200`), `CACHE_NODE` (`:9302`), `INDEX` (`clickbench`),
`OUTDIR` (`./ab-perf-stats-results`), `ARMS`, `ONLY`, `SKIP`, `TIMEOUT` (300s).

### Arms (three query families over the same 43 ClickBench queries)
- **`plain`** — vanilla ClickBench (no `match()`); pure listing/parquet scan path.
- **`match_nonsel`** — adds a non-selective `match(URL, 'http')`; routes through the
  Lucene-indexed executor (delegation path) while still scanning most rows.
- **`match_sel`** — adds a selective `match(URL, 'yandex')`; indexed path, few rows.
Run multiple arms to cover both the listing and indexed (delegation) code paths.

### Outputs (in `OUTDIR`)
- `ab_stats_results.json` — per-query latency + stats deltas (both nodes, cold+warm).
- `ab_correctness_data.json` — raw result rows for diffing.
- `ab_stats_report.html` — rendered report.
- `console.txt` — run log.

## 5. Inspect / diagnose results

Helper scripts at repo root consume `OUTDIR/ab_correctness_data.json` (or
`ab_stats_results.json`):

```bash
python3 check_correctness.py ab-perf-stats-results/ab_correctness_data.json  # known-failing qs diff
python3 gen_report.py        ab-perf-stats-results/ab_stats_results.json      # per-query stats table
python3 check_q23.py         ab-perf-stats-results/ab_stats_results.json      # single-query drilldown
```

`check_correctness.py` does order-independent set comparison and flags tie-breaking
(same sort keys, different rows among ties) vs real row-set divergence — useful because
ClickBench `sort | head` queries legitimately differ only in tie order.

Stats keys worth watching (from `gen_report.py`): `rg_processed`/`rg_skipped` (row-group
pruning), `parquet_bytes_scanned`, `parquet_scan_*_time_ms`, `prefetch_wait_*`,
`object_store_read_time_ms`, and the cache groups `metadata_cache`, `statistics_cache`,
`column_index_cache`, `offset_index_cache` (hit/miss/entries/bytes). Per-node DataFusion
stats come from `GET /_plugins/_analytics_backend_datafusion/stats`; per-index cache clear
is `POST /<index>/_cache/clear`.

## Typical loop
1. Edit Rust → `cd sandbox/libs/dataformat-native/rust && cargo build` (or `cargo test --lib ...`).
2. Restart the cluster (re-run the `./gradlew run` command; `--preserve-data` keeps the index).
3. `ARMS="..." ./ab_perf_stats.sh` → inspect `ab_stats_report.html` / `check_correctness.py`.
4. For perf numbers: `cargo build --release`, point `java.library.path` at `target/release`,
   and run all 43 queries per arm (don't subset) so both query paths are covered.
