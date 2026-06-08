# Multi-RG decode — benchmark report

Date: 2026-06-11. Branch: `switch-indexed`. Author: paired session.

This documents **every** benchmark run while validating the consolidated
multi-RG indexed scan (`indexed_table/decoder_stream.rs`, gated by the
`OPENSEARCH_INDEXED_MULTI_RG_DECODE` env var / `indexed_multi_rg_decode` config).
It records the setup, the raw numbers, the correctness checks, the measurement
pitfalls we hit, and the honest conclusions — including the cases where the
earlier (retracted) numbers were wrong and why.

> TL;DR: across 37 queries (26 ClickBench + 11 match/AND), enabling multi-RG is
> **net-neutral on total wall time (~1.00–1.01×)**. It is a *targeted* win
> (1.2–1.8×) on selective / setup-bound / match+residual queries, **flat** on
> heavy aggregation/distinct and broad-match queries (which dominate total
> runtime), and has **one real regression**: ClickBench q13 (string group-by) at
> 0.80×. All result sets are byte-/row-count-identical between the two paths.

---

## 0. Headline comparison — indexed single-RG vs indexed multi-RG (all 37 cases)

**Both columns go through the indexed path** (`QueryShardExec`/`IndexedExec`).
This is *not* a vanilla-vs-indexed comparison — it is purely single-RG (flag OFF)
vs multi-RG (flag ON) decode *within* the indexed path. For the pure-parquet
ClickBench queries this required `route_pure_parquet_through_indexed=true` on both
runs (set in the harness); routing was **verified** by confirming
`QueryShardExec` appears in the `QUERY_PROFILE` plan for sample queries
(`stats count()`, `where AdvEngineID!=0 …`, `SearchPhrase!='' group by …`).
For vanilla-DataFusion-vs-indexed numbers, see §8 instead.

### Pure-parquet ClickBench — indexed single-RG vs indexed multi-RG

| query | single-RG (ms) | multi-RG (ms) | speedup | bucket |
|---|---:|---:|---:|---|
| q1  | 17 | 15 | 1.13× | faster |
| q2  | 244 | 234 | 1.04× | ~same |
| q3  | 859 | 854 | 1.01× | ~same |
| q4  | 715 | 667 | 1.07× | ~same |
| q5  | 1051 | 846 | 1.24× | faster |
| q6  | 2061 | 1920 | 1.07× | ~same |
| q7  | 31 | 41 | 0.76× | SLOWER (tiny/noise) |
| q8  | 311 | 172 | 1.81× | faster |
| q9  | 3124 | 3264 | 0.96× | ~same |
| q10 | 4699 | 5177 | 0.91× | ~same |
| q11 | 1608 | 1186 | 1.36× | faster |
| q12 | 2084 | 1369 | 1.52× | faster |
| q13 | 3833 | 4765 | 0.80× | **SLOWER (real)** |
| q15 | 3878 | 3808 | 1.02× | ~same |
| q16 | 3965 | 4381 | 0.91× | ~same |
| q17 | 9716 | 9799 | 0.99× | ~same |
| q18 | 9617 | 10133 | 0.95× | ~same |
| q19 | 12466 | 12133 | 1.03× | ~same |
| q20 | 449 | 355 | 1.26× | faster |
| q37 | 201 | 202 | 1.00× | ~same |
| q38 | 133 | 119 | 1.12× | faster |
| q39 | 2253 | 2185 | 1.03× | ~same |
| q40 | 2600 | 2559 | 1.02× | ~same |
| q41 | 133 | 75 | 1.77× | faster |
| q42 | 136 | 75 | 1.81× | faster |
| q43 | 117 | 89 | 1.31× | faster |
| **TOTAL** | **66,301** | **66,423** | **1.00×** | |

### Delegation (match) + AND — indexed single-RG vs indexed multi-RG

| query | single-RG (ms) | multi-RG (ms) | speedup | bucket |
|---|---:|---:|---:|---|
| match(SearchPhrase,"google") ~2k | 92 | 90 | 1.02× | ~same |
| match(Title,"test") ~5k | 83 | 71 | 1.17× | faster |
| match(URL,"yandex") ~13k | 113 | 107 | 1.06× | ~same |
| match(Title,"google") ~36k | 121 | 95 | 1.27× | faster |
| match(Referer,"http") ~66M | 1255 | 1313 | 0.96× | ~same |
| match(Title) AND match(URL) [n=0] | 41 | 48 | 0.85× | SLOWER (sub-50ms noise) |
| match(SearchPhrase) AND match(Title) [n=1] | 44 | 52 | 0.85× | SLOWER (sub-50ms noise) |
| match(Title) AND AdvEngineID≠0 (residual) | 161 | 112 | 1.44× | faster |
| match(URL) AND CounterID=62 (residual) | 34 | 45 | 0.76× | SLOWER (sub-50ms noise) |
| match(Title) group by SearchPhrase | 686 | 658 | 1.04× | ~same |
| match(Title) AND CounterID=62 group by URL | 54 | 55 | 0.98× | ~same |
| **TOTAL** | **2,684** | **2,646** | **1.01×** | |

**Grand total (all 37): single-RG 68,985 ms vs multi-RG 69,069 ms = 1.00×.**
Buckets (>1.1× faster / 0.9–1.1× same / <0.9× slower): **12 faster, 20 same,
5 slower** — of the 5 "slower", 3 are sub-50 ms match queries below the noise
floor, q7 is a 31 ms query (noise), and **q13 is the only substantive regression
(~930 ms)**. Detailed per-set tables, fingerprints, and notes in §5–§6.

---

## 1. What "multi-RG" changes (and what it does NOT)

The per-RG path (`stream.rs`, flag OFF) and the multi-RG path (`decoder_stream.rs`,
flag ON) are **identical** except for how each row group's parquet bytes decode:

| | per-RG (OFF) | multi-RG (ON) |
|---|---|---|
| Per RG it builds… | a fresh DataFusion `DataSourceExec` + `TaskContext` + `ParquetSource` + opener | a bare `ParquetPushDecoder` from pre-derived metadata |
| `ArrowReaderMetadata` (parquet→arrow `FieldLevels`) | re-derived inside DF's opener **per RG** | derived **once**, `Arc`-reused |
| Predicate (RowFilter) pushdown | yes (via `ParquetSource.with_predicate`, row-granular only) | **no** (dropped) |
| Parquet I/O metrics (`parquet_*`) | inherited from `DataSourceExec` | not emitted (structural N/A) |

**Prefetch / Lucene / refinement are shared and identical**: both prefetch RG
n+1's Lucene one RG ahead overlapping RG n's decode; both call the same
`prefetch_rg` and `build_rg_plan`. A parquet-decoder change therefore **cannot**
change Lucene eval time — a fact we used as a sanity check (see §6).

The intended win is purely: amortize the per-RG `DataSourceExec`/opener/
`ArrowReaderMetadata` reconstruction. Measured at `parquet_first_poll_time`
≈ **8–9 ms / RG-batch group** on a 110-RG segment (see §5), which is significant
only when it's a large fraction of the query — i.e. selective / low-work queries.

---

## 2. Environment & methodology

- **Node**: standalone archive at `/Users/gbh/Documents/data-fol/3.7.0-ARCHIVE`,
  `./bin/opensearch`, loading the Rust dylib via `-Djava.library.path=.../
  dataformat-native/rust/target/release`.
- **Data**: `clickbench` index, ~99,997,497 docs (~23 GB), single shard.
- **Build**: `cargo build --release` (LTO; the dylib used for all timing runs was
  built 2026-06-11 with all three correctness fixes from §4). **Debug builds were
  only ever used for correctness checks — never for timing** (debug is 10–50×
  slower at runtime and its numbers are meaningless).
- **A/B procedure**: stop node → start WITHOUT env var (per-RG baseline) → run →
  stop → start WITH `OPENSEARCH_INDEXED_MULTI_RG_DECODE=1` (multi-RG) → run. Both
  on the **same** release dylib, so it is a pure flag A/B. Flag presence verified
  each launch via `ps eww <pid> | grep MULTI_RG`.
- **Required cluster settings (both runs)**:
  `search.concurrent_segment_search.mode=none`,
  `datafusion.min_target_partitions=1`,
  `datafusion.indexed.route_pure_parquet_through_indexed=true`
  (the last is needed so pure-parquet queries route through the indexed path at
  all; delegation/`match()` queries always do).
- **Timing**: per query, clear the OS/parquet cache, run N iterations, drop the
  first 2 (cold/JIT warmup), report the **median** of the rest. `curl
  -w %{time_total}` end-to-end wall time (includes gRPC/Flight transport + JVM +
  scan — NOT just the Rust scan).
- **Correctness**: each query's result is fingerprinted (row count `n=`, and a
  stable count/first-row value `c=`) and compared OFF vs ON.

**Trust caveats (important):**
- ClickBench sweep used **N=6** (4 effective samples). Small queries (q1, q7 at
  15–40 ms) have a large relative variance at this N — treat their deltas as noise
  unless > ~2×. Big queries are more stable.
- Single absolute times below ~50 ms are at/below the run-to-run noise floor
  (±10–30 ms observed). Do not read sub-50ms "regressions" as real.

---

## 3. Query sets

### 3.1 ClickBench subset (26 queries)
Canonical ClickBench PPL from
`sandbox/plugins/analytics-engine/src/test/resources/clickbench/queries/q*.ppl`,
with `source=hits` rewritten to `source=clickbench`. Selection per request:
**first 20 (q1–q20) minus q14, plus last 7 (q37–q43)** = q1–q13, q15–q20, q37–q43.

### 3.2 match / AND supplementary set (11 queries)
Hand-built to exercise the **delegation path** (`match()` → Lucene FFM collector)
and AND combinations, across a selectivity spread. Fields probed for selectivity:
`SearchPhrase=google`≈1.9k, `Title=test`≈5.5k, `URL=yandex`≈13k,
`Title=google`≈36k, `Referer=http`≈65.9M docs.

---

## 4. Correctness bugs found (and fixed) during benchmarking

The benchmark sweep is what *surfaced* these — the pre-existing e2e tests all
used a thread-safe **mock** collector and same-order/in-memory parquet, so none
of these reproduced in unit tests. All three are fixed in `decoder_stream.rs`;
all 37 queries' results are byte-/row-identical OFF vs ON after the fixes.

1. **Non-reentrant FFM collector called concurrently.** `advance_to_next_rg`
   armed RG n+1's prefetch *before* awaiting RG n's, so at stream start two
   `collect_packed_u64_bitset` calls ran on the same non-reentrant Lucene handle
   → buffer corruption. Live symptom (any `match()` query, flag ON): Java
   `ArrayIndexOutOfBoundsException: Index 63 out of bounds for length 32`,
   `numBytes must be >= 0, got -257`, surfacing as `collectDocs(...) failed: -1`.
   Fix: await-then-arm. Regression test:
   `streaming_at_scale.rs::multi_rg_never_collects_concurrently`.
2. **Projection index space.** `ProjectionMask::roots` was fed table-schema
   indices instead of physical parquet root indices → wrong columns decoded when
   the file's physical order differs from the table schema AND the query projects
   a subset. Fix: translate table→physical by field name. Tests:
   `schema_drift.rs::diff_reordered_subset_projection_*`.
3. **`Panic: byte view array`.** Multi-RG decoded parquet's default arrow types
   (`Utf8View` for strings) while the output schema declared `Utf8`; any
   `GROUP BY <string col>` panicked. Live symptom: q13/q15/q17/q18/q38 and
   `... group by SearchPhrase` failed with flag ON. Fix: `build_supplied_schema`
   coerces the decoder's output schema to the table types (mirrors what
   DataFusion's opener does on the per-RG path). Note the unit test
   (`schema_drift.rs::multi_rg_group_by_string_column_does_not_panic`) does NOT
   reproduce it — small in-memory parquet infers `Utf8`, not `Utf8View` — so this
   was only confirmed on the live node.

---

## 5. ClickBench sweep — full results (release, median of N=6 drop-first-2)

Both paths returned identical row counts (`n=`) for every query.

| query | OFF (ms) | ON (ms) | speedup | note |
|------:|---------:|--------:|--------:|------|
| q1  (count *)                              |    17 |    15 | 1.13× | tiny, noise |
| q2  (count where AdvEngineID≠0)            |   244 |   234 | 1.04× | |
| q3  (sum/count/avg)                        |   859 |   854 | 1.01× | |
| q4  (avg UserID)                           |   715 |   667 | 1.07× | |
| q5  (count distinct UserID)                |  1051 |   846 | 1.24× ↑ | |
| q6  (count distinct SearchPhrase)          |  2061 |  1920 | 1.07× | |
| q7  (min/max EventDate)                    |    31 |    41 | 0.76× ↓ | tiny, noise |
| q8  (AdvEngineID≠0 group by, sort)         |   311 |   172 | **1.81×** ↑ | original target |
| q9  (RegionID, dc(UserID))                 |  3124 |  3264 | 0.96× | |
| q10 (RegionID, multi-agg + dc)             |  4699 |  5177 | 0.91× | |
| q11 (MobilePhoneModel, dc(UserID))         |  1608 |  1186 | **1.36×** ↑ | |
| q12 (MobilePhone+Model, dc(UserID))        |  2084 |  1369 | **1.52×** ↑ | |
| q13 (SearchPhrase≠'' group by SearchPhrase)|  3833 |  4765 | **0.80× ↓** | **real regression** |
| q15 (SearchEngineID+SearchPhrase group by) |  3878 |  3808 | 1.02× | |
| q16 (UserID group by)                      |  3965 |  4381 | 0.91× | |
| q17 (UserID+SearchPhrase group by)         |  9716 |  9799 | 0.99× | heavy |
| q18 (UserID+SearchPhrase group by, no sort)|  9617 | 10133 | 0.95× | heavy |
| q19 (UserID+minute+SearchPhrase group by)  | 12466 | 12133 | 1.03× | heaviest |
| q20 (UserID = literal)                      |   449 |   355 | 1.26× ↑ | |
| q37 (URL PageViews, date range)            |   201 |   202 | 1.00× | |
| q38 (Title PageViews, date range)          |   133 |   119 | 1.12× ↑ | |
| q39 (URL PageViews, IsLink, offset)        |  2253 |  2185 | 1.03× | |
| q40 (TraficSource… CASE group by)          |  2600 |  2559 | 1.02× | |
| q41 (URLHash+EventDate, RefererHash=)      |   133 |    75 | **1.77×** ↑ | very selective |
| q42 (WindowClientW/H, URLHash=)            |   136 |    75 | **1.81×** ↑ | very selective |
| q43 (span(EventTime,1m), date range)       |   117 |    89 | 1.31× ↑ | |
| **SUM** | **66,301** | **66,423** | **1.00×** | |

Counts by bucket: **10 faster (>1.1×), 14 ~same (0.9–1.1×), 2 slower (<0.9×)**.

Wins cluster on selective / setup-bound queries (q8, q41, q42, q12, q11, q43,
q20, q5). Flat on heavy aggregation/distinct (q17/q18/q19, q9/q10/q16). The two
"slower" are q7 (tiny, noise) and **q13 (real, ~930 ms / 0.80×)** — a string
group-by; suspected to be the `Utf8` coercion (fix #3) forcing a non-view string
path that is slower than the per-RG `Utf8View` for high-cardinality group-by.
**q13 is the one finding that warrants follow-up** (rerun at higher N + profile).

---

## 6. match / AND (delegation) sweep — full results (release, N=6)

All 11 correctness fingerprints matched OFF vs ON (incl. the empty
`Title AND URL` = 0 rows, and the string group-bys).

| query | OFF (ms) | ON (ms) | speedup | note |
|------|---------:|--------:|--------:|------|
| match(SearchPhrase,"google") ~2k          |   92 |   90 | 1.02× | |
| match(Title,"test") ~5k                    |   83 |   71 | 1.17× ↑ | |
| match(URL,"yandex") ~13k                   |  113 |  107 | 1.06× | |
| match(Title,"google") ~36k                 |  121 |   95 | **1.27×** ↑ | |
| match(Referer,"http") ~66M (broad)         | 1255 | 1313 | 0.96× | Lucene-bound |
| match(Title) AND match(URL)  [n=0]         |   41 |   48 | 0.85× ↓ | sub-50ms noise |
| match(SearchPhrase) AND match(Title) [n=1] |   44 |   52 | 0.85× ↓ | sub-50ms noise |
| match(Title) AND AdvEngineID≠0 (residual)  |  161 |  112 | **1.44×** ↑ | biggest deleg win |
| match(URL) AND CounterID=62 (residual)     |   34 |   45 | 0.76× ↓ | sub-50ms noise |
| match(Title) group by SearchPhrase         |  686 |  658 | 1.04× | |
| match(Title) AND CounterID=62 group by URL |   54 |   55 | 0.98× | |
| **SUM** | **2,684** | **2,646** | **1.01×** | |

Pattern (consistent with §5):
- Single `match()` 5k–36k selectivity → mild win **1.17–1.27×**.
- `match() AND numeric residual` → **1.44×** (best delegation case: many RGs,
  per-RG refinement, setup amortization helps most).
- Broad `match(Referer,"http")` (66M) → flat 0.96× (Lucene collect dominates).
- The three "↓" are all 34–52 ms queries returning 0–2 rows — **below the noise
  floor**, not real regressions (the same query was observed to swing ±30 ms
  run-to-run; see §7).

---

## 7. Per-operator profiling (and a retracted measurement)

We dumped the `QUERY_PROFILE` per-operator metrics (logged unconditionally on
stream close via `RustLoggerBridge`, grep `QUERY_PROFILE` in
`3.7.0-ARCHIVE/logs/opensearch.log`) for the selective `match(Title,"google")`
(36k matches, 110 RGs) query.

**Run-to-run instability — the key cautionary finding.** Three runs of the *same
query on the same (per-RG) path*:

| metric | run1 | run2 | run3 | stable? |
|---|---|---|---|---|
| `index_query_time` (Lucene eval) | 31.55 ms | 14.70 ms | 13.49 ms | **NO (2.3× swing)** |
| `prefetch_wait_time` | 11.16 ms | 3.07 ms | 2.92 ms | no |
| `parquet_first_poll_time` (per-RG setup) | 9.27 ms | 8.78 ms | 9.43 ms | **YES (±0.6 ms)** |
| `elapsed_compute` (scan CPU) | 28.76 | 24.40 | 27.03 ms | roughly |

Consequence: **`index_query_time`, `prefetch_wait_time` and total wall time at
small sizes are noise-dominated and cannot reliably distinguish the two paths.**
Lucene eval time *especially* cannot differ between paths (both call the identical
`prefetch_rg`) — a 31-vs-11 ms reading was cold-vs-warm, not a path effect.

> Retraction: an earlier write-up in this session claimed multi-RG showed
> "Lucene 31 ms → 11 ms" and "parquet_first_poll 10 ms → 1 ns (eliminated)". That
> was **wrong on two counts**: (a) the Lucene delta was cold/warm noise, and (b)
> the multi-RG path does not *populate* `parquet_first_poll_time` /
> `parquet_poll_time` at all, so the "1 ns" was an unwritten-metric placeholder,
> not an eliminated cost. The only honest cross-path signal from the profile is
> that `parquet_first_poll_time` is a **stable ~9 ms per-RG setup cost on the
> per-RG path** that the multi-RG path does not incur by construction.

**Profile decomposition (selective query, per-RG, representative warm run,
`elapsed_compute` ≈ 26 ms):** `parquet_poll_time` ≈ 18 ms (of which
`parquet_first_poll_time` setup ≈ 8 ms), `index_query_time` ≈ 13 ms,
`parquet_read_time` (I/O) ≈ 3 ms, `prefetch_wait_time` ≈ 3 ms, masking/coalesce
< 0.2 ms. → The win lever (setup) is real but is ~8 ms; whether it matters depends
entirely on query size.

**Broad query (`match(Referer,"http")`, 66M), per-RG, `elapsed_compute` ≈ 701 ms:**
`index_query_time` (Lucene) ≈ 666 ms dominates; `filter_record_batch` ≈ 78 ms,
`build_mask` ≈ 55 ms, `parquet_poll` ≈ 66 ms, `parquet_first_poll` (setup) ≈ 5 ms
(< 1%). → Setup amortization is irrelevant here; the lever would be the Lucene
eval or the arrow mask/filter kernels.

---

## 8. Pure-parquet vs VANILLA DataFusion (context for q-routing decisions)

A separate q08 measurement comparing the indexed path to vanilla DataFusion
(route flag OFF → q08 bypasses the indexed path entirely):

| q08 path | median | vs vanilla |
|---|---|---|
| Vanilla DataFusion (production default for pure-parquet) | ~120 ms | 1.0× |
| Indexed **multi-RG** | ~140 ms | ~1.15× **slower** |
| Indexed **per-RG** | ~298 ms | ~2.5× slower |

Notes:
- Vanilla DF 54.0.0 *also* uses `ParquetPushDecoder` (the workspace
  `[patch.crates-io]` arrow 58.3.0 pin is global; the `into_builder` additions are
  additive, vanilla behavior unchanged). So the push decoder is not a multi-RG
  advantage — vanilla has it too, and amortizes RG setup internally.
- Vanilla wins q08 because it keeps **RowFilter predicate pushdown**
  (skip-during-decode) which the indexed path gives up. `decoder_stream` carries
  no predicate and never pushes one; the per-RG path pushes conditionally
  (row-granular only). This is the likely source of the residual ~15% gap.
- **Conclusion: do NOT route pure-parquet through the indexed path** — vanilla is
  faster. Multi-RG's value is the *delegation* path, which has no vanilla
  alternative (the Lucene collector IS the indexed path).

---

## 9. Conclusions

1. **Net wall-time impact of enabling multi-RG: ~1.00–1.01× (neutral)** across 37
   queries. It is not a blanket speedup.
2. **Targeted win 1.2–1.8×** on selective / setup-bound / `match`+residual
   queries (q8, q41, q42, q12, q11, q43, q20, q5; match+AdvEngineID residual).
   These are the cases where the ~8 ms/RG-group setup tax is a large fraction.
3. **Flat** on heavy aggregation/distinct and broad-match queries — which
   dominate total runtime — so the net comes out even.
4. **One real regression: ClickBench q13** (string group-by) at 0.80× (~930 ms).
   Open item — likely the `Utf8` schema coercion; needs higher-N rerun + profile.
   All sub-50 ms "regressions" in the match sweep are noise, not real.
5. **Correctness: all 37 queries byte-/row-identical OFF vs ON** after fixing the
   three bugs in §4.
6. **Decision input:** enabling multi-RG globally is roughly free on this suite
   and helps a specific selective/delegation class. Ship it only if (a) the
   workload skews toward that class and (b) q13's regression is understood/closed.
   Keep pure-parquet on vanilla regardless (§8).

---

## 10. Reproduce

Scripts used (transient, under `/tmp`):
- `/tmp/clickbench_ab.sh N LABEL` — reads `.ppl` files, swaps `source=hits`→
  `source=clickbench`, runs the 26-query subset, prints `med=` + fingerprint.
- `/tmp/match_ab.sh N LABEL` — the 11 match/AND queries, same format.
- `/tmp/q08_vanilla.sh` — q08 with route flag OFF (vanilla path).

Procedure: build `--release`; for each of {OFF, ON}: stop node, start
(`OPENSEARCH_INDEXED_MULTI_RG_DECODE=1` for ON), wait for clickbench shard
`yellow`, run the script, capture output. Diff fingerprints OFF vs ON for
correctness; compare `med=` for perf.

> All timings here are end-to-end PPL latency (curl `time_total`) on a single
> local archive node and include JVM + gRPC/Flight transport overhead, not just
> the Rust scan. They are directional, not production SLAs. N=6 (4 effective
> samples); rerun at higher N before treating any single < ~1.2× delta as signal.
