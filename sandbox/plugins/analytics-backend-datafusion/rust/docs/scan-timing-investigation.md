# Indexed-scan timing investigation — handoff

> Goal: account for **100%** of a PPL query's wall-clock on the indexed scan, with
> **measured** numbers, no assumptions. We added diagnostic timers to the Rust
> scan and correlate them against DataFusion's own parquet `FileStream` metrics.
>
> Status: **RESOLVED.** As of the 2026-06-06 23:32 run, the open §7 correlation is
> closed with a clean same-stream, same-run measurement (§6). The waterfall now
> accounts for ~98% of wall. The headline is **NOT** "decode-dominated, read-wait
> negligible" — that was an artifact of a scope/run-mismatch in the prior session.
> The scan is ~55% decode + ~44% **serialized** read-wait (§6, §7).

---

## 0. What the previous session got wrong (read this first)

The earlier handoff stated the query was "decode-dominated (~80%)" with read-wait a
"distant 4th lever (~133ms)". That was **wrong**, caused by the §7 scope trap it
itself warned about but then fell into:

- The `StreamMetrics` `Time`/`Count` fields ARE shared (Arc) across all chunk
  streams *of one `execute(partition)`* — but NOT across partitions or across
  separate queries. The 133ms "cumulative gap" it quoted came from a **different
  run** (likely warm OS page cache, or a sibling partition with few RGs) than the
  16-RG/692ms decode stream it compared against. Comparing them produced the bogus
  3× (431-vs-133) mismatch.
- When measured **on the same stream in the same run** (§6), `inter_poll_gap`,
  `derived_read_wait`, and the directly-timed `object_store` read all agree at
  ~450ms. Read-wait is ~44% of wall, co-equal with decode — not negligible.

Everything in §2–§5 below was re-verified against source/live node on 2026-06-06
and is correct. §6–§9 are rewritten with the resolved numbers.

---

## 1. The query under investigation

```bash
curl -s -X POST "localhost:9200/_plugins/_ppl" -H 'Content-Type: application/json' \
 -d '{"query": "source = clickbench | where match(Title, '\''h'\'') | fields Title, WatchID, ClientIP, RegionID, UserID, CounterID, OS, UserAgent, RefererCategoryID, RefererRegionID, URLCategoryID, URLRegionID, ResolutionWidth, ResolutionHeight, FlashMajor, Age, Sex, Income, Interests"}'
```

- Index: **`clickbench`** (NOT `hits_indexed`), 1 shard, ~100M docs (99,997,497), 46 segments.
- Filter `match(Title,'h')` → lowered to `delegated_predicate(0)`, consumed *inside*
  `QueryShardExec` as the index predicate (no `FilterExec`).
- Implicit `LIMIT 10000` (PPL default) → `GlobalLimitExec`. Returns exactly 10000 rows.
- 19 projected columns, several wide strings (`Title`, `UserAgent`).
- The dominant work-stream matched **43,826** candidate rows across **16 row groups**
  (before the LIMIT cap). End-to-end HTTP `time_total` ≈ 1.2s (debug build).

A second useful query (cheap, 1 column, exercises the aggregation path):
```bash
curl -s -X POST "localhost:9200/_plugins/_ppl" -H 'Content-Type: application/json' \
 -d '{"query": "source = clickbench | where match(Title, '\''http'\'') | stats count() as cnt by RegionID"}'
```

---

## 2. Build & run workflow  (THE fast loop — read this first)

**ALWAYS build the native lib from the WORKSPACE ROOT, never the plugin dir, and
NEVER the upstream datafusion checkout:**

```bash
# Workspace root — this produces target/{debug,release}/libopensearch_native.dylib
cargo build --manifest-path /Users/gbh/Documents/dev/OpenSearch/sandbox/libs/dataformat-native/rust/Cargo.toml
```

- A plain `cargo build` inside `sandbox/plugins/analytics-backend-datafusion/rust`
  does NOT relink the dylib the node loads. Use the workspace `--manifest-path`.
- `~/Documents/dev/datafusion` is a **read-only reference checkout**. Building it does
  nothing for us. (Verified: the build links `datafusion-datasource 53.1.0` from the
  crates.io registry, not that checkout — see `Cargo.lock`.)

**Debug vs release:** the archive node's `config/jvm.options` line 26 was pointed at the
**debug** dir for a fast loop (~15s incremental builds vs ~10min release):
```
-Djava.library.path=/Users/gbh/Documents/dev/OpenSearch/sandbox/libs/dataformat-native/rust/target/debug
```
(Numbers in this doc are from the **debug** build — slower than release, but the
*proportions* are what matter. Switch back to `/release` for real perf work.)

**Standalone node** (has clickbench data baked in; loads the dylib via the path above):
```bash
# stop
kill $(pgrep -f "Dopensearch" | head -1)
# start (detaches; logs to logs/opensearch.log)
cd /Users/gbh/Documents/data-fol/3.7.0-ARCHIVE && ./bin/opensearch > /tmp/os-archive.log 2>&1 &
# wait for shard recovery (red -> yellow); query only works at yellow+
curl -s -m 3 "localhost:9200/_cat/indices/clickbench?h=health"
```

**Verify the loaded dylib has your change before trusting a run:**
```bash
# 1. dylib contains your log string:
strings .../target/debug/libopensearch_native.dylib | grep <your-log-string>
# 2. node started AFTER the dylib was built:
ls -lt .../target/debug/libopensearch_native.dylib   # mtime
ps -o lstart= -p $(pgrep -f Dopensearch | head -1)   # node start time
```

**Full loop after a Rust edit:** workspace build → kill node → start node → wait
yellow → run query.

### Gotchas that cost hours
1. **Logging:** Rust→Java logs ONLY appear via `native_bridge_common::log_info!`.
   Plain `log::info!`/`log::warn!` (std `log` facade) is **silently dropped**. Logs
   land in `/Users/gbh/Documents/data-fol/3.7.0-ARCHIVE/logs/opensearch.log` tagged
   `o.o.n.s.RustLoggerBridge`.
2. **Drop-timing:** our per-query dump runs in `IndexedStream::Drop` (and at EOF in
   `poll_inner`, dedup'd by a one-shot guard). Drop fires when the query handle
   closes — often only when the NEXT query runs. So run the target query, then a
   second throwaway query (e.g. `match(Title,'zzz')`), then read the log.
3. **One dump line PER stream.** `dump_metrics_once` runs in every chunk stream's
   Drop. To find the heavy work-stream, grep for the rollup with the most RGs:
   `grep 'inner-parquet-rollup' opensearch.log | grep 'rgs=16'`. Its consecutive
   `gap-distribution` / `read-io-vs-gap` / `indexed-metrics-cumulative` lines (same
   millisecond timestamp) belong to THAT stream — that's the apples-to-apples set.
4. **Permission prompts:** the harness matches the command PREFIX. `for …; do curl;
   done` and `VAR=x; cmd` do NOT match `Bash(curl*)` allowlist entries → they
   prompt. Use single, simple commands (no loops, no leading `VAR=`).

---

## 3. Architecture (why timing is split across layers)

```
Java REST/PPL ── df_execute_query (FFM, block_on on IO runtime) ── plan setup on CPU runtime
                                                                         │
   physical plan:  ProjectionExec → GlobalLimitExec(10000) → CooperativeExec → QueryShardExec(46 segs)
                                                                         │
   QueryShardExec::execute  (partitions=1)
     └─ builds chunk streams (IndexedStream), each driven by ONE poll loop
         └─ per row group: index eval (Lucene/FFM) via spawn_blocking PREFETCH,
            then a fresh single-RG DataSourceExec (parquet) → current_stream
   stream wrapped in CrossRtStream: produced on CPU runtime, consumed on IO runtime
   via a depth-1 mpsc channel; Java pulls each batch through df_stream_next (block_on).
```

Two consequences central to the timing:
1. **One `DataSourceExec` per row group** (`parquet_bridge.rs:182`), all cycled by one
   poll loop. RG-B's parquet stream is only created AFTER RG-A's returns `None`.
2. **Prefetch overlaps the LUCENE/FFM eval, NOT the parquet read.** `IndexReader`
   `spawn_blocking`s the *index* eval of RG+1 while RG decodes. There is **no**
   read-ahead of RG+1's parquet column bytes. So the parquet `get_ranges` await and
   the decode of each RG run **serially** (proven in §7).

`StreamMetrics` is created once per `execute(partition)` (`table_provider.rs:477`)
and `.clone()`d (Arc-share) into each chunk stream of that partition. So its
`Time`/`Count` fields are cumulative **within a partition's execution**, but are
independent across partitions and across queries. Do NOT compare a cumulative line
to a per-stream rollup from a different run — that was the §0 mistake.

---

## 4. Instrumentation added (all diagnostic; on branch `work-stealing-indexed`)

Files touched: `src/api.rs`, `src/cross_rt_stream.rs`, `src/indexed_table/stream.rs`,
`src/indexed_table/metrics.rs`, `src/indexed_table/parquet_bridge.rs`. (Re-verified
present and wired as described on 2026-06-06.)

| Log line (grep tag) | Where | What it measures |
|---|---|---|
| `[stream_next-timing]` | `api.rs` QueryStreamHandle::Drop | per-query `stream_next` total split into `poll` (drive plan) vs `export` (RecordBatch→FFI_ArrowArray) |
| `[cross-rt-producer]` | `cross_rt_stream.rs` driver | `produce` (CPU runtime generating batches) vs `send_blocked` (depth-1 channel backpressure) |
| `[indexed-metrics-cumulative]` | `stream.rs` dump_metrics_once | the waterfall: `elapsed_compute`, `inter_poll_gap`, `parquet_poll`, `coalesce`, `index_query`, `poll_count`, `parquet_pending`, etc. Cumulative across the partition's chunk streams (shared Arc metrics). |
| `[inner-parquet-rgN]` | `stream.rs` dump | EACH row group's full DataFusion parquet `MetricsSet`, one line per RG. Per-stream (`my_inner_plans` Vec). |
| `[inner-parquet-rollup]` | `stream.rs` dump | per-stream sums of the DataFusion FileStream timers (Σprocessing, Σuntil_data, Σscanning_total, Σopening) + derived read-wait |
| `[gap-distribution]` | `stream.rs` dump | per-stream sorted list of individual inter-poll gap samples (ms) — proves uniform vs spike |
| `[read-io-vs-gap]` | `stream.rs` dump | object-store read wall-time (wrapped `get_range`/`get_ranges` in `parquet_bridge.rs` via `ReadIoStats`) vs inter_poll_gap |

New metric fields in `metrics.rs` `StreamMetrics`/`PartitionMetrics`:
`inter_poll_gap` (Time), `poll_count` (Count), `parquet_pending_count` (Count),
`inner_plans` (shared Vec<plan>), `io_stats` (shared `ReadIoStats`).
`ReadIoStats` is a struct in `parquet_bridge.rs` (atomics: total_ns, count, max_ns),
recorded inside `CachedMetadataReader::get_bytes`/`get_byte_ranges` around the actual
`store.get_range(s)` await.

### Scope traps (so the next session doesn't repeat them)
- **Per-stream `Vec` fields (`my_inner_plans`, `gap_samples_ms`) are NOT shared.** The
  dump runs in EVERY stream's Drop → one line per stream. The heavy stream is the one
  with the most RGs. The four dump lines at the **same millisecond timestamp** belong
  to the same stream — use those together.
- **Mixing a per-stream value against a cumulative/other-run value is the #1 mistake**
  — it produced the bogus 3× mismatch the prior session called "unexplained" (§0).

---

## 5. DataFusion parquet FileStream metric semantics (VERIFIED line-by-line, 53.1.0)

Read from the crate the build actually links:
`~/.cargo/registry/.../datafusion-datasource-53.1.0/src/file_stream.rs`.
Verified against the real start/stop call sites (line numbers from that file):

- **`time_elapsed_opening`** — `start()` L114/L201/L231, `stop()` L128. `FileOpener::open()`
  → reader ready. Here ~0.1ms/RG (footer metadata pre-cached) → negligible.
- **`time_elapsed_processing`** — `start()` L266, `stop()` L268, wrapping the WHOLE
  `poll_inner` body each poll. Pure in-poll CPU; it does NOT keep running across a
  `Poll::Pending` return (the park happens between poll calls, outside the span).
  **Maps 1:1 to our `parquet_poll`.** ✓ confirmed §6.
- **`time_elapsed_scanning_until_data`** — `start()` L130 (reader ready), `stop()` L173
  (first batch). ONE span, never restarts. **Keeps running across `Poll::Pending`**, so
  it = read-wait + first-batch decode. `Σuntil_data − Σprocessing` ≈ read-wait. ✓ §6.
- **`time_elapsed_scanning_total`** — `start()` L131, `stop()` L174 at each batch,
  **`start()` again L189** after returning the batch → the ENVELOPE. The crate's own
  doc comment (L381–386): "includes the time of the parent operator's execution." NOT a
  clean decode metric; ≈ whole scan wall.

Decomposition (now measured, not just believed — §6):
```
scanning_total (envelope) ≈ processing (decode) + read_wait + parent_tail
until_data                =  processing (decode) + read_wait
```

---

## 6. RESOLVED: the same-stream, same-run waterfall (2026-06-06 23:32)

The 16-RG work-stream's own four dump lines (identical timestamp 23:32:02.789,
debug build, cold-ish cache):

```
[inner-parquet-rollup] rgs=16 | Σprocessing(decode)=568.1ms Σuntil_data=1004.6ms
                       Σscanning_total(envelope)=1030.5ms Σopening=2.0ms
                       derived_read_wait(until_data-processing)=436.5ms
[gap-distribution]     n=18 total=450.6ms max=47.02ms p50=27.83ms
                       sorted_ms=[0.02, 0.71, 13.53, 18.28, 20.33, 22.81, 25.57, 27.13,
                       27.38, 27.83, 28.00, 28.77, 29.22, 29.81, 30.11, 36.68, 37.36, 47.02]
[read-io-vs-gap]       object_store_reads=16 read_total=450.3ms read_max=47.04ms
                       read_avg=28.142ms | inter_poll_gap=450.6ms | gap_minus_read=0.3ms
[indexed-metrics-cumulative] wall_est=1036.3ms | elapsed_compute=585.7ms
                       inter_poll_gap=450.6ms poll_count=19 parquet_pending=16
                       parquet_poll=568.4ms index_query=7.4ms coalesce=13.8ms
                       prefetch_wait=0.7ms | sum_subparts=582.7ms in_poll_residual=3.0ms
                       rg_processed=16 rg_skipped=0 rows_matched=43826 batches=2
```

### The three identities that close §7 (all SAME stream, SAME run):

1. **Decode identity:** `parquet_poll=568.4ms` ≈ `Σprocessing=568.1ms` → match to **0.05%**.
   Our in-poll parquet timer IS DataFusion's `time_elapsed_processing`. Decode confirmed.
2. **Read-wait identity:** `inter_poll_gap=450.6ms` ≈ `derived_read_wait=436.5ms`
   (≈3% gap, within envelope contamination) AND ≈ directly-timed `object_store
   read_total=450.3ms` (**match to 0.07%**). The parked-between-polls time IS the
   parquet `get_ranges` await.
3. **Envelope:** `wall_est=1036ms` ≈ `Σscanning_total=1030ms` → match to 0.6%.

`gap_minus_read = 0.3ms` → the park is **entirely** the object-store read; runtime/
handoff overhead is nil. `prefetch_wait=0.7ms` → the Lucene eval is fully hidden behind
decode (prefetch overlap works). 18 gap samples, each ~13–47ms, sum to the read total
→ **one park per RG, uniform, no single stall.**

### The actual breakdown of the 1036ms scan wall:
```
decode (parquet_poll / Σprocessing) ........ 568ms   ~55%
read-wait (inter_poll_gap = get_ranges) .... 450ms   ~44%   ← NOT negligible
coalesce ................................... 14ms
index eval (hidden behind decode) .......... ~7ms (prefetch_wait only 0.7ms surfaced)
in_poll_residual + tail .................... ~3ms
```
Accounts for ~98% of wall. `bytes_scanned ≈ 449MB` for 43,826 candidate rows / 19
columns; `page_index` is DISABLED (`parquet_bridge.rs:160` `.with_enable_page_index(false)`
— collector bitset matches aren't visible to parquet predicates).

---

## 7. The key structural finding: read and decode are SERIAL

`parquet_pending=16` (one Pending per RG) + 18 uniform gap samples summing to the full
read total + `prefetch_wait≈0` together prove:

- Each RG's parquet `get_ranges` await parks the poll thread (~28ms avg), THEN that
  RG decodes (in-poll, `parquet_poll`), THEN the next RG's stream is created and its
  read parks again. **No cross-RG overlap of read with decode.**
- So wall ≈ Σ(read_i) + Σ(decode_i) = 450 + 568 ≈ 1018ms ≈ measured 1036ms.
- The existing prefetch machinery overlaps only the *Lucene/FFM index eval* (which is
  cheap, ~7ms), not the parquet column-chunk reads (the expensive ~450ms).

This is the lever the prior session missed: **overlapping RG+1's parquet read with
RG's decode** would hide ~450ms behind the 568ms decode, dropping the scan toward
`max(568, 450) + first_read + tail ≈ ~620ms` — a **~40% wall reduction**, with zero
change to bytes read or rows decoded.

---

## 8. Levers, re-ranked by MEASURED payoff

1. **Read/decode pipelining across RGs (~40% wall).** Prefetch RG+1's parquet byte
   ranges (or run its `DataSourceExec` open+first-read) while RG decodes. Attacks the
   serialized 450ms directly. Cleanest big win; no correctness risk to the result set.
   Possible approaches: keep two single-RG `DataSourceExec`s in flight; or feed all
   selected RGs to ONE multi-RG `DataSourceExec` and let DataFusion's own
   `start_next_file`/`NextOpen` background-open pipeline run (note: that pipelines
   file-opening, and within a file parquet-rs already does some read-ahead — measure
   whether a single multi-RG access plan already overlaps better than our per-RG split).
2. **Late materialization / narrower projection (attacks BOTH decode + read volume).**
   Decode the filter/sort columns first, fetch the other ~17 only for surviving rows.
   Hits the 568ms decode AND the 449MB read. Strong, but more invasive.
3. **Re-enable page-index pruning for parquet-readable columns** so sparse selections
   skip pages instead of decoding/reading whole chunks. Attacks the 449MB. Blocked
   today because collector bitset matches aren't visible to parquet predicates — would
   need the page-boundary `RowSelection` idea sketched in `parquet_bridge.rs`
   `create_full_scan_stream` TODO.
4. **Compact StringView output earlier** (`compact_string_view_columns` exists) —
   attacks materialization size, smaller lever for this query.

Note: the prior doc ranked read-ahead as a "distant 4th". That was based on the bogus
133ms read-wait figure. With read-wait measured at ~450ms (§6), pipelining moves to #1.

---

## 9. Caveats on these numbers

- **Debug build.** Release decode is faster (maybe ~0.7×), which would shift the
  decode:read ratio further TOWARD read-wait being dominant. Re-measure on release
  (point jvm.options at `/release`, ~10min build) before committing to a lever.
- **Local `file://` store, possibly cold cache.** `read_avg=28ms` for ~28MB ranges ≈
  ~1GB/s. On a warm OS page cache an earlier run showed a much smaller gap (the source
  of the old 133ms figure) — read-wait is cache-state-sensitive. The serial-ness
  (§7) is structural and holds regardless; the magnitude of the win from pipelining
  depends on cache state and storage backend. On S3S/remote stores read-wait would
  dominate even harder, making pipelining a bigger win.
- Always re-derive from a single stream's same-timestamp dump block; never mix runs.

---

## 10. Cleanup owed

All §4 instrumentation is uncommitted diagnostic code on `work-stealing-indexed`
(which also carries the unrelated work-stealing Tier-1/Tier-2 feature + its docs).
Before any real change: either commit the timers as a separate `wip: scan timing
instrumentation` commit or revert them, and switch jvm.options back to `/release`.
The `[read-io-vs-gap]` log's "= runtime/handoff if >0" wording is misleading — for the
heavy stream `gap_minus_read≈0`; on light streams the residual it shows is the Lucene
`prefetch_wait`, not runtime overhead. Reword or drop that suffix if the timers stay.
