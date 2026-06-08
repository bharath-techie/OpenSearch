# Indexed-scan latency waterfall — from EXPLAIN-ANALYZE metrics only

> How to reconstruct where a PPL indexed-scan query spends its time using **only
> operator metrics** that survive into production (`EXPLAIN ANALYZE` / the plan
> metrics dump) — **no object-store IO instrumentation**.
>
> The key result: read-wait and decode are each measurable **two independent
> ways from the `QueryShardExec` metrics**, and the two agree. So we never need
> to time `get_ranges` directly — `QueryShardExec`'s own metrics are sufficient.

Companion doc: `scan-timing-investigation.md` (the diagnostic-instrumentation
investigation that proved these identities with a dedicated `ReadIoStats` IO
timer). This doc is the **production-usable** distillation: same conclusions,
derived without the IO wrapper.

---

## 1. The reference query and its metrics

Single-partition (`search.concurrent_segment_search.mode = none` →
`target_partitions = 1`), debug build, clickbench (~100M docs), heavy 16-RG
work-stream:

```
source = clickbench | where match(Title, 'h')
  | fields Title, WatchID, ClientIP, RegionID, UserID, CounterID, OS, UserAgent,
           RefererCategoryID, RefererRegionID, URLCategoryID, URLRegionID,
           ResolutionWidth, ResolutionHeight, FlashMajor, Age, Sex, Income, Interests
```

The plan-metrics dump (trimmed to the numbers used below):

```
ProjectionExec   elapsed_compute=0.008ms  output_rows=10.00K  output_bytes=3.1MB
  GlobalLimitExec  elapsed_compute=3.21ms  output_rows=10.00K  (fetch=10000)
    CooperativeExec  (no metrics)
      QueryShardExec  partitions=1  output_rows=16.38K  elapsed_compute=597.69ms
        ── our timers ──
        inter_poll_gap        = 433.90 ms
        parquet_poll_time     = 577.09 ms
        coalesce_time         =  16.55 ms
        index_query_time      =  20.72 ms
        prefetch_wait_time    =   4.72 ms
        parquet_read_time     =   2.25 ms   (MISNAMED — plan build, not IO; see §5)
        build_mask_time       =   0.20 ms
        filter_record_batch_time = 0.40 ms
        on_batch_mask_time    =   0.006 ms
        projection_fixup_time =   0.043 ms
        metadata_load_time    =   1.27 ms
        poll_count            =  19
        parquet_pending_count =  16
        ── parquet FileStream timers (aggregated across the 16 inner RG scans) ──
        time_elapsed_processing            = 566.25 ms
        time_elapsed_scanning_until_data   = 972.29 ms
        time_elapsed_scanning_total        = 993.03 ms
        time_elapsed_opening               =   2.08 ms
        ── volume ──
        bytes_scanned         = 489.3 M
        rows_matched          =  43.83 K
        row_groups_processed  =  16
        rows_pruned_by_page_index = 16.51 M   (candidate-stage prune count; NOT page index)
```

---

## 2. The waterfall (top of plan → leaf)

`QueryShardExec` has no single "wall" metric; its wall is
`elapsed_compute + inter_poll_gap` (in-poll work + parked-between-polls).

```
PLAN CRITICAL PATH ≈ 1034.8 ms   (single-threaded pull chain; sum of each operator's own cost)
│
├─ ProjectionExec ............ elapsed_compute =   0.008 ms   <0.01%   (19 column renames, all ns)
│
├─ GlobalLimitExec ........... elapsed_compute =   3.21  ms    0.3%    (16.38K → 10.00K slice)
│
├─ CooperativeExec ........... (no metrics)                     0%     (yield points only)
│
└─ QueryShardExec WALL ≈ ... 1031.59 ms                        99.7%   = elapsed_compute + inter_poll_gap
   │
   ├─ IN-POLL .............. elapsed_compute = 597.69 ms        58%
   │     ├─ decode .............. parquet_poll_time   = 577.09 ms   (96.6% of in-poll)
   │     ├─ coalesce ............ coalesce_time       =  16.55 ms
   │     ├─ per-RG plan build ... parquet_read_time   =   2.25 ms   (NOT IO — §5)
   │     ├─ build_mask .......... build_mask_time     =   0.20 ms
   │     ├─ filter_record_batch . filter_rb_time      =   0.40 ms
   │     ├─ on_batch+proj+mask ........................ ≈  0.05 ms
   │     └─ residual (unattributed) ................... ≈  1.20 ms   (0.2% — books closed)
   │
   └─ PARKED ............... inter_poll_gap   = 433.90 ms        42%
         └─ read-wait: 16 parks, one per RG (parquet_pending_count=16, poll_count=19)

  OFF the poll thread (NOT inside elapsed_compute):
    index_query_time = 20.72 ms  — Lucene/FFM eval on the spawn_blocking prefetch thread,
                                   ~96% hidden behind decode. Only prefetch_wait_time = 4.72 ms
                                   surfaced as the poll thread briefly parking on it.
    metadata_load_time = 1.27 ms — parquet footer (cached).
```

Headline: **~58% decode, ~42% read-wait**, everything else <1%.

---

## 3. The crux — read-wait WITHOUT IO metrics

We cannot time the object-store `get_ranges` await in production (that needed a
diagnostic `ReadIoStats` wrapper). We don't have to: the two biggest components
are each derivable **two independent ways from the `QueryShardExec` line**, and
the pairs agree.

| component | source A (our timer) | source B (parquet's own) | agreement |
|---|---|---|---|
| **decode** | `parquet_poll_time` = 577.09 | `time_elapsed_processing` = 566.25 | **~2%** |
| **read-wait** | `inter_poll_gap` = 433.90 | `time_elapsed_scanning_until_data − time_elapsed_processing` = 972.29 − 566.25 = **406.04** | **~6%** |
| envelope (ceiling only) | wall = 597.69 + 433.90 = 1031.59 | `time_elapsed_scanning_total` = 993.03 | ~4% |

Two independently-implemented timers landing in the same band is what makes the
split trustworthy — it is not a definitional tautology. (For reference, the
diagnostic IO timer in the companion doc measured read_total ≈ 446 ms on a
comparable run — same band as both source-A and source-B estimates here.)

---

## 4. Production formula (QueryShardExec metrics only)

```
scan_wall   = elapsed_compute + inter_poll_gap
decode      = parquet_poll_time                       # cross-check: ≈ time_elapsed_processing
read_wait   = inter_poll_gap                          # cross-check: ≈ until_data − processing
in_poll_overhead = elapsed_compute − parquet_poll_time   # = coalesce + masks + plan-build (~20ms)
index_eval  = index_query_time                        # hidden unless prefetch_wait_time is large
```

The two `≈` cross-checks are the self-validation: if `parquet_poll_time` and
`time_elapsed_processing` diverge, or `inter_poll_gap` and
`(until_data − processing)` diverge, the metrics are suspect and you investigate.
When they agree (as here), the waterfall is sound.

Why each cross-check holds (semantics verified against datafusion-datasource
53.1.0 `file_stream.rs`; see companion doc §5):
- `time_elapsed_processing` wraps the inner `poll_next` body → pure in-poll
  decode, excludes the parked read → equals our `parquet_poll_time`.
- `time_elapsed_scanning_until_data` is one span from reader-ready to first
  batch and **keeps running across `Poll::Pending`** → includes the read-wait;
  subtracting the decode (`processing`) leaves the read-wait → equals our
  `inter_poll_gap`.
- `time_elapsed_scanning_total` is the restart-each-batch envelope ("includes the
  parent operator's execution" per the crate doc) → use only as a ceiling, never
  equate 1:1 with read-wait.

---

## 5. Caveats (read before quoting these numbers)

1. **`parquet_read_time` is misnamed.** It times per-RG `DataSourceExec`
   *construction* (the `t_plan` span in `stream.rs`), **not** the read. The real
   read is inside `inter_poll_gap`. A cold reader of the dump will misattribute
   2.25 ms as "the read." Recommend renaming to `parquet_plan_build_time`.
2. **The tree stops at `QueryShardExec`.** REST/PPL parse, JSON-encode of the
   result rows, the FFI boundary, and the CPU↔IO cross-runtime handoff are **not**
   in any operator metric here. End-to-end HTTP for this query was ~1.1–1.2 s
   warm; the REST/PPL/JSON layer (~155 ms in an earlier measurement, by
   HTTP−FFI difference) lives outside DataFusion metrics entirely. A Java-side
   span is required to close that gap.
3. **`rows_pruned_by_page_index = 16.51 M` is NOT page-index work.** Page index is
   OFF on this path (`pages_pruned=0`, `page_index_pages_pruned=0`). That counter
   is the candidate-stage prune (RG rows − matches). The 489 MB `bytes_scanned`
   for 43.83 K matched rows reflects this over-read.
4. **`output_rows = 16.38 K` at the scan vs 10.00 K returned.** The implicit
   `fetch=10000` does NOT stop the scan early — all 16 matching RGs were decoded,
   then `GlobalLimitExec` sliced to 10 K. Early-termination / limit pushdown is a
   separate lever, not part of this waterfall.
5. **Debug build, single run, cold-ish cache.** Absolute ms are not
   release-representative; the *proportions* and the *cross-check identities* are
   what carry over. On a remote store (S3) read-wait would dominate harder, but
   the same QueryShardExec-only formula and cross-checks still apply.
