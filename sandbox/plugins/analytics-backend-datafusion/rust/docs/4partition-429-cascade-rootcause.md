# 4-partition 429 cascade — root cause investigation

> Status: **RESOLVED — it is a bug.** Root cause = **jemalloc page-retention misread
> as live memory pressure by the RSS guard**, compounded by jemalloc `background_thread`
> being disabled so retained pages are never returned to the OS while idle. NOT a leak,
> NOT DataFusion 54. Definitive evidence in §VERDICT below.

## `background_thread:true` — NOT testable on this box (compiled out on Darwin)

Checked the actual jemalloc build header
(`target/.../jemalloc_internal_defs.h`):
```
/* #undef JEMALLOC_BACKGROUND_THREAD */
```
→ `background_thread` is **compiled out** — jemalloc only supports it on Linux, not
macOS/Darwin. Setting `MALLOC_CONF=background_thread:true` here is ignored/errors, so it
cannot be validated on this machine. It IS the right durable fix on the Linux production
nodes (a background thread purges retained pages on a timer regardless of foreground activity
or panic-unwind), but that must be verified on Linux, not here.

### Fix options ranked
1. **Fix the RSS guard (real root-cause fix, platform-independent):** gate on jemalloc
   `allocated` (live) instead of `resident` (RSS incl. retained dead pages), or
   purge-then-recheck. q14 leaves 21 GB *retained* but only 65 MB *live*; gating on
   `allocated` means the guard stops false-rejecting regardless of decay/retention/platform.
   This is the actual bug — the guard reads the wrong number.
2. **`decay_ms=0` (immediate mitigation, works HERE):** proven to collapse 29 GB → 738 MB
   synchronously in `free()`, surviving even q14's panic-abort path. Cost: a synchronous
   purge per free → CPU under allocation churn. Fine for bench/debug; evaluate for prod.
3. **`background_thread:true` (durable, prod-Linux only):** untestable on this Darwin box.

## Decay sweep on q14 — heartbeat-measured, three settings

Same query (q14, `dc(UserID) by SearchPhrase`, 4 partitions), same JEMHEARTBEAT instrument,
measured jemalloc resident AFTER q14 frees, then ~35s idle:

| `*_decay_ms` | peak resident | resident after free | retained held while idle | verdict |
|---|---|---|---|---|
| **30000** | ~24–30 GB | **21.66 GB** | 21.6 GB, flat 40s | ❌ no reclaim |
| **1000**  | ~30 GB    | **21.52 GB** | 21.5 GB, flat 35s | ❌ no reclaim |
| **0**     | ~29 GB    | **0.74 GB**  | 671 MB, flat 35s  | ✅ recovers |

decay=0 trace (the win): allocated/resident ramp to **28.7 GB / 29.4 GB**, then on free →
`allocated=67 MB, resident=738 MB` — an **immediate ~29 GB → 0.74 GB collapse**, holding flat.

**Why 0 works and 1000/30000 don't, even for q14:** q14 fails via the arrow panic
(Mode C) — an abnormal thread *abort*, not a clean operator teardown. `decay_ms>0` is lazy:
it purges during subsequent allocator activity or via a background thread. The panic unwind
frees the memory (`allocated` collapses) but does NOT generate the follow-on activity the
lazy timer needs, so 21+ GB of pages are dumped into the retained pool and sit there. `decay_ms=0`
purges **synchronously inside `free()`**, so it reclaims regardless of clean-teardown vs
panic-unwind. (~671 MB stays retained even at 0 = jemalloc arena/metadata floor, immaterial.)

This ties the modes together: q14's Mode-C panic is precisely what makes its Mode-B retention
*unrecoverable* under timed decay. Only synchronous purge (decay=0) or a background purge
thread (`background_thread:true`) survives the abort path.

## THIRD failure mode — arrow `byte array offset overflow` panic (HARD CRASH)

While re-testing decay behavior, q14 (`dc(UserID) by SearchPhrase` — string group key)
crashed the **entire node** with a Rust panic, not a 429/OOM:

```
thread 'datafusion-cpu' panicked at arrow-array-58.3.0/src/builder/generic_bytes_builder.rs:87:
"byte array offset overflow"
External error: task 675 panicked with message "byte array offset overflow"
```

Cause (arrow-array 58.3.0, `generic_bytes_builder.rs:87`):
```rust
fn next_offset(&self) -> T::Offset {
    T::Offset::from_usize(self.value_builder.len()).expect("byte array offset overflow")
}
```
When `T::Offset = i32` (a `Utf8`/`Binary` builder, i.e. NOT `LargeUtf8`), and the
accumulated string bytes in one builder exceed `i32::MAX` (2 GB), `from_usize` returns
`None` → `.expect()` **panics** → aborts the `datafusion-cpu` thread → **process dies**.

- Triggered by 4-partition q14 building a multi-GB string aggregation (group-by on the
  `SearchPhrase` string column) in a single non-Large bytes builder.
- A **hard crash**, distinct from MODE A (OOM 429) and MODE B (retention 429). Earlier
  q14 "ERR" results were a MIX of these three depending on timing — which is why the
  failures looked inconsistent.
- Not specific to 58.3.0 vs 58.2.0 — it's inherent arrow behavior for >2 GB in a 32-bit
  offset builder — but it IS a node-killing path worth flagging for the DF54 upgrade.
- Fix direction (separate from the jemalloc issue): the aggregation should use a
  `LargeUtf8`/`i64`-offset builder (or chunk output) when group output can exceed 2 GB,
  and/or the panic should be a recoverable `Err` rather than a thread abort.

### "Does 30s decay reclaim while idle?" — MEASURED at FULL q14 scale, definitive: NO.

Heartbeat trace across an actual q14 run (`dc(UserID) by SearchPhrase`, 4 partitions,
`decay_ms=30000`), node survived the failure and then sat fully idle:

```
                       allocated    resident    retained
idle baseline            61 MB       247 MB      186 MB
q14 ramping            3.59 GB      3.78 GB      190 MB
                      18.38 GB     18.96 GB      579 MB
                      21.35 GB     22.03 GB      680 MB
                      21.66 GB     23.66 GB     1998 MB   ← peak live
q14 fails / frees        65 MB     21.66 GB    21597 MB   ← live collapses, 21.6 GB RETAINED
+5s  idle                65 MB     21.66 GB    21597 MB   ← flat
+10s idle                65 MB     21.66 GB    21597 MB
+15s..+40s idle          65 MB     21.65 GB    21588 MB   ← FLAT, ~0 reclaim over 40s
```

This reconciles the whole investigation in one trace:
- **~22 GB IS retained after q14** (OS `top` showed ~29–36 GB incl. unreturned address space) — real, not imagined.
- **`allocated` collapses 21.66 GB → 65 MB on free → NOT a leak**, the memory is freed.
- **`decay_ms=30000` reclaims essentially nothing while idle** — resident flat (21.662→21.653 GB) for the full 40s window.

Earlier I used a smaller `count() by UserID` probe (186 MB retained) only because q14 sometimes
hits the Mode-C arrow panic and kills the node before it can idle; this run caught q14 surviving,
giving the GB-scale confirmation. Same mechanism either way:

**jemalloc decay is activity-driven; with `background_thread` OFF, an idle node never fires
it, so retained pages persist far beyond `decay_ms`.** `decay_ms=0` is the only setting that
recovered (synchronous purge inside `free()`); `background_thread:true` is the durable fix.

Added a heartbeat that logs jemalloc `allocated` (live) vs `resident` (RSS) every 5s
(`runtime_manager.rs`, TEMP), set `decay_ms=30000`, ran one big allocation
(`count() by UserID`), then sat the node fully idle and watched the heartbeat:

```
                          allocated    resident   retained
idle baseline             57 MB        70 MB      13 MB
during query             1370 MB      1563 MB    193 MB
just after free            61 MB       317 MB     256 MB   ← live freed, pages retained
+5s (residual activity)    61 MB       247 MB     186 MB   ← ONE lazy decay step
+10s … +45s (idle)         61 MB       247 MB     186 MB   ← FLAT for 9 heartbeats, no further drop
```

So: live memory dropped to 61 MB immediately, but **186 MB of retained dead pages stayed
held for the entire 40s+ idle window** — the 30s timer produced **zero** reclaim once the
node was idle. The single 317→247 MB step happened only while the query's teardown was still
making allocator calls. This is direct jemalloc evidence (not OS `top`, not confounded by
probe queries) of the exact mechanism that stranded ~22 GB at query scale:

**jemalloc decay is activity-driven; with `background_thread` OFF, an idle node never fires
it, so retained pages persist far beyond `decay_ms`.** `decay_ms=0` is the only setting that
recovered (synchronous purge inside `free()`); `background_thread:true` is the durable fix.

## FIX VERIFIED — jemalloc decay (no rebuild)

Set the existing cluster settings (wired to jemalloc `arena.*.{dirty,muzzy}_decay_ms` via FFI):
```
PUT _cluster/settings {"persistent":{"native.jemalloc.dirty_decay_ms":0,"native.jemalloc.muzzy_decay_ms":0}}
```
`decay_ms=0` → jemalloc returns freed pages to the OS immediately instead of retaining them.

**Result — the exact q14→q20 sequence that previously cascaded (q14 fail → q15-q43 all 429):**

| query | before fix | after fix (decay=0) |
|---|---|---|
| q14 | ERR (Mode A, real 21 GB) | ERR (Mode A — still genuinely too big; correct) |
| q15 | 429 | **OK 10r** |
| q16 | 429 | **OK 10r** |
| q17 | 429 | **OK 10r** |
| q18 | 429 | **OK 16384r** |
| q19 | 429 | **OK 10r** |
| q20 | 429 | **OK 4r** |
| process RSS (top) | pinned 29–36 GB | **flat ~9.6 GB throughout** |

MODE B (retention-driven false reject) is **eliminated**: freed pages return immediately,
jemalloc `resident` tracks live usage, the RSS guard stops misfiring, RSS no longer wedges.
MODE A (q14 genuinely needs >pool) still fails — correct behavior, not the bug.

### Recommended durable fixes (in priority)
1. **`background_thread:true`** in `MALLOC_CONF` (`.cargo/config.toml`) — robust default so
   pages are reclaimed on a timer without relying on `decay_ms=0` (which forces a synchronous
   purge on every free and can cost CPU under churn). Needs a rebuild.
2. Tune `dirty_decay_ms`/`muzzy_decay_ms` to a small value (e.g. 1000 ms) rather than 0 to
   balance reclaim vs purge overhead — runtime, no rebuild.
3. **Fix the guard logic** regardless: gate on jemalloc `allocated` (live), or
   purge-then-recheck, or skip the reject when `resident − allocated` (retained) is large —
   so a future retention spike can't false-reject even if decay lags.

## VERDICT v2 — both failure modes measured side-by-side (24 GiB pool run)

Re-ran with `datafusion.memory_pool_limit_bytes = 24 GiB` and the jemalloc
allocated-vs-resident instrumentation. Captured BOTH distinct failure modes on the
SAME query (q14 = `dc(UserID) by SearchPhrase`, 4 partitions):

| metric (at reject)        | q14 run #1 (fresh pool) | q14 run #2 (after retention) |
|---------------------------|-------------------------|------------------------------|
| jemalloc **allocated** (live) | **21.3 GB**             | **0.76 GB**                  |
| jemalloc **resident** (RSS)   | 21.9 GB                 | 22.6 GB                      |
| **pool_used**                 | 21.0 GB                 | **0 B**                      |
| spill threshold (0.85×24GiB)  | 20.4 GB                 | 20.4 GB                      |
| outcome                       | genuine live OOM        | **false reject**             |

Two genuinely different things were being conflated:

1. **MODE A — real capacity exhaustion (working as designed).** The first heavy query
   genuinely holds ~21 GB live (`allocated≈pool_used≈resident≈21 GB`): 4 concurrent
   `approx_distinct(UserID)` hash tables. A 24 GiB pool legitimately can't fit it →
   query fails. This is a real limit, not a bug. (Spill exists but `approx_distinct`/this
   shape doesn't shed enough.)

2. **MODE B — retention-driven FALSE reject (the bug).** *After* a heavy query frees,
   jemalloc reports **allocated = 0.76 GB (live) but resident = 22.6 GB (RSS)** and
   **pool_used = 0**. The RSS guard keys on `resident` (22.6 GB > 20.4 GB threshold) and
   rejects the next query — even though ~23 GB of the pool is genuinely free and only
   0.76 GB is live. The 22 GB is freed-but-unreturned jemalloc pages.

**Root cause (definitive): the RSS guard compares jemalloc `resident` against a fraction
of the pool limit, but `resident` includes retained/dead pages. It should use `allocated`
(live) — or purge-then-recheck, or skip the reject when `resident − allocated` is large.**
Lowering the pool to 24 GiB makes MODE B *worse* (threshold drops to 20.4 GB, so retained
pages trip it more easily).

> Note: `top`/OS RSS stayed at ~29 GB even when jemalloc `resident` briefly dipped below
> threshold (a `count()` slipped through once) — OS reclaim lags jemalloc's own counter.
> The guard uses jemalloc `resident`, so its behavior tracks the 22 GB number, not `top`.

### Heap-profiler attempt — CRASHED the node (separate finding)
Activating on-demand jemalloc heap profiling (`prof_active:true`) then running heavy
allocation **SIGSEGV'd the node** inside `prof_backtrace_impl` (`hs_err_pid49548.log`):
```
SIGSEGV at prof_backtrace_impl+0x684  (libopensearch_native.dylib)
  _rjem_je_prof_backtrace → prof_tctx_create → malloc_default
   ← parquet_get_filtered_native_bytes_used → DashMap<String,WriterState>::next
   ← IndexingMemoryController.getNativeBytesUsed (background stats poll)
```
jemalloc's profiling backtrace is unstable on aarch64-macOS when sampling allocations
made across the FFM downcall boundary (background memory-stats poll iterating a DashMap).
So heap profiling is NOT a usable diagnostic on this platform for this workload — and it
also wouldn't show MODE B's 22 GB anyway (those pages aren't live, so profiling can't
attribute them).

## VERDICT (measured, not inferred)

Instrumented `DynamicLimitPool::try_grow` to log jemalloc `allocated` (live) vs
`resident` (RSS) at the moment of rejection. Reproduced on the archive node (debug
dylib). The reject log:

```
RSSGUARD SPILL reject:
  resident            = 29,246,504,960 B   (29.2 GB)   <- jemalloc RSS (what the guard checks)
  jemalloc_allocated  =        89,864,728 B   (~86 MB)  <- LIVE allocations
  spill threshold     = 29,205,777,612 B   (29.2 GB = 0.85 × 32 GiB limit)
  pool_used           = 0 B
  req                 = 179,704 B           (rejected a 180 KB alloc; also rejects 64 B)
```

**Live native memory = 86 MB, but RSS = 29.2 GB — a 327× gap.** The DataFusion pool is
empty (`pool_used=0`). So:

1. **It is NOT a leak.** `allocated`=86 MB means almost nothing is live. The 4-partition
   hash tables (peak 147–172 MB each × 4) were allocated, then **freed** when the queries
   finished.
2. **It IS jemalloc retention.** jemalloc freed the objects but kept the **physical pages**
   (dirty/muzzy) instead of returning them to the OS → RSS stays at 29 GB.
3. **The RSS guard misfires.** `memory.rs::try_grow` (§2) compares `resident` (29.2 GB of
   mostly-dead retained pages) against `0.85 × pool_limit` and rejects every allocation —
   even 64 bytes — treating retained RSS as live pressure. Spill can't help (pages are
   already free; there's nothing to spill), and the reject is pre-CAS so operators never
   even try.
4. **RSS never recovers while idle** because jemalloc `background_thread` is **disabled**
   (not in the compiled `MALLOC_CONF`, not set at startup) and page decay only runs during
   allocator activity or via that background thread. With the node idle after the failures,
   nothing triggers a purge → RSS pinned at ~29–36 GB indefinitely (observed: still 36 GB
   after 40s+ idle), so the 429 cascade never clears without a restart.

### Why 1-partition was fine
At 1 partition the transient hash-table allocations are small enough that retained RSS
never reaches `0.85 × 32 GiB`, so the guard never trips. 4× concurrency × bigger per-op
hash tables pushes peak (and therefore retained) RSS over the line.

### The two compounding defects
- **Primary:** the guard keys on `resident` (RSS incl. retained dead pages) as a proxy for
  memory pressure. It should consider `allocated` (live) — or at least force a jemalloc
  purge / not reject when `allocated ≪ resident`.
- **Secondary:** `background_thread` is off and no decay is applied at startup, so retained
  pages are never reclaimed while idle — turning a transient spike into a permanent wedged
  state.

### Config in play
- pool limit = **32 GiB** (`datafusion.memory_pool_limit_bytes`, persistent cluster setting).
  Thresholds are fractions of this; RSS only has to reach 27.2 GB (0.85) to wedge.
- `MALLOC_CONF` (`.cargo/config.toml`) = `prof:true,prof_active:false,lg_prof_sample:17`
  — profiling only, **no decay tuning, no `background_thread:true`**.
- `native.jemalloc.dirty_decay_ms` / `muzzy_decay_ms` cluster-setting defaults = 30000 ms,
  but decay still requires allocator activity or a background thread to actually run.

### Candidate fixes (not applied)
1. Enable jemalloc `background_thread:true` (via `MALLOC_CONF` or runtime mallctl) so dirty
   pages are purged on a timer even when idle — directly un-wedges the cascade.
2. Make the RSS guard purge-then-recheck, or gate on `allocated` (live) rather than
   `resident`, or skip the reject when `resident - allocated` (retained) is large.
3. Lower `dirty_decay_ms`/`muzzy_decay_ms` so pages return faster (partial mitigation;
   still needs activity/bg thread to fire).

---

## (original investigation notes below — superseded by VERDICT above)

> The rejecting code path was proven from logs; the bug-vs-config question (§5) is now
> answered by the jemalloc allocated-vs-resident measurement above.

## 0. Symptom

With `search.concurrent_segment_search.mode=auto` + `search.concurrent.max_slice_count=4`
(→ 4 partitions) on the archive node (clickbench, 99,997,497 docs), running the 43
ClickBench PPL queries: the first ~13 succeed (and are **faster** than 1-partition —
e.g. q9 2.4s→0.94s, q13 2.8s→0.87s), then from q14 onward almost every query fails
with HTTP 500/429 `Resources exhausted`.

## 1. What was DISPROVEN

Initial hypothesis (from code-reading, before logs): a phantom-reservation / stream-handle
**leak** on the query error path — a failed query never drops its `QueryStreamHandle`, so
its phantom reservation stays in the pool and admission-rejects everything after.

**Disproven by runtime evidence:**
- A *single* failed q14 does NOT poison the pool. Sequence tested on a fresh node:
  `count()` OK → q14 FAILS → `count()` **OK** immediately → OK after 30s idle → OK after 90s idle.
- The failing-allocation errors report **`0 already reserved`** and list "top memory
  consumers" all at **`consumed 0.0 B, peak 0.0 B`** — i.e. the pool is **empty** when it rejects.
- The pool recovers on its own once the node sits idle briefly.

So there is **no leak** and **no retained pool memory**. The earlier code-based leak
diagnosis was wrong.

## 2. What IS proven (from `/tmp/os-rootcause.log`, live archive run)

The rejection comes from the **`DynamicLimitPool::try_grow` RSS guard**
(`src/memory.rs:145-179`), specifically one of its two pre-CAS branches:

```rust
let limit = self.dynamic_limit.load(...);                 // = 34359738368 (32 GiB)
let resident = crate::memory_guard::cached_resident_bytes();   // jemalloc native RSS
if resident > 0 && limit >= 16 * 1024 * 1024 {
    let critical_bytes = (limit as f64 * 0.95) as usize;  // ≈ 30.4 GiB
    let spill_bytes    = (limit as f64 * 0.85) as usize;  // ≈ 27.2 GiB
    if resident_usize > critical_bytes {                  // (A) critical
        return Err(pool_limit_error(additional, name, reservation.size(), 0, limit));
    }
    if resident_usize > spill_bytes {                     // (B) spill
        return Err(pool_limit_error(additional, name, reservation.size(), 0, limit));
    }
}
```

Evidence it is this path and not any other:
- The error string is `Failed to allocate N bytes for <op> (0 already reserved) — 0 available out of 34359738368 limit`.
  The literal **`available = 0`** is passed **only** by these two branches
  (`memory.rs:161` and `:176`). The post-CAS reject path passes `limit - used`, not 0.
- Log counts during the cascade: `Failed to allocate … 0 available` = **66**;
  `Cannot reserve untracked memory budget` (the `acquire_budget` admission path) = **0**.
  → 100% of rejections are this guard, 0% are the admission/phantom path.
- It rejects allocations as small as **64 bytes** for a fresh `count()` even though the
  pool's own accounting is empty — consistent with an RSS-gated (not pool-usage-gated) reject.
- These branches are **silent** (no `log_info!`), which is why only the Java-side
  `CircuitBreakingException → HTTP 429` surfaced and the cause was invisible at first.

Mechanism: under 4 partitions, q14's `approx_distinct(UserID) by SearchPhrase` runs
4 concurrent `GroupedHashAggregateStream`s (log shows **peak 147–172 MB each**). The reject
is gated on **process RSS vs a fraction of the pool limit**, and fires **before** the
CAS/spill path — so operators that *could* spill never get the chance, and any concurrently
arriving query (even a tiny one) is rejected during the elevated-RSS window. Rejections clear
once RSS falls (jemalloc releases native buffers) — matching the observed idle-recovery.

## 3. Why it is NOT a DataFusion 54 regression

The RSS guard is identical on `main` (DF53) and `df-54-upgrade` (DF54). It is triggered by
the 4-partition memory profile, not the DataFusion version. (In the earlier **1-partition**
A/B, q14 also failed but did NOT cascade — at 1 partition the per-query memory is small enough
that RSS never parks above the threshold.)

## 4. Relevant configuration in play

- `datafusion.memory_pool_limit_bytes = 34359738368` (32 GiB) — **persistent cluster
  setting**, overrides the 24 GiB in `jvm.options`. The RSS thresholds are fractions of THIS.
- JVM heap `-Xms8g -Xmx8g`. `datafusion.spill_directory` now set (spill enabled).
- Idle native RSS observed = **2.15 GB**.

## 5. THE OPEN QUESTION — bug vs config (not yet decided)

The guard fired. The undecided part is **whether RSS was legitimately high**:

- **If `resident` genuinely exceeded ~27–30 GB at trip time** → the guard worked as designed;
  root cause is a **config mismatch** (pool limit 32 GiB is larger than the box can actually
  back at 4× concurrency, so the RSS protection trips). Fix = tune pool limit / thresholds.
- **If `resident` was NOT actually that high** → the guard is rejecting **spuriously** = a real
  **bug** (e.g. `cached_resident_bytes()` returning a bad/stale value, or threshold math against
  the wrong base). Fix = the guard logic.

This is unresolved because **branches (A)/(B) log nothing**, so the actual `resident` value at
trip time was never recorded. Idle RSS being only 2.15 GB makes a spike to ~28 GB *suspicious*
but unproven (the hash tables are hundreds of MB, not tens of GB — which would lean toward
"spurious / bug", but this must be measured, not assumed).

### How to settle it (no assumptions)
Add a one-line `log_info!` to branches (A) and (B) printing `resident`, `critical_bytes`,
`spill_bytes`, `limit` — then reproduce. That converts "the guard fired" into "the guard fired
because resident=<X> vs threshold=<Y>", which definitively classifies bug vs config. (Requires
one ~19-min release rebuild.) Alternatively read `df_get_memory_pool_stats` (tripped_count) to
confirm guard firings without RSS, but that still won't give the RSS value needed to adjudicate.

## 6. File / line references
| What | Where |
|---|---|
| RSS guard (the rejecting code) | `src/memory.rs:145-179` (branches at `:154`, `:169`) |
| `pool_limit_error` (builds the `0 available` message) | `src/native_error.rs:38-50` |
| `cached_resident_bytes()` (RSS source, 100ms cache) | `src/memory_guard.rs:38` |
| thresholds (0.75/0.85/0.85/0.95) | `src/memory_guard.rs:111-125` |
| admission path (NOT the cause here; 0 hits) | `src/query_budget.rs:201-324` |
| log evidence | `/tmp/os-rootcause.log` (66× `Failed to allocate … 0 available`) |
