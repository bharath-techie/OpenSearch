# Work-stealing OFF vs ON — ClickBench 43-query A/B: findings

**Date:** 2026-06-12
**Node:** archive (3.7.0-ARCHIVE home + 3.8.0-ARCHIVE data dir, ~99,997,497 clickbench docs)
**Build:** release dylib + 3.8.0 plugin jar with `datafusion.indexed.work_stealing` wired.
**Concurrency:** multi-partition — `search.concurrent_segment_search.mode=auto`,
`search.concurrent.max_slice_count=4` → `target_partitions=min(4,10 cores)=4`.
**Method:** same build, only the dynamic `datafusion.indexed.work_stealing` setting
toggled between arms. Per query: 1 cold (cache cleared) + 2 warm; perf = best-of-2-warm.

## Correctness: NO regression from work-stealing ✅

- **37 / 42** queries (excluding q14) matched **byte-exact** OFF vs ON.
- **5 "mismatches" (q18, q25, q32, q33, q40)** are all `head`/LIMIT over a
  **non-total order** — PPL defines no unique result for them, so OFF and ON each
  return a *valid* answer; the rows that differ are exactly the tied/unordered ones:
  - q18: `stats count() by UserID,SearchPhrase | head 10` — **no sort** → any 10 groups valid.
  - q32, q33: `... | sort -c | head 10` — differing rows **all have c=1** (verified): a
    pure tie-band at the bottom of the sort; the deterministic head-by-count is stable.
  - q25: `sort EventTime | head 10` — EventTime ties.
  - q40: `sort -PageViews | head 10 from 1000` — differing rows **all PageViews=15**
    (verified tie), offset 1000 deep.
  Confirmed mechanism: work-stealing changes which segments/chunks complete first, so
  for an *unordered* limit the surfaced rows differ (ON isn't even stable run-to-run on
  q18). This is correct PPL behaviour, not a bug. A total `ORDER BY` above the scan
  (the common real case) is re-sorted by `SortExec` and is unaffected — proven by the
  37 exact matches, which include every query with a deterministic total order.
- **q14** errored in **both** arms identically (`dc(UserID) ... sort -u | head 10`,
  ~123s timeout) — a pre-existing distinct-count perf/timeout issue, unrelated to
  work-stealing (OFF and ON behave the same).

> Recommendation: byte-exact correctness holds wherever the query result is
> well-defined. The unordered-LIMIT non-determinism is inherent to LIMIT-without-
> total-sort and is not introduced by work-stealing (the static path is merely
> deterministic by accident of fixed partition order). If deterministic unordered
> LIMIT is ever required, that's a separate query-semantics decision, not a
> work-stealing fix.

## Performance: ~neutral, slight win (best-of-2-warm)

- **Total warm (sum best-of-2): OFF=108.71s  ON=106.02s  → ON/OFF=0.975x (ON ~2.5% faster).**
- Per-query Δ is mostly within run-to-run noise (±5–10%). Notable warm improvements:
  q6 0.77x (-0.93s), q22 0.81x (-0.53s), q34 0.90x (-0.73s), q27 0.74x, q28 0.90x.
  Notable regressions: q33 1.10x (+0.52s), q35 1.05x (+0.39s), q19 1.03x (+0.23s).
- Cold times also broadly favor ON (e.g. q6 4.33→3.58, q17 3.54→2.53, q11 1.63→0.73).
- Caveat: a clean wall-clock read is muddied by q14/q24/q29 (20–124s outliers) and the
  fact that this clickbench shape is NOT especially lopsided per partition, so the
  rebalancing upside is modest. Work-stealing's win is largest under *uneven* per-
  partition cost (heavy dynamic filtering / skewed segments); a balanced scan shows
  ~neutral, which is the expected and acceptable result for a default-on feature.

## Verdict

Work-stealing ON is **correctness-safe** (no real regression across 43 queries at
4 partitions) and **performance-neutral-to-slightly-positive** on the clickbench
suite. Safe to keep ON by default. The real perf upside needs a deliberately lopsided
workload to demonstrate (future targeted benchmark).

## Artifacts
- `bench_off.json`, `bench_on.json` — full per-query rows + timings.
- `COMPARISON.md` — generated table (the ❌ marks there are the unordered-LIMIT cases
  explained above; not real mismatches).
- `console_off.txt` / `console_on.txt` — per-query run logs.
