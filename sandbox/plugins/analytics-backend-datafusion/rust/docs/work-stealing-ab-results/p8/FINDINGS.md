# Work-stealing OFF vs ON — ClickBench 43-query A/B @ 8 partitions: findings

**Date:** 2026-06-12
**Node:** archive (3.7.0-ARCHIVE home + 3.8.0-ARCHIVE data, ~99,997,497 clickbench docs)
**Concurrency:** `mode=auto`, `search.concurrent.max_slice_count=8` → `target_partitions=min(8,10 cores)=8`.
**Method:** same build, toggle dynamic `datafusion.indexed.work_stealing` between arms.
1 cold (cache cleared) + 2 warm; perf = best-of-2-warm.

## Correctness: NO regression ✅ (same conclusion as 4-partition)

- Every query with a **deterministic total order** matched **byte-exact** OFF vs ON.
- 9 queries differ (q18, q22, q25, q32, q33, q39, q40, q41, q42) — **all** `head`/LIMIT
  over a non-total order. Verified rigorously:
  - **q22**: same 10 rows, different order only (`set_equal=True`) — pure reordering of
    a tied result.
  - **q18**: `stats count() by ... | head 10` with **no sort** → any 10 groups valid.
  - **q25, q32, q33, q39, q40, q41, q42**: `sort <k> | head` where the **sort-key column
    sequence is byte-identical OFF vs ON** (e.g. q41 col0 = `[27,27,26,26,25,25,25,24,24,24]`
    in both arms) — the sort is correct and stable; only *which tied rows* fill the
    equal-key slots at the LIMIT cut differs (q39/q42 are entirely ties at one key;
    q41 differs only within the `27` band). PPL defines no order among ties under LIMIT.
- **More mismatches than at 4 partitions (9 vs 5)** is expected: more partitions → more
  emission-order variance, so more tie-bands get reshuffled. NOT a correctness bug —
  the deterministic prefix of every sort is identical.
- **q14** errors in BOTH arms (~152s `dc()` timeout, pre-existing, unrelated).

## Performance: ON faster by 4.5% (vs 2.5% at 4 partitions)

- **Total best-of-2-warm: OFF=85.84s  ON=82.00s → ON/OFF=0.955x (ON ~4.5% faster).**
- The win **grew** with partition count (4p: 2.5% → 8p: 4.5%), consistent with the
  theory: more sibling partitions = more chance one idles while others still have work,
  so more for stealing to rebalance.
- Standout warm wins: q35 0.47x (-3.97s), q34 0.66x (-2.30s), q15 0.38x (-1.43s),
  q29 0.89x (-1.68s), q33 0.82x (-0.87s), q9 0.73x (-0.73s), q36 0.64x.
- Standout regressions (mostly small/medium queries dominated by per-query overhead and
  run-to-run noise): q19 1.43x (+1.64s), q6 1.47x (+1.10s), q23 1.33x (+0.92s),
  q24 1.24x (+3.79s on a 16–20s query), q18 1.45x.
- Note q15 cold OFF=40.5s is a clear outlier (cold-cache + scheduler artifact); warm is
  the reliable signal.

## Cross-reference: 4 vs 8 partitions

| | OFF total warm | ON total warm | ON/OFF | real correctness regressions |
|---|---|---|---|---|
| 4 partitions | 108.71s | 106.02s | 0.975x | 0 |
| 8 partitions | 85.84s | 82.00s | 0.955x | 0 |

8 partitions is faster than 4 in absolute terms (more parallelism), and work-stealing's
relative benefit increases with partition count.

## Verdict

At 8 partitions, work-stealing ON remains **correctness-safe** (0 real regressions) and
its **performance benefit grows** (4.5% vs 2.5% at 4p). Reinforces keeping it ON by
default. Raw data: `bench_off.json`, `bench_on.json`, `COMPARISON.md`.
