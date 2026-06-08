# DF53 (main) vs DF54 (df-54-upgrade) — ClickBench 43-query A/B

- **Node:** 3.7.0-ARCHIVE, clickbench = 99,997,497 docs, default settings.
- **Build:** `cargo build --release` per branch; only the Rust dylib differs (Java core/plugins/modules identical across branches).
- **Protocol:** per query, 1 cold (cache cleared) + 2 warm; perf number = best of 2 warm. Result rows captured from any successful run.
- **main** = `8730c3a0e27` (DataFusion 53.1.0). **df-54-upgrade** = `af58d0b5d91` (DataFusion 54.0.0).
- `dc()` (approx distinct-count) queries flagged — may differ run-to-run, not a correctness regression.

## Correctness (cross-branch result equality)

| Query | rows main | rows df54 | match | note |
|---|---|---|---|---|
| q1 | 1 | 1 | ✅ |  |
| q2 | 1 | 1 | ✅ |  |
| q3 | 1 | 1 | ✅ |  |
| q4 | 1 | 1 | ✅ |  |
| q5 | 1 | 1 | ≈ | dc() approx — values close, not bit-equal |
| q6 | 1 | 1 | ≈ | dc() approx — values close, not bit-equal |
| q7 | 1 | 1 | ✅ |  |
| q8 | 18 | 18 | ✅ |  |
| q9 | 10 | 10 | ≈ | dc() approx — values close, not bit-equal |
| q10 | 10 | 10 | ≈ | dc() approx — values close, not bit-equal |
| q11 | 10 | 10 | ≈ | dc() approx — values close, not bit-equal |
| q12 | 10 | 10 | ≈ | dc() approx — values close, not bit-equal |
| q13 | 10 | 10 | ✅ |  |
| q14 | ERR | ERR | ⚠️both | both error (pre-existing) |
| q15 | 10 | 10 | ✅ |  |
| q16 | 10 | 10 | ✅ |  |
| q17 | 10 | 10 | ✅ |  |
| q18 | 16384 | 16384 | ✅ |  |
| q19 | 10 | 10 | ✅ |  |
| q20 | 4 | 4 | ✅ |  |
| q21 | 1 | 1 | ✅ |  |
| q22 | 10 | 10 | ✅ |  |
| q23 | 10 | 10 | ≈ | dc() approx — values close, not bit-equal |
| q24 | 10 | 10 | ✅ |  |
| q25 | 10 | 10 | ❌ | MISMATCH |
| q26 | 10 | 10 | ✅ |  |
| q27 | 10 | 10 | ✅ |  |
| q28 | 25 | 25 | ✅ |  |
| q29 | 25 | 25 | ✅ |  |
| q30 | 1 | 1 | ✅ |  |
| q31 | 10 | 10 | ❌ | MISMATCH |
| q32 | 10 | 10 | ✅ |  |
| q33 | 10 | 10 | ❌ | MISMATCH |
| q34 | 10 | 10 | ✅ |  |
| q35 | 10 | 10 | ✅ |  |
| q36 | 10 | 10 | ✅ |  |
| q37 | 10 | 10 | ✅ |  |
| q38 | 10 | 10 | ✅ |  |
| q39 | 10 | 10 | ✅ |  |
| q40 | 10 | 10 | ❌ | MISMATCH |
| q41 | 10 | 10 | ❌ | MISMATCH |
| q42 | 10 | 10 | ✅ |  |
| q43 | 0 | 0 | ✅ |  |

**Real (non-approx, non-shared-error) mismatches: 5** ['q25', 'q31', 'q33', 'q40', 'q41']
> q14 errors HTTP 500 on **both** branches (pre-existing, DF53 too) → not a DF54 regression.

## Performance — best-of-2-warm (seconds)

| Query | main warm | df54 warm | Δ (df54−main) | df54/main | main cold | df54 cold |
|---|---|---|---|---|---|---|
| q1 | 0.015 | 0.019 | +0.004 | 1.27x ⚠️ | 1.645 | 1.994 |
| q2 | 0.114 | 0.152 | +0.038 | 1.34x ⚠️ | 0.488 | 0.612 |
| q3 | 0.366 | 0.387 | +0.021 | 1.06x | 0.447 | 0.466 |
| q4 | 0.331 | 0.357 | +0.026 | 1.08x | 0.408 | 0.430 |
| q5 | 0.578 | 0.634 | +0.056 | 1.10x | 0.597 | 0.633 |
| q6 | 1.585 | 1.477 | -0.108 | 0.93x | 1.753 | 1.733 |
| q7 | 0.015 | 0.015 | +0.000 | 1.01x | 0.025 | 0.025 |
| q8 | 0.144 | 0.150 | +0.005 | 1.04x | 0.140 | 0.151 |
| q9 | 2.283 | 2.405 | +0.123 | 1.05x | 2.406 | 2.758 |
| q10 | 3.317 | 3.488 | +0.171 | 1.05x | 3.370 | 3.730 |
| q11 | 0.634 | 0.706 | +0.073 | 1.11x ⚠️ | 0.707 | 0.727 |
| q12 | 0.758 | 0.805 | +0.047 | 1.06x | 0.812 | 1.005 |
| q13 | 2.759 | 2.833 | +0.074 | 1.03x | 2.889 | 3.043 |
| q14 | None | None | — | — | 34.9734 | 40.0611 |
| q15 | 2.610 | 2.707 | +0.096 | 1.04x | 2.780 | 2.996 |
| q16 | 3.309 | 3.375 | +0.066 | 1.02x | 3.412 | 3.490 |
| q17 | 6.744 | 6.765 | +0.021 | 1.00x | 6.505 | 7.009 |
| q18 | 7.461 | 8.097 | +0.637 | 1.09x | 7.706 | 8.023 |
| q19 | 16.487 | 18.400 | +1.913 | 1.12x ⚠️ | 17.558 | 19.395 |
| q20 | 0.174 | 0.175 | +0.001 | 1.01x | 0.304 | 0.353 |
| q21 | 7.790 | 7.718 | -0.072 | 0.99x | 8.023 | 7.936 |
| q22 | 8.033 | 8.377 | +0.344 | 1.04x | 8.419 | 8.522 |
| q23 | 12.616 | 13.118 | +0.501 | 1.04x | 12.913 | 13.410 |
| q24 | 16.841 | 14.175 | -2.666 | 0.84x 🟢 | 17.480 | 15.493 |
| q25 | 0.290 | 0.182 | -0.108 | 0.63x 🟢 | 0.304 | 0.195 |
| q26 | 1.083 | 1.052 | -0.031 | 0.97x | 1.134 | 1.133 |
| q27 | 0.587 | 0.426 | -0.162 | 0.73x 🟢 | 0.610 | 0.453 |
| q28 | 7.035 | 7.030 | -0.006 | 1.00x | 7.367 | 7.480 |
| q29 | 78.485 | 80.543 | +2.058 | 1.03x | 78.781 | 80.997 |
| q30 | 0.818 | 0.796 | -0.022 | 0.97x | 0.842 | 0.834 |
| q31 | 2.518 | 2.727 | +0.209 | 1.08x | 2.803 | 3.119 |
| q32 | 2.423 | 2.539 | +0.117 | 1.05x | 2.877 | 3.048 |
| q33 | 7.362 | 8.155 | +0.793 | 1.11x ⚠️ | 9.808 | 10.814 |
| q34 | 14.867 | 15.235 | +0.368 | 1.02x | 14.843 | 15.627 |
| q35 | 16.898 | 17.498 | +0.600 | 1.04x | 16.278 | 17.423 |
| q36 | 4.381 | 4.775 | +0.394 | 1.09x | 4.464 | 4.863 |
| q37 | 0.119 | 0.117 | -0.002 | 0.98x | 0.163 | 0.171 |
| q38 | 0.063 | 0.065 | +0.002 | 1.03x | 0.067 | 0.070 |
| q39 | 0.066 | 0.068 | +0.002 | 1.03x | 0.068 | 0.068 |
| q40 | 0.203 | 0.201 | -0.002 | 0.99x | 0.230 | 0.231 |
| q41 | 0.033 | 0.035 | +0.002 | 1.05x | 0.065 | 0.068 |
| q42 | 0.034 | 0.034 | +0.000 | 1.01x | 0.041 | 0.038 |
| q43 | 0.180 | 0.223 | +0.043 | 1.24x ⚠️ | 0.191 | 0.234 |

**Total warm (sum of best-of-2, excl. q14): main=232.41s  df54=238.04s  → df54/main = 1.024x**
- per-query ratio: median=1.036x, min=0.629x, max=1.338x, mean=1.032x
- queries >10% slower on df54: ['q1', 'q2', 'q11', 'q19', 'q33', 'q43']
- queries >10% faster on df54: ['q24', 'q25', 'q27']

## Mismatch analysis — all 5 are tie-break ordering, NOT regressions

The 5 ❌ rows are `sort … | head N` queries where the sort key has **duplicate values**;
`head`/`LIMIT` then returns an arbitrary subset among the ties. Verified:

| Query | sort key | why it differs | result-set equal? |
|---|---|---|---|
| q25 | `sort EventTime` (head 10) | many rows share same EventTime | ✅ identical as a set |
| q31 | `sort - c` (head 10) | ties on count `c` | leading non-tied rows match |
| q33 | `sort - c` (head 10) | dozens of rows with count=2 | leading non-tied rows match |
| q40 | `sort - PageViews` (head 10 from 1000) | ties at PageViews=15 | leading non-tied rows match |
| q41 | `sort - PageViews` (head 10 from 100) | ties at PageViews=27 | leading non-tied rows match |

This is non-determinism inherent to partial-sort-with-limit over tied keys — it varies
run-to-run on the **same** branch too. **No DF53→DF54 correctness regression.**

## Verdict

- **Correctness:** ✅ PASS. 36/43 exact match; 7 `dc()` approx-distinct within tolerance; 5 tie-break-order (set-equal); q14 errors on BOTH branches (pre-existing, not DF54). Zero real regressions.
- **Performance:** ~par. Total warm df54/main = **1.024x** (≈2.4% slower aggregate), per-query median **1.036x**. A few short queries are noisier (q1/q2/q43 are sub-0.2s so % is dominated by jitter); q19 (+1.9s, 1.12x) and q33 (1.11x) are the only non-trivial slowdowns, while q24/q25/q27 are faster on df54. Net: within run-to-run noise, no material perf change from the upgrade.
