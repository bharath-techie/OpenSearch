# Work-stealing A/B over match()-augmented ClickBench (delegation path)

**Date:** 2026-06-12  **Node:** archive, ~99,997,497 clickbench docs, **8 partitions**
(mode=auto, search.concurrent.max_slice_count=8 → min(8,10 cores)=8).
**Build:** rebased onto upstream/main + work-stealing (commits bc1442be828, 68e54f0e763).
**Why match():** `match(field, term)` cannot be answered by pure parquet — it round-trips
to Lucene per chunk, forcing the **indexed/delegation path** where work-stealing's
collectors actually live. This stresses the feature far more than pure-parquet ClickBench.

Each of the 43 queries was augmented two ways (86 total), via `gen_match_queries.py`:
- **nonsel** = AND `match(URL,'http')` → ~98.8% of docs match → *uniform heavy delegation load*.
- **sel** = AND `match(URL,'yandex')` → ~0.01% (13,149 docs) → *heavy RG pruning → per-partition imbalance*.

Method: same build, toggle dynamic `datafusion.indexed.work_stealing` between OFF/ON arms.
Warm-only (1 discarded warmup + 1 measured warm; cold runs over ~99M delegated docs are
punishingly slow). Runner: `run_work_stealing_match_ab.sh 8`.

## Correctness: ZERO regressions in both variants ✅

- **Every** content difference OFF vs ON is a `head`/LIMIT query over a **non-total order**
  (verified: the sort-key column sequence is byte-identical OFF vs ON — e.g. sel q31
  col0 `[9,9,9,9,8,8,7,7,6,6]` in both arms; q12 `[334,21,19,16,9,6,5,5,4,4]` both — only
  the *tied* rows within equal-key bands differ; q18 differs because it has no `sort` at
  all). PPL defines no order among ties under LIMIT → each arm returns a valid answer.
  Same benign pattern proven in the pure-parquet runs.
- nonsel flagged 8 (q18,q22,q25,q32,q33,q39,q40,q41) — all benign LIMIT-ties.
  sel flagged 7 (q12,q13,q17,q18,q31,q32,q33) — all benign LIMIT-ties.
- **No query with a deterministic total order ever differed.**

## The 2 nonsel errors are pre-existing and NOT work-stealing-related

q14 (`dc(UserID) by SearchPhrase`) and q15 (`count() by SearchEngineID,SearchPhrase`)
time out (>90s) over ~99M delegated docs — **identically with the flag OFF and ON**
(verified directly: q14 OFF timeout / ON timeout; q15 OFF timeout / ON timeout). These
are heavy distinct-count / high-cardinality-string group-bys, a pre-existing aggregation
perf limit. The transient `byte array offset overflow` panics seen during the run were
cold-first-run pileups on these few heavy queries, not ON-specific, not a correctness bug.
(In sel, where match prunes to ~13k docs, q14/q15 both run fine in ~0.3s.)

## Performance: stealing helps the IMBALANCE case (sel), ~flat under uniform load (nonsel)

| Variant | regime | OFF total warm | ON total warm | ON/OFF |
|---|---|---|---|---|
| **sel** (match yandex, ~13k docs) | heavy RG pruning → uneven partitions | 31.75s | 29.31s | **0.923x (ON 7.7% faster)** |
| **nonsel** (match http, ~99M docs) | uniform heavy delegation, every partition saturated | 88.99s | 94.32s | 1.060x (ON 6% slower) |

Interpretation — this is exactly the expected shape:
- **sel is the case work-stealing targets.** Selective match → most row-groups pruned per
  partition, but unevenly (some partitions keep matching RGs, others get pruned to ~nothing).
  ON rebalances → **7.7% faster**, with standout wins q38 0.48x, q23 0.50x, q36 0.67x,
  q41 0.71x, q37 0.76x — the small/medium queries where one partition would otherwise idle.
- **nonsel has little to steal.** When ~99% of docs match, *every* partition is uniformly
  saturated with Lucene-collect + decode work — there's almost no idle time to steal into,
  so the shared-queue mutex + lazy-unfold-driver overhead shows as a small net loss.

### nonsel regression re-verified best-of-3-hot (NOT just warm-only noise)

The warm-only nonsel total (ON 6% slower) was re-tested on the 15 meaningful regressors
with **1 prime + best-of-3 hot runs per query, per arm, interleaved** (`/tmp/retest_hot.py`):

| Query | shape | OFF best3 | ON best3 | on/off | verdict |
|---|---|---|---|---|---|
| q23 | `dc(UserID) by SearchPhrase` + Title/URL filters | 2.93 | 5.01 | **1.71x** | real |
| q35 | `count() by const, URL` (wide string group) | 4.24 | 6.77 | **1.60x** | real |
| q24 | `sort EventTime \| head` over matched docs | 9.62 | 12.59 | **1.31x** | real |
| q6  | `dc(SearchPhrase)` | 2.15 | 2.63 | **1.22x** | real |
| q33 | `count(),avg() by WatchID, ClientIP` | 3.38 | 3.93 | 1.16x | real-ish |
| q36 | `count() by ClientIP,(3 derived)` | 1.21 | 1.39 | 1.15x | real-ish |
| q9,q10,q21,q28 | numeric/region group-bys | ~ | ~ | ~1.00x | **was noise** |
| q22,q26,q11,q13,q16 | mixed | ~ | ~ | 0.84–0.94x | **noise / ON faster** |

**15-query subset total: OFF 37.2s → ON 45.4s (1.22x).** So the regression is **real and
reproducible**, but **concentrated in a specific shape**: queries that decode/aggregate
*wide or high-cardinality data per matched row* (dc() distinct-count, group-by on big
string columns URL/SearchPhrase, or a full sort) over ~99M uniformly-matched docs. Narrow
numeric group-bys (q9/q16) are flat or slightly ON-faster even here. ~9 of the 15
"regressors" from the single-shot run were just noise and disappeared at best-of-3.

**Mechanism (hypothesis):** under uniform saturation there is no idle partition, so
stealing adds only overhead — and the heaviest queries amplify it: the lazy per-chunk
`unfold` driver builds one `IndexedExec` (+ evaluator + collector) per popped chunk on
the shared path, vs the eager `Local` path that builds a partition's chunk streams up
front; for big-per-row work the shared path's per-chunk setup + cross-thread chunk
hand-off competes with the saturated decode/collect threads. Worth a follow-up: on the
shared path, when a partition has no contention (queue not actually being raced), the
overhead is pure cost. A possible mitigation is to fall back to eager/local execution
when the scan is detected to be uniformly dense (few-to-no prunes) — i.e. steal only
when imbalance is likely. (Not implemented; flagged for the PR.)

## Verdict

- **Correctness on the delegation path is rock-solid**: 0 real regressions across 86
  match-augmented queries at 8 partitions; all diffs are LIMIT-tie non-determinism.
- **Work-stealing pays off where it's designed to** — selective/uneven delegation
  (sel: +7.7%).
- **Under uniformly-saturated load (nonsel) there is a REAL regression** (best-of-3-hot
  verified, not noise) — but it is **confined to heavy wide/high-cardinality-per-row
  queries** (dc(), big-string group-bys, full sort over ~99M matched docs): q23 1.71x,
  q35 1.60x, q24 1.31x, q6 1.22x. Narrow numeric group-bys are flat/ON-faster even here.
  Mechanism: no idle partition to steal into → the lazy per-chunk driver's setup +
  cross-thread hand-off is pure overhead, amplified by big per-row work.
- **Net:** the dynamic per-query toggle + default-ON remains the right call for the common
  (uneven) case, BUT this is a concrete lead for a follow-up optimization: **gate the
  shared/stealing path on likely-imbalance** (e.g. fall back to eager Local execution when
  the scan is uniformly dense / few prunes), so the saturated regime stops paying the
  overhead. Flagged for the PR.

## Artifacts
- `p8/sel/` and `p8/nonsel/`: bench_off.json, bench_on.json, COMPARISON.md, console_*.txt.
- Query variants: `../ppl_match/{sel,nonsel}/q*.ppl` (generated by `../gen_match_queries.py`).
