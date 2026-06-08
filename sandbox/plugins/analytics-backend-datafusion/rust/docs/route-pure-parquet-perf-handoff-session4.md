# Route-pure-parquet-through-indexed: perf handoff (session 4)

> Continues session 3. **Lever 1 (residual `on_batch_mask`) is now ROOT-CAUSED and FIXED**,
> verified single-variable on both debug and release. The session-2/3 hypotheses (per-batch
> expr re-planning; un-fusable two-pass filter) were both wrong about the dominant cost.

---

## 0. Headline — FIXED, release-confirmed

**Root cause of q08's slow residual:** the indexed residual predicate was
`cast(AdvEngineID as Int32) != Int32(0)` — a **per-batch, per-row column widening cast**
(AdvEngineID is Int16). Vanilla never does this: its optimizer's `unwrap_cast_in_comparison`
rule rewrites the cast onto the literal → `AdvEngineID != Int16(0)` (a one-time const fold).
The indexed path skipped the optimizer (it lowers the raw substrait `Expr` straight to a
PhysicalExpr), so the column cast survived to the hot path and ran on all 58.15M rows.

**Fix:** coerce + simplify each residual leaf `Expr` against the scan schema before lowering,
in `substrait_to_tree.rs::convert_expr` → `simplify_residual_expr()`. Order matters and mirrors
DataFusion's analyzer→optimizer: `ExprSimplifier::coerce` (inserts the cast TypeCoercion would)
THEN `.simplify()` (runs `unwrap_cast_in_comparison`, moving the cast to the literal). Coerce-first
is essential: the raw expr has no cast yet, so unwrap has nothing to do until coercion inserts it.

**Measured effect (single-variable A/B — only the cast differs, via a temporary
`DISABLE_RESIDUAL_SIMPLIFY` env toggle, now removed):**

| `on_batch_mask_time` | cast present (pre-fix) | cast unwrapped (post-fix) |
|---|---|---|
| **debug** | 78–81 ms | 36–41 ms (**−51%**) |
| **release** | ~20–24 ms (session-2 baseline) | **10.6–12.3 ms (~−50%)** |

Release q08 residual filter is now **at/under vanilla parity**: indexed `on_batch_mask 10.6 +
filter_record_batch 3.4 = 14.0 ms` vs vanilla `FilterExec 15.85 ms` (eval+gather). The +7 ms
Lever-1 gap from session-2 §5 is **CLOSED**.

**Correctness:** full 43-query harness `correctness()` clean — all EXACT-MATCH, byte-identical
vanilla-vs-indexed. Zero new regressions; the only non-passes (q14, q17, q28, q36, q18, q29) are
the exact pre-existing failures from session 2. q08 rows verified byte-identical directly.

---

## 1. The change (ready to land, minus instrumentation cleanup)

- `src/indexed_table/substrait_to_tree.rs`:
  - new `fn simplify_residual_expr(expr, df_schema)` — `SimplifyContext::default().with_schema(...)`
    → `ExprSimplifier::new(ctx).coerce(...).and_then(.simplify(...))`, falling back to the input
    expr on any error (correctness-preserving; `create_physical_expr` re-coerces anyway).
    NOTE: DataFusion is **53.1.0 from crates.io** (NOT the newer `~/Documents/dev/datafusion`
    checkout — its `SimplifyContext::builder()` API does not exist in 53.1.0; use `default()`/`with_schema`).
  - `convert_expr` leaf arm calls it before `create_physical_expr`.
  - new unit test `unwrap_cast_folds_column_cast_onto_literal` (added an Int16 `small` column to
    `test_schema`) — asserts the lowered PhysicalExpr tree contains NO `CastExpr`. This test FAILS
    without the `coerce`-before-`simplify` ordering, so it locks in the fix.
- The session-3 `CachedResidual` change (remap-once) is also still in the tree — it's correct and
  a strict improvement for big predicates, just not the q08 lever. Keep it.

### Still to remove before landing
- TEMP `api.rs::stream_close` QUERY_PROFILE instrumentation (carried from session 1/2).
- The `RESIDUAL_PROBE` log in `indexed_executor.rs` was added and **already removed** this session.
- The `DISABLE_RESIDUAL_SIMPLIFY` env toggle was added for the A/B and **already removed**.

---

## 2. Remaining q08 gap (release, post-fix) — Lever 2 only

q08 wall (release, interleaved median): vanilla 0.145 s, indexed 0.181 s → **+25%**. CPU breakdown:

| work | vanilla | indexed | Δ | note |
|---|---|---|---|---|
| decode | 28.50 (`processing`) | 28.88 (`parquet_poll`) | ~0 | parity |
| residual filter | 15.85 (`FilterExec`) | 14.0 (`on_batch_mask 10.6`+`filter_record 3.4`) | **−1.9** | **FIXED** |
| agg partial | 3.26 | (in elapsed_compute) | ~0 | parity |
| candidate compute | — | 2.24 (`index_query_time`) | +2.2 | indexed-only |
| **per-RG setup ×110** | ~1 | `metadata_load 3.4 + opening 4.4 ≈ 7.8` | **+6.8** | **Lever 2 (untouched)** |

The ENTIRE remaining gap is now **Lever 2: per-RG construction** — 110 row-groups each pay
`metadata_load` + `time_elapsed_opening` that vanilla pays ~once per file. This is architectural
(session-1 §5 / session-2 §5 "one DataSourceExec over candidate RGs"). It is the next lever.

---

## 3. Generality

The fix helps ANY indexed residual comparing a narrow column to a wider literal (very common:
Int16/Int8 columns vs integer literals, which Calcite/substrait widen). q02 (`AdvEngineID!=0`
count) and the q37–q42 multi-predicate `where` clauses all carry such comparisons. Re-run the
release perf suite to quantify per-query (debug wall is warmup-dominated — gotcha #4 — so trust
release + the single-accumulator metrics).

---

## 4. Environment / state at session end
- Node on **release** dylib (jvm.options reverted debug→release; release built 20:25 with the fix).
- Settings: persistent unchanged; transient re-applied (pushdown off, single-partition). The
  `route_pure_parquet_through_indexed` transient was toggled per-run during A/B — currently `false`
  (persistent default governs). Set it as needed.
- `jvm.options` backed up at `/tmp/jvm.options.bak` before edits.
- All session-3 + session-4 changes compile clean (debug + release) and unit tests pass.
</content>
