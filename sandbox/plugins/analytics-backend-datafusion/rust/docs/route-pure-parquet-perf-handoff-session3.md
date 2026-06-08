# Route-pure-parquet-through-indexed: perf handoff (session 3)

> Continues session 2. Goal unchanged: pure-parquet queries at parity with vanilla.
> This session: chased **Lever 1** (the residual `on_batch_mask` cost on q08), implemented
> the proposed fix (cache the remapped residual expr), and **measured that it does NOT help q08**.
> The session-2 Lever-1 hypothesis was wrong about *where* the time goes. Corrected model below.

---

## 0. Headline

**The session-2 Lever-1 fix (cache the per-batch `remap_expr_to_batch`) is implemented, correct,
unit-tested — and made NO measurable difference to q08.** `on_batch_mask_time` is unchanged
(pre-fix warm 19.8–24.0 ms vs post-fix 19.5–27.6 ms — same noise band). So the dominant cost in
`on_batch_mask` is **NOT** the residual expr re-planning. Keep the fix (it removes genuine
redundant allocation and is a strict improvement for large/complex predicates), but it is not
the q08 lever.

---

## 1. What was changed (kept — correct, tested, harmless)

`CachedResidual` in `indexed_table/eval/eval_helpers.rs`: wraps the residual `PhysicalExpr` and
runs `remap_expr_to_batch` **once** (via `OnceLock`), reusing the reseated expr for every batch
instead of re-walking + rebuilding the expr tree per batch.

- Wired into `PredicateOnlyEvaluator` (`residual_expr: Option<Arc<…>>` field → `residual: Option<CachedResidual>`)
  and `SingleCollectorEvaluator` (same). Public `new()` signatures unchanged (still take
  `Option<Arc<dyn PhysicalExpr>>`); the wrap happens inside the constructor.
- Free fn `evaluate_residual` retained for `bitmap_tree.rs:950` (no per-query instance to cache on there).
- 2 new unit tests in `eval_helpers` (`cached_residual_remaps_and_matches_uncached`,
  `cached_residual_reuses_across_batches`); all 36 eval-module tests pass.
- Compiles clean (debug + release). Release dylib rebuilt 2026-06-08 19:10.

**Why it didn't help q08:** q08's residual is `AdvEngineID != 0` — a trivial 1-column expr. The
tree-walk it avoids is a handful of nodes. The 20 ms in `on_batch_mask` is **per-batch arrow
`evaluate` + `ColumnarValue::into_array` + downcast overhead × 7,150 batches**, not the remap.
The fix should still help queries with big AND/OR predicate trees (q37–q42), but that wasn't measured.

---

## 2. q08 measured profile (RELEASE, single-partition, pushdown OFF) — the corrected breakdown

Query: `source = clickbench | where AdvEngineID!=0 | stats count() by AdvEngineID | sort - count()`

Wall (warm, 3 runs): **vanilla ≈ 0.114 s, indexed ≈ 0.177 s → ~60 ms gap.**

| work | vanilla | indexed | note |
|---|---|---|---|
| residual filter EVAL | folded into FilterExec | `on_batch_mask_time` **≈ 20 ms** | eval-only on indexed |
| residual filter APPLY/gather | `FilterExec elapsed_compute` **≈ 15 ms** (eval+gather fused) | `filter_record_batch_time` ≈ 4 ms | |
| → residual total | **≈ 15 ms** | **≈ 24 ms** | **indexed +9 ms** |
| decode | `DataSourceExec processing` ≈ 27 ms | `parquet_poll_time` ≈ 30–37 ms | indexed +3–10 ms |
| index_query (candidate) | — | ≈ 2–3 ms | indexed-only |
| metadata/open (per-RG ×110) | ~1 ms | `metadata_load 4.3 + opening 5.7` ≈ 10 ms | indexed-only |
| agg partial | ≈ 3.2 ms | ≈ 4.3 ms | parity |

Indexed `QueryShardExec elapsed_compute` ≈ 57–73 ms. Key batch counters:
`parquet_batches_received = 7.15 K`, `batches_pre_coalesce = 5.92 K`, `batches_produced = 92`,
`rows_matched = 58.15 M`, `rows_pruned_by_page_index = 41.81 M` (so ~58 M rows / 7.15 K ≈ full
8 K-row batches — NOT a tiny-batch fragmentation problem).

**The real q08 gap is the SUM of two structural costs, neither of which is the remap:**
1. **Un-fused residual (≈ +9 ms).** Vanilla's `FilterExec` evaluates the predicate AND gathers
   surviving rows in one fused vectorized pass (15 ms total). Indexed does it in two passes:
   `on_batch_mask` builds a `BooleanArray` (20 ms), then `filter_record_batch` gathers (4 ms).
   The 20 ms eval-only > 15 ms eval+gather → the two-pass split + per-batch
   `ColumnarValue::into_array`/downcast churn is the cost, over 7,150 batches.
2. **Per-RG construction (≈ +9 ms).** 110 row-groups each pay `metadata_load` + `time_elapsed_opening`
   (≈ 10 ms summed) that vanilla pays ~once per file. This is session-2 Lever 2 (architectural).

---

## 3. Corrected next steps (supersedes session-2 §6 item 1)

1. **Fuse the residual eval+apply OR delegate to a real `FilterExec`-equivalent.** The win is
   replacing the two-pass (`on_batch_mask` BooleanArray → `filter_record_batch`) with arrow's
   fused `filter`/`FilterBuilder` (which DataFusion's `FilterExec` uses, and which optimizes the
   predicate-column gather). Caching the expr (done) was a prerequisite cleanup but not the win.
   Measure: does `on_batch_mask_time + filter_record_batch_time` drop toward vanilla's 15 ms?
   — Look at `arrow::compute::FilterBuilder::optimize()` (amortizes the filter plan across columns).
2. **Lever 2 (per-RG construction, ≈ +9 ms): one DataSourceExec over candidate RGs.** Bigger,
   architectural; removes the ×110 metadata/open. Session-2 §5 "big lever". This + #1 would
   close most of the 60 ms.
3. q07 stats short-circuit for min/max — still the biggest single suite win.
4. Pushdown gate on `min_skip_run==1`; q28 indexed-only 500; EventTime-sort cluster — unchanged.

---

## 4. Environment / state at end of session

- Node restarted on the new release dylib (pid as of 19:11; `-Djava.library.path=…/target/release`).
- Index `clickbench` yellow. Persistent: `route_pure_parquet_through_indexed=true`,
  `concurrent_segment_search.mode=none`, `memory_pool_limit_bytes`, `max_slice_count=2`.
  Transient (RE-APPLIED this session, won't survive restart): `indexed_pushdown_filters=false`,
  `parquet_pushdown_filters=false`, `min_skip_run_default=1024`, `min_target_partitions=1`.
  The transient `route_pure_parquet_through_indexed` override was nulled — persistent `true` governs.
- Uncommitted: all session-1/2 changes PLUS this session's `CachedResidual` in
  `eval_helpers.rs` + `predicate_evaluator.rs` + `single_collector.rs` (+ 2 tests).
  TEMP `api.rs::stream_close` QUERY_PROFILE instrumentation STILL present — remove before landing.
- Methodology gotchas from session 2 (§ top) all still apply — especially: trust the
  single-accumulator profile metrics, not wall; confirm the effective plan via QUERY_PROFILE.
</content>
</invoke>
