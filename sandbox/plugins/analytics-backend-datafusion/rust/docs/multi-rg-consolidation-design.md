# Multi-RG decode consolidation — production rewrite design

## Goal
Replace the indexed stream's **one `DataSourceExec` per row group** with **one
`DataSourceExec` over all candidate RGs in a segment chunk**, recovering the
per-RG setup tax measured at ~33% cold on real archive segments (see memory
`multi-rg-decoder-consolidation-spike`). This matches what vanilla DataFusion 54
does post-#22289 (single `PushDecoderStreamState` driving many RGs).

## The key enabling fact (verified)
A single parquet `DataSourceExec` over a multi-RG `ParquetAccessPlan` delivers
batches that **never span a row-group boundary**, and flushes a (possibly short)
batch at each RG boundary. Evidence:
- arrow-rs 58.3.0 `push_decoder/mod.rs:452-476`: `DecodingRowGroup` pulls from ONE
  RG's `ParquetRecordBatchReader` until `next()==None`, then transitions to
  `ReadingRowGroup` for the next RG. No carry-forward, no cross-RG batch.
- `remaining.rs:81-122`: fresh reader + `selection.split_off(row_count)` per RG.
- DF54 `push_decoder.rs:170-189` (`transition`): returns each batch as-is, no
  coalescing.
- DF54 is push-decoder-only by default: `opener/mod.rs:1244-1257`.

=> Per-RG refinement state can be attributed by tracking CUMULATIVE delivered
rows and detecting boundary crossings.

## Current architecture (per-RG, what we're replacing)
`stream.rs::IndexedStream::poll_inner`:
1. `index_reader.poll_next_row_group()` yields ONE prefetched RG (candidates +
   per-RG `context`/`TreePrefetch`).
2. Builds a `RowSelection` for that RG, calls
   `parquet_bridge::create_row_selection_stream(rg, selection, push)` →
   a NEW `DataSourceExec` + `TaskContext::default()` for that single RG.
3. Drains that stream; each batch goes through `finalize_batch` which calls
   `evaluator.on_batch_mask(rg_state, current_rg_first_row, position_map,
   batch_offset, ...)`. State (`current_rg_context`, `current_position_map`,
   `current_rg_first_row`, `mask_offset`, `batch_offset`) is reset per RG when
   the inner stream returns `None`.
4. Repeat for next RG. The `IndexReader` prefetch overlaps only the Lucene/bitmap
   eval (~6ms), NOT the parquet open/read of the next RG.

## Target architecture (multi-RG, one DataSourceExec)
Two layers change: the bridge (build a multi-RG plan) and the stream (attribute
batches to RGs by cumulative count).

### Bridge changes (`parquet_bridge.rs`)
- DONE (spike): `create_multi_rg_full_scan_stream(config, &[rg_idx])` — sets
  `RowGroupAccess::Scan` on all RGs in one `ParquetAccessPlan`.
- NEW: `create_multi_rg_selection_stream(config, &[(rg_idx, RowSelection)], push)`
  — sets `RowGroupAccess::Selection(sel)` per RG in one access plan. This is the
  real production entry point (candidates → per-RG RowSelection, all in one plan).
  The push-decoder splits the carried selection per RG via `split_off`, so we just
  hand it the concatenation of per-RG selections in RG order.

### Stream changes (`stream.rs::IndexedStream`)
Replace the single `current_stream` (one RG) with a single `current_stream`
spanning the WHOLE chunk, plus an RG-boundary cursor.

New state:
- `chunk_rgs: Vec<RowGroupInfo>` — the RGs in this chunk, in order.
- `rg_contexts: Vec<Box<dyn Any>>` — per-RG `TreePrefetch`/candidate context,
  indexed parallel to `chunk_rgs`. Produced up-front (or pipelined) by the
  evaluator for every RG in the chunk BEFORE the parquet stream starts.
- `rg_position_maps: Vec<PositionMap>` — per-RG, parallel.
- `rg_masks: Vec<Option<BooleanArray>>` — per-RG candidate masks (if needs_row_mask).
- `cur_rg: usize` — index into chunk_rgs of the RG the next delivered batch belongs to.
- `rows_into_cur_rg: usize` — rows of cur_rg delivered so far (the per-RG batch_offset).
- `cur_rg_delivered_total: usize` — RowSelection-selected rows for cur_rg (to know
  when we've crossed into the next RG).

Boundary detection in `finalize_batch`:
- Each delivered batch belongs ENTIRELY to `chunk_rgs[cur_rg]` (verified invariant).
- Use `chunk_rgs[cur_rg].first_row` as `rg_first_row`, `rg_contexts[cur_rg]` as
  `rg_state`, `rg_position_maps[cur_rg]` as the position map, `rows_into_cur_rg`
  as `batch_offset` — exactly today's `on_batch_mask` call, just indexed by cur_rg.
- After consuming the batch: `rows_into_cur_rg += batch_len`. When
  `rows_into_cur_rg == cur_rg_delivered_total` (RG exhausted — the short flush),
  advance `cur_rg += 1`, reset `rows_into_cur_rg = 0`, load next RG's
  selected-total. (Equality holds exactly because batches don't span boundaries.)

### Prefetch model
Today: prefetch overlaps Lucene eval for next RG only. New options:
- Phase 1 (simplest, still gets the 33%): evaluate ALL chunk RGs' candidates
  up-front (or via existing prefetch pipeline) into `rg_contexts`/`rg_masks`,
  build the multi-RG selection, then start ONE parquet stream. Parquet's async
  reader now pipelines RG n+1 IO behind RG n decode WITHIN the single decoder —
  this is the win. Lucene eval is serial-up-front but bounded (~6ms × #RGs, and
  can reuse the existing parallel collector path).
- Phase 2 (A1b, optional): keep Lucene eval pipelined with parquet decode by
  chunking — but measure first; Phase 1 alone captured the 33%.

## Correctness-critical invariants to preserve
1. Batch ∈ single RG (verified). If a future arrow version coalesces, an
   assertion `rows_into_cur_rg + batch_len <= cur_rg_delivered_total` catches it.
2. Pushdown decision (`push`) is per-stream now, not per-RG. Today it's computed
   per-RG from `min_skip_run`/`needs_row_mask`/`forbid_parquet_pushdown`. With one
   stream over many RGs, the decision must be uniform across the chunk. Safe choice:
   take the MOST CONSERVATIVE (push only if every RG would allow it). For the
   BitmapTree path `forbid_parquet_pushdown()==true` (always off) — unaffected.
   For PredicateOnly/SingleCollector, push is allowed when row-granular; mixing
   row-granular and block-granular RGs in one chunk means we either (a) push off
   for the whole chunk, or (b) group RGs by strategy into separate chunks. Start
   with (a); revisit if pushdown matters (it HURT q08, so off is fine there).
3. `___row_id` injection (`row_id_injection.rs`) uses `global_base + rg_first_row`
   — now `global_base + chunk_rgs[cur_rg].first_row`. Same formula, indexed.
4. Dynamic-filter RG pruning (`DynamicRgPruner`): today prunes per-RG at prefetch
   AND poll. With one multi-RG stream, an RG can't be dropped mid-stream once the
   access plan is built. So dynamic pruning must happen BEFORE building the plan
   (drop pruned RGs from `chunk_rgs` + the selection). This LOSES the poll-phase
   backstop (filter tightening during the chunk scan). For q08 (no dynamic filter)
   irrelevant; for TopK, either keep chunks small or accept coarser pruning. This
   is the #22407 `force_per_row_group` tension — documented, not solved here.
5. Metrics: per-RG counters (`rg_processed`, `rows_matched`, etc.) still increment
   per RG at boundary crossings, not per stream.

## Chunk sizing
"Chunk" = set of RGs in one IndexedExec/stream. Today effectively 1 RG.
Bigger chunk = more IO pipelining + amortized setup, but more up-front Lucene
eval before first batch and coarser dynamic pruning. Natural choice: one chunk =
one segment's candidate RGs (q08 segment ≈ 20-110 RGs). Make it a config knob
(`indexed_rg_chunk_size`, default = whole segment) so we can A/B.

## Rollout
1. Land bridge `create_multi_rg_selection_stream` (+ keep single-RG for fallback).
2. Add chunked stream behind a flag `indexed_multi_rg_decode` (default OFF).
3. Run cb_harness.py q08 A/B (flag off vs on) on the archive node, single-partition
   (`concurrent_segment_search.mode=none` + `min_target_partitions=1`).
4. Verify byte-exact correctness across all 43 queries (the harness checks this).
5. If clean + faster, flip default ON.

## Files touched
- `parquet_bridge.rs`: + `create_multi_rg_selection_stream` (done: multi_rg_full_scan).
- `stream.rs`: `IndexedStream` state + `poll_inner` + `finalize_batch` RG cursor.
- `eval/mod.rs`: possibly a batch API to produce all-RG contexts up-front (or reuse
  prefetch_rg in a loop).
- `table_provider.rs` / exec construction: pass chunk RGs instead of 1.
- query config: `indexed_multi_rg_decode` flag + `indexed_rg_chunk_size`.
