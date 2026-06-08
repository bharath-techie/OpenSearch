# One-decoder, RG-by-RG indexed scan via push-decoder into_builder — full design

Status: DESIGN ONLY (no code). Supersedes the all-up-front multi-RG approach in
`multi-rg-consolidation-design.md`, which is ABANDONED (see §0).

### ⚠️ NO FORK NEEDED — the API is UPSTREAM and RELEASED (key update)
The per-RG-reconfigurable push decoder (`into_builder`/`is_at_row_group_boundary`/
`row_groups_remaining`/`try_next_reader`) landed via **apache/arrow-rs PR #9968**
(merged into main 2026-05-20, commit 7c6eb2c) and is in the **RELEASED arrow-rs
59.0.0** tag (verified: all four methods present at push_decoder/mod.rs:463/531/537/591).
It is NOT in 58.3.0 (our current pin — verified absent).
PR #9968 superseded the earlier awkward `swap_strategy(StrategySwap)` design
(`can_swap_strategy` was renamed to `is_at_row_group_boundary`); pydantic's
`adaptive-strategy-swap` branch was just the staging ground that became #9968.

THE DEPENDENCY CONSTRAINT (the real cost now):
- arrow-rs 59.0.0 (released) HAS the API. ✓
- DataFusion 54.0.0 (released, our pin) wants arrow 58.3.0. ✗  (no released DF on 59)
- DataFusion **main** (in-dev, still labelled 54.0.0) ALREADY uses arrow 59.0.0. ✓
⇒ To get the API we must move from DF 54.0.0(crates.io)+arrow58.3.0 to
  DF main + arrow 59.0.0. That's an arrow major bump (58→59) AND a DF
  crates.io→git-main move. See §6 (new) for the upgrade cost/risk.

All findings below are read from real reference code:
- apache/arrow-rs 59.0.0 release tag — push_decoder/mod.rs, remaining.rs, reader_builder/mod.rs
  (the pydantic branch @ a92f5e1 was byte-identical to what shipped — our reading holds).
- adriangb/datafusion PR #22237 (`AdaptiveParquetStream`) — the CONSUMER pattern to port,
  head e3f7f3e (note: it pins the pre-#9968 swap_strategy API; the loop shape still applies).

────────────────────────────────────────────────────────────────────────
## 0. The problem this solves (and why the shipped multi-RG code is wrong)
────────────────────────────────────────────────────────────────────────

Measured on 3.7.0-ARCHIVE q08 (100M rows, 20 segments / 110 RGs, single-partition),
8-iter medians, both indexed (route_pure_parquet ON):
- Per-RG (today's default):     **229ms**
- All-up-front multi-RG (mine):  **162ms**  (~1.4x)

The 1.4x came ENTIRELY from amortizing parquet decoder setup:
`parquet_first_poll_time` 107.6ms (110× lazy decoder build) → 0.4ms (built once).
Same bytes_scanned (1.14M), same rows, same memory — proven not I/O, not memory.

BUT the shipped multi-RG path (`poll_build_staged` in stream.rs) drains EVERY RG's
candidate/Lucene eval BEFORE opening the single `DataSourceExec`. For q08 the
"eval" is just cheap page-stats so it's invisible. For a real `index_filter`
(Lucene collector) query over N RGs it would run ALL N Lucene searches serially
before the first row, and DESTROY the per-RG prefetch overlap (RG n decode hiding
RG n+1 Lucene) that the `IndexReader` was built for. That is a regression for
exactly the queries the indexed path exists to serve. → ABANDON all-up-front.

ROOT CAUSE of the forced up-front: a single DataFusion `DataSourceExec` is built
from ONE complete `ParquetAccessPlan` (every RG's RowSelection) BEFORE it opens.
Each RG's RowSelection is the output of that RG's Lucene eval. So:
  one DataSourceExec ⟸ full access plan ⟸ all RG selections ⟸ all Lucene first.
There is NO way around this on stock `DataSourceExec` / stock arrow-rs, because
stock `RemainingRowGroups::new` takes the full `selection: Option<RowSelection>`
once at construction.

────────────────────────────────────────────────────────────────────────
## 1. What adriangb's PR proves (the design we want)
────────────────────────────────────────────────────────────────────────

`AdaptiveParquetStream::transition` (adrian_opener.rs ~line 1949) drives ONE
`ParquetPushDecoder` ROW GROUP AT A TIME, reconfiguring filter/projection at each
RG boundary — NOT up-front. Verbatim doc comment:
  "Advances the state machine ... Drives one row group at a time, swapping filter
   strategy at row-group boundaries."

Loop shape (paraphrased, exact in adrian_opener.rs:1949-2089):
```
loop {
  if remaining_limit == Some(0) { return None }
  // Step 1: ensure a reader for the CURRENT row group
  if active_reader.is_none() {
     if pushdown_filters && current_run_needs_filter {
         maybe_swap_strategy()?;          // ← re-place filters for the NEXT rg, lazily
     }
     loop {                               // pull next reader, fetching bytes as needed
        match decoder.try_next_reader() {
           NeedsData(ranges) => { data = reader.get_byte_ranges(ranges).await; decoder.push_ranges(...) }
           Data(reader)      => { active_reader = Some(reader); break }
           Finished          => { if let Some(next)=pending_runs.pop_front() {swap run; continue} return None }
        }
     }
  }
  // Step 2: pull next batch from active reader (sync — bytes already pushed)
  match active_reader.next() {
     Some(Ok(batch)) => { /* step 3 */ }
     None            => { active_reader = None; continue }   // rg exhausted → back to step 1
  }
  // Step 3: post-scan filters + projector + schema replace + limit → return batch
}
```

KEY: `maybe_swap_strategy()` runs at EVERY RG boundary (when `active_reader` is
None and the run needs a filter), reconfiguring the decoder for the row groups
STILL TO COME. The bytes for one RG are pushed before its reader is built, so
`active_reader.next()` is synchronous; the await is only on `get_byte_ranges`.

Their `maybe_swap_strategy` (adrian_opener.rs:2108) builds a fresh `RowFilter`
from the conjuncts and calls the forked `decoder.swap_strategy(StrategySwap)`.
That is the *adaptive-pushdown* use; OUR use is simpler — we don't re-place
filters by selectivity, we just feed the NEXT RG's Lucene-derived RowSelection.

────────────────────────────────────────────────────────────────────────
## 2. The forked arrow-rs API (pydantic/arrow-rs adaptive-strategy-swap @ a92f5e1)
────────────────────────────────────────────────────────────────────────

NOTE: the PR pins an OLDER commit of this branch that exposed `swap_strategy(StrategySwap)`
+ `can_swap_strategy()`. The branch HEAD (a92f5e1) has REPLACED that with a cleaner
`into_builder()` API. Document both; RECOMMEND `into_builder` (it's what ships now).

### 2a. ParquetPushDecoder public surface (pyd push_decoder/mod.rs)
- `try_next_reader() -> DecodeResult<ParquetRecordBatchReader>`  (mod.rs:463)
    Returns the next RG's reader, or NeedsData(ranges), or Finished. ONE rg at a time.
- `try_decode() -> DecodeResult<RecordBatch>`  (batch-granular variant)
- `push_ranges(ranges, data)` / `push_range(...)`  (mod.rs:488) — feed fetched bytes
- `buffered_bytes() -> u64`  (mod.rs:508)
- `clear_all_ranges()`  (mod.rs:517)
- `is_at_row_group_boundary() -> bool`  (mod.rs:531)
    True "between row groups": previous RG's reader fully extracted/drained and next
    not yet planned. False while iterating an active reader.
- `row_groups_remaining() -> usize`  (mod.rs:537) — RGs left after the in-flight one
- `into_builder() -> Result<ParquetPushDecoderBuilder>`  (mod.rs:591) ← THE HOOK
    "Decompose this decoder back into a builder for the row groups NOT yet decoded."
    Must be at an RG boundary. The returned builder pins not-yet-decoded RGs (via
    with_row_groups), carries not-yet-consumed RowSelection + offset/limit budget, so
    already-decoded RGs aren't re-produced. Buffered bytes carry across the rebuild
    (bytes for RGs the new config still reads are NOT re-fetched). Every option
    (projection, row_filter, row_selection_policy, batch_size, metrics, predicate
    cache) is left as-is and can be overridden before `.build()`.

### 2b. Canonical adaptive loop (from mod.rs:551-567 doc):
```rust
let mut decoder = builder.build()?;
loop {
  match decoder.try_next_reader()? {
    DecodeResult::NeedsData(ranges) => { let d = fetch(ranges); decoder.push_ranges(ranges, d)?; }
    DecodeResult::Data(reader) => {
        for batch in reader { /* consume this RG's batches */ }
        if decoder.is_at_row_group_boundary() && decoder.row_groups_remaining() > 0 {
            decoder = decoder.into_builder()?
                .with_row_filter(new_filter_for_remaining_rgs())   // or .with_row_selection(...)
                .build()?;
        }
    }
    DecodeResult::Finished => break,
  }
}
```

### 2c. Internal state machine (pyd push_decoder/mod.rs:596-)
`ParquetDecoderState` = ReadingRowGroup{remaining} | DecodingRowGroup{reader, remaining} | Finished.
`try_next_reader` transitions until it yields a reader (Data) or NeedsData/Finished.
ONE `RowGroupReaderBuilder` is reused across all RGs (the amortized-setup win).

### 2d. RemainingRowGroups (pyd remaining.rs) — how "resume from next RG" works
- `RowGroupFrontier` owns cross-RG state: `row_groups: VecDeque<usize>`,
  `selection: Option<RowSelection>` (a CURSOR — `split_off(row_count)` per RG),
  `budget: RowBudget` (offset/limit), `has_predicates: bool`.
- `next_readable_row_group()` (remaining.rs:126): pops the next RG, slices the
  global selection via `selection.split_off(row_count)`, skips fully-deselected RGs,
  applies offset/limit budget, returns `NextRowGroup{idx,row_count,selection,budget}`.
- `into_parts() -> RemainingRowGroupsParts` (remaining.rs:250): at a boundary, hands
  back {schema, metadata, row_groups (NOT yet decoded), selection (NOT yet consumed),
  offset, limit, reader_builder parts}. This is what `into_builder()` rebuilds from.
- `is_at_row_group_boundary()` = inner builder is in `Finished` state (remaining.rs:292).

CRITICAL CONSEQUENCE: with `into_builder`, you give the decoder ONLY the RGs and a
selection at build time, but you can REBUILD at each boundary to supply the NEXT
RG's selection/filter. So you can feed selections INCREMENTALLY — exactly what we
need — without ever computing all of them up front.

────────────────────────────────────────────────────────────────────────
## 3. Our design — IndexedStream driving one decoder, one Lucene per RG, overlapped
────────────────────────────────────────────────────────────────────────

GOAL: one long-lived `ParquetPushDecoder` per segment chunk (setup amortized once),
fed RG-by-RG with each RG's Lucene-derived `RowSelection` JUST BEFORE that RG decodes,
while the PREVIOUS RG's decode hides the NEXT RG's Lucene eval (preserve today's
`IndexReader` prefetch overlap). No up-front staging.

### 3a. Two ways to feed per-RG selection (pick one)

OPTION A — rebuild-per-RG via `into_builder` (simplest, matches shipped API):
Build the decoder for ONE RG at a time. At each boundary:
  1. decoder yields current RG's reader → we decode + apply on_batch_mask.
  2. at boundary, `into_builder()` → `.with_row_groups([next_rg])`
     `.with_row_selection(next_rg_selection)` → `.build()`.
But `with_row_groups` on the rebuilt builder pins "not-yet-decoded RGs" — we'd
override it to just the next one. PROBLEM: rebuilding every RG re-runs builder
setup → we LOSE the amortization that was the whole point. ✗ Not this.

OPTION B — build once over ALL chunk RGs with a selection we supply lazily:
The decoder is built ONCE with the full `row_groups: Vec<usize>` for the chunk, but
the `selection` must be known up front in the current fork API (RowGroupFrontier
takes the whole `Option<RowSelection>` at `new`). That is the up-front trap again
UNLESS we extend the fork (3c).

OPTION C (RECOMMENDED) — build once, feed selection per-RG via a NEW fork hook:
Add to the fork a way to supply the next RG's RowSelection at the boundary, e.g.
`decoder.set_next_row_group_selection(RowSelection)` that `RowGroupFrontier`
consumes in `next_readable_row_group` instead of slicing a pre-set global selection.
Then:
```
build decoder ONCE with row_groups = all chunk RGs, selection = None (lazy mode)
loop {
  match decoder.try_next_reader() {
    NeedsData(ranges) => fetch + push    // parquet asks only for the planned RG's bytes
    Data(reader)      => decode this RG's batches, apply on_batch_mask (our refinement)
    Finished          => done
  }
  // at boundary, BEFORE the next try_next_reader plans the next RG:
  if at_boundary && rgs_remaining > 0 {
     // the IndexReader has ALREADY prefetched the next RG's candidates during
     // THIS RG's decode (existing overlap). Convert to RowSelection and hand it in:
     decoder.set_next_row_group_selection(next_rg_selection);
  }
}
```
This keeps ONE builder (amortized setup) AND feeds selection lazily AND overlaps
Lucene(n+1) with decode(n). It is the minimal fork: a per-RG selection setter,
NOT the full StrategySwap/adaptive-pushdown machinery.

### 3b. How the IndexReader prefetch overlap is preserved
Today `IndexReader` runs `evaluator.prefetch_rg(rg+1)` in a spawn_blocking task
while the current RG decodes. In Option C, we keep that EXACTLY: while
`decoder.try_decode()`/reader iteration drains RG n's batches, the background task
computes RG n+1's candidate bitmap. At RG n's boundary we pull the (already-ready)
RG n+1 candidates, build its RowSelection (build_rg_plan), and
`set_next_row_group_selection`. The await-on-Lucene only happens if decode out-runs
Lucene — same as today. ZERO up-front staging.

### 3c. The fork change (minimal) — pydantic-style, on our arrow-rs 58.3.0
Add to `RowGroupFrontier` / `RemainingRowGroups` / `ParquetPushDecoder`:
- A "lazy selection" mode: `selection` starts None and `next_readable_row_group`
  does NOT slice a global selection; instead it takes a per-RG selection supplied
  via a new setter, defaulting to "select all" if none supplied.
- `ParquetPushDecoder::set_next_row_group_selection(RowSelection)` (and/or
  `set_next_row_group_row_filter(RowFilter)` if we ever push residual into parquet).
- Guarded to a boundary (`is_at_row_group_boundary()`), like `into_builder`.
ALTERNATIVELY: just adopt the pydantic branch wholesale via `[patch.crates-io]`
(it already has `into_builder` + boundary introspection) and use a thin
rebuild-with-shared-builder-parts path. Decide after vetting whether
`into_builder().with_row_groups(remaining).with_row_selection(next)` actually
re-pays builder setup or reuses `reader_builder` parts (remaining.rs:271
`row_group_reader_builder.into_parts()` suggests the inner builder parts CARRY
OVER — if so, into_builder may NOT re-pay setup, making Option A viable after all).
  → OPEN QUESTION to resolve by reading RowGroupReaderBuilder::into_parts /
    from_parts + ArrowReaderBuilder::build cost. THIS DECIDES A vs C.

### 3d. Bypassing DataSourceExec
We stop using `parquet_bridge::create_*_stream` (which wrap DataSourceExec) for the
consolidated path. Instead IndexedStream owns a `ParquetPushDecoder` built via
`ParquetPushDecoderBuilder::new_with_metadata(arrow_reader_metadata)` +
`.with_projection(mask)` + `.with_row_groups(chunk_rgs)` + batch size + metrics, and
drives it in `poll_inner` with our existing `CachedMetadataReader` serving
`get_byte_ranges` (we already have an AsyncFileReader — parquet_bridge.rs:224). The
existing finalize_batch / on_batch_mask / PositionMap / row-id logic is UNCHANGED;
it just consumes batches from the decoder instead of from a DataSourceExec stream.

### 3e. Correctness invariants carried over (unchanged from per-RG)
- Batch never spans an RG boundary (one ParquetRecordBatchReader per RG, short flush)
  → per-RG on_batch_mask state attribution stays valid.
- Each RG's selection covers the full RG row count (select+skip == num_rows).
- Pushdown OFF for the consolidated chunk (residual applied post-decode via on_batch_mask).
- Gated to NON-dynamic-filter queries (TopK/join keep per-RG path for mid-scan prune).
- emit_row_ids: base = global_base + current_rg.first_row, advanced per RG (same).

────────────────────────────────────────────────────────────────────────
## 4. Open questions to resolve BEFORE coding (must-answer)
────────────────────────────────────────────────────────────────────────
1. [RESOLVED — read pyd reader_builder/mod.rs:282-351]
   Q: Does `into_builder().build()` re-pay the per-RG setup that the old path
   paid 110×?  A: NO. `RowGroupReaderBuilderParts` (line 282) carries across a
   rebuild: batch_size, projection, `fields` (the parsed ParquetField schema map —
   the expensive bit), filter, max_predicate_cache_size, metrics,
   row_selection_policy, AND `buffers` (already-fetched bytes, NOT re-requested).
   The per-RG runtime `state` (array readers / column chunk data) is DISCARDED
   (`state: _`, line 338) — but that array-reader build (try_build → ArrayReaderBuilder
   ::build_array_reader, lines 610/789) happens once PER RG in BOTH stock and fork;
   it is inherent to decoding a row group, NOT the 110× overhead we removed.
   The 107ms the old per-RG path wasted was the LAYER ABOVE: a fresh DataSourceExec
   + TaskContext::default() + ArrowReaderMetadata derive + builder build PER RG.
   `into_builder` reassembles only the cheap builder struct and KEEPS `fields`+`buffers`.
   ⇒ CONCLUSION: Option A (rebuild via into_builder at each boundary, overriding
   row_groups=[next] + row_selection=next) PRESERVES the amortization. It is viable
   and is the SIMPLEST path (uses the shipped fork API, no new fork method needed).
   Option C (new lazy-selection setter) is only worth it if profiling shows the
   per-boundary into_builder/build reassembly is measurable vs decode — unlikely
   given it's struct moves + an Arc clone of `fields`. START WITH OPTION A.
2. Does the pydantic branch compile against OUR exact pins (arrow 58.3.0, the other
   datafusion-* crates at 54.0.0)? The branch base is 58.3.0 — but DF 54 expects
   stock parquet 58.3.0 API; the fork ADDS methods (compatible) but check it doesn't
   REMOVE/rename anything DF 54 datasource-parquet calls.
3. Does driving ParquetPushDecoder directly (bypassing DataSourceExec) lose any
   metrics/behavior we rely on (ParquetFileMetrics, predicate cache, page index)?
   We set with_enable_page_index(false) already; confirm parity.
4. Encryption / object-store path: CachedMetadataReader already serves get_byte_ranges
   — confirm the push decoder's NeedsData ranges are byte ranges it can serve directly.

────────────────────────────────────────────────────────────────────────
## 5. Files / artifacts
────────────────────────────────────────────────────────────────────────
Reference copies pulled to /tmp (this session): adrian_opener.rs (3728L, has
AdaptiveParquetStream + ParquetMorselizer + maybe_swap_strategy), adrian_source.rs,
adrian_selectivity.rs, adrian_row_filter.rs, adrian_row_group_filter.rs,
adrian_metrics.rs, pyd_mod2.rs (push_decoder/mod.rs @ a92f5e1), pyd_remaining.rs,
pyd_rb_mod.rs (reader_builder/mod.rs).
Our current (to be reworked): stream.rs (poll_build_staged + StagedRg = the
all-up-front approach to REPLACE), parquet_bridge.rs (create_multi_rg_* = DataSourceExec
wrappers, bypassed by the new path), datafusion_query_config.rs (env flag, reused).

────────────────────────────────────────────────────────────────────────
## 6. The real decision now: arrow 58.3→59 + DF 54-crates.io→main upgrade
────────────────────────────────────────────────────────────────────────
The push-decoder API is no longer the blocker — the dependency bump is. Two routes:

ROUTE 1 — Upgrade the whole workspace to DF main + arrow 59.0.0.
  PROS: get into_builder from a RELEASED arrow (59.0.0) + maintained DF main; no fork
  to carry; aligns with where upstream is going.
  CONS / RISK (must scope before committing):
   - arrow 58→59 is a MAJOR bump: API breakages across arrow-array/arrow-schema/
     parquet that our code AND datafusion-substrait/datasource use. Need a compile
     pass to enumerate breakage.
   - DF crates.io 54.0.0 → DF git main: main is unreleased, version still "54.0.0"
     but APIs drift daily; we'd pin a git rev. Our memory notes already flag DF API
     drift between 53.1.0 and the dev checkout (SimplifyContext::builder etc).
   - Re-validate ALL the indexed_table work (substrait nullability fix, dynamic
     filters, the 427 tests) against the new arrow/DF.
   - The Java FFM/ABI surface: arrow C-data-interface (ffi feature) version must match
     what the Java side (arrow-java) speaks. 58→59 arrow C ABI is stable but VERIFY.

ROUTE 2 — Backport ONLY the push-decoder into_builder API onto a local arrow 58.3.0.
  i.e. cherry-pick arrow-rs #9968 (+ its prereqs) onto the 58.3.0 tag we already have
  checked out at ~/Documents/dev/arrow-rs, pin via [patch.crates-io] path=...
  PROS: NO workspace-wide arrow/DF bump; DF stays 54.0.0+arrow58.3.0; minimal blast
  radius; we control exactly what changes.
  CONS: it IS a local arrow patch (a "fork" in the carry-a-patch sense, though it's
  just upstream commits backported, not novel code); must rebase if we bump later;
  #9968 may depend on other 59-only push-decoder refactors (the remaining.rs rewrite
  with RowGroupFrontier/RowBudget is substantial — check #9968's prerequisite PRs).
  → SCOPE: read #9968's diff + its base; if it's self-contained on 58.3.0's
    push_decoder it's a clean backport; if it needs the whole 59 push_decoder
    rewrite, Route 1 is less work.

RECOMMENDATION: decide by measuring blast radius:
  (a) `git -C ~/Documents/dev/arrow-rs log 58.3.0..59.0.0 -- parquet/src/arrow/push_decoder`
      → how many commits/how invasive is the 58→59 push_decoder delta (Route 2 size).
  (b) Trial: bump our workspace to arrow 59.0.0 + DF git main rev, `cargo build`, count
      breakages (Route 1 size).
  Whichever is smaller wins. Until then, the per-RG path stays the default; the
  all-up-front multi-RG code is reverted/abandoned (it's a regression for collector queries).
