# Step 2/3: Row-Group Scoping the Scoped Page Index — Design Draft (OPEN for review)

**Status:** DRAFT — research + design complete, initial code sketch only. NOT wired
into the live paths. Built on the verified Step 1 (commits up to `f65d3fbea68`).

This is the "tricky" step the user flagged. The design below is grounded in a
close read of DataFusion 54.0.0 + parquet 58.1.0 source (citations inline), not
guesswork. **No hacks**: it relies only on documented arrow-rs/DF behavior.

---

## 0. What Step 1 already does (baseline)

The scoped cache (`indexed_table::page_index_loader`) builds, per `(file, predicate
parquet-cols)`:
- `ColumnIndex`: real for predicate columns, `NONE` placeholder elsewhere — **all
  row groups**.
- `OffsetIndex`: real for **all columns, all row groups**.

Key = `(path, parquet_cols)`. Both scan paths build the identical artifact, so
entries are shared (verified on 100M-doc clickbench: listing then indexed `match`
hits the same 20 entries).

The heap hog is the **ColumnIndex** (per-page *string* min/max). Step 1 already
scopes it by COLUMN. Step 2 scopes it by ROW GROUP too — decode predicate-column
ColumnIndex only for row groups that can match, not all of them.

---

## 1. The hard constraints (from DF54 + parquet58 source)

### 1a. Page index is loaded BEFORE DataFusion's RG pruning
`opener/mod.rs` state machine order:
`PrepareFilters → LoadPageIndex (mod.rs:387-389) → PruneWithStatistics
(RowGroupAccessPlanFilter, mod.rs:896-916) → LoadBloomFilters → PruneWithBloomFilters
→ row_groups.build() (mod.rs:1097) → PAGE PRUNE (mod.rs:1102-1114)`.

⇒ **We do not know DataFusion's surviving-RG set at index-build time.** The page
index must already cover whatever RGs survive DF's stats+bloom pruning.

### 1b. RowGroupAccessPlanFilter uses NO page index
`row_group_filter.rs` `RowGroupPruningStatistics` uses only footer RG stats +
bloom filters (no `column_index`/`offset_index` refs anywhere). So DF's surviving
set is a function of **footer RG stats + bloom filters + the predicate** — inputs
we also have (minus bloom, which only prunes MORE).

### 1c. Page pruner iterates DF's surviving set, not all RGs
`page_filter.rs:240-242`: `for row_group_index in access_plan.row_group_indexes()`.
Per-file gate `page_filter.rs:217-226`: if `offset_index().is_none() ||
column_index().is_none()` → skip page pruning for the whole file. A `Some(vec)`
with placeholder contents PASSES this gate.

### 1d. NONE ColumnIndex is SAFE (graceful)
`statistics.rs:603-621` `get_data_page_statistics!`: a `ColumnIndexMetaData::NONE`
hits the `_` arm → `append_nulls(len)` → "no usable stats, keep all pages". No
panic. **BUT** `len = column_offset_index[rg][col].page_locations().len()`
(statistics.rs:1719-1721) — so the **OffsetIndex for that rg/col must still be
real and correct** for the NONE arm to emit a correctly-sized null array.

### 1e. Empty OffsetIndex is NOT SAFE for any scanned RG
- Prune time: `data_page_row_counts` panics at `statistics.rs:1835`
  (`page_locations.last().unwrap()`) if a scanned column's `page_locations` is empty.
- Read time: `in_memory_row_group.rs:88-103` — with a RowSelection active,
  `selection.scan_ranges(&offset_index[idx].page_locations)` returns zero ranges →
  `arrow_reader/mod.rs:1383/1449` "failed to skip rows, expected N got 0".
- A *missing* inner column (`.get(col) == None`) degrades gracefully
  (`PagesPruningStatistics::try_new` returns None, page_filter.rs:508-520), but a
  *present-but-empty* `page_locations` panics.

### Conclusion
| Index | RG-scopable? | Why |
|---|---|---|
| **ColumnIndex** (predicate cols) | **YES** — NONE on non-surviving RGs | NONE is graceful; only loses pruning on RGs we placeholder (and we only placeholder RGs that can't match anyway) |
| **OffsetIndex** (all cols) | **NO** (must stay all-RG) | empty placeholder panics / breaks reads on any RG DF scans, and we can't know DF's set first |

So **Step 2 = RG-scope the ColumnIndex; keep OffsetIndex all-RG.** This is the
real, safe win: the ColumnIndex is the heap hog. Step 3 (RG-scope the OffsetIndex)
is **probably not safely achievable** on the listing path (see §4) — likely we
stop at Step 2 and explicitly document why OffsetIndex stays all-RG.

---

## 2. Which RG set do we scope the ColumnIndex to?

We can't read DF's survivor set, but we can **recompute a superset** of it:

> `surviving_rgs(file, predicate)` = the row groups that pass **footer RG-stats
> pruning** for the predicate.

- We build the predicate-column ColumnIndex only for `surviving_rgs`; `NONE`
  elsewhere.
- DF's actual scanned set = footer-stats pruning **∩ bloom pruning ∩ range/limit**
  ⊆ our footer-stats set. So **every RG DF scans has a real ColumnIndex** → full
  page-pruning power, zero degradation.
- RGs we placeholdered (`NONE`) are exactly the ones DF also prunes by footer
  stats → DF never looks at them at page-prune time anyway → the NONE is never
  even dereferenced. And if DF *did* look (it won't, since stats agree), NONE is
  still graceful (§1d) because we keep the real OffsetIndex for all RGs.

**Cross-path sharing preserved:** both paths compute `surviving_rgs` from the
SAME footer stats + the SAME predicate ⇒ identical set ⇒ identical key+artifact.
The key gains a `surviving_rgs` component, but it is a deterministic function of
`(file, predicate)`, so it does not diverge by path (this is precisely what the
prior rejected iteration got wrong — it keyed on path-specific RG sets).

### Computing footer-stats survivors
DataFusion exposes `PruningPredicate` + `RowGroupAccessPlanFilter`, but the
cleanest reusable primitive is the existing `build_pruning_predicate` +
`PruningPredicate::prune` over a `RowGroupPruningStatistics`-equivalent. The
indexed path ALREADY computes this: `StatsPruneTree.rg_can_match: Vec<bool>`
(`page_pruner.rs:642`, built `table_provider.rs:649`). The listing path would need
the same footer-stats prune. Options:
- (a) Reuse `PruningPredicate` against footer RG stats (a small wrapper around
  `StatisticsConverter::row_group_mins/maxes`), computing `Vec<bool>` per RG.
- (b) Only RG-scope on the indexed path (where `rg_can_match` already exists) and
  leave the listing path all-RG. **Rejected** — breaks the "same artifact both
  paths" rule and re-introduces key divergence.

⇒ Implement (a): a shared `surviving_row_groups(footer_meta, arrow_schema,
predicate) -> Vec<usize>` used by BOTH paths before `load_scoped_page_index`.

---

## 3. Proposed API changes (incremental, key stays unified)

```rust
// page_index_loader.rs

/// RGs that pass footer RG-stats pruning for `predicate`. Superset of DataFusion's
/// scanned set (which further applies bloom/range/limit). Deterministic in
/// (file, predicate) so both scan paths agree.
pub fn surviving_row_groups(
    footer_meta: &ParquetMetaData,
    arrow_schema: &SchemaRef,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Vec<usize>;

/// Like load_scoped_page_index, but the predicate-column ColumnIndex is built
/// ONLY for `surviving_rgs` (NONE elsewhere). OffsetIndex stays real for ALL RGs
/// (correctness — see §1e). `surviving_rgs` becomes part of the cache key.
pub async fn load_scoped_page_index_rgs(
    store, location, footer_meta, parquet_cols,
    surviving_rgs: &[usize],   // sorted/deduped; from surviving_row_groups()
) -> Option<Arc<ParquetMetaData>>;
```

Cache key grows to `(path, parquet_cols, surviving_rgs)`. Because `surviving_rgs`
is derived identically on both paths, sharing holds. `load_scoped_page_index`
(Step 1, all-RG) stays as the fallback when no predicate stats are prunable.

`build_scoped_page_index` change: the `read_rg: Vec<bool>` gate already exists in
the Step-1-era reference (`build_augmented_metadata` had a `surviving_rgs` param);
re-introduce it but ONLY for the ColumnIndex fetch/scatter. The OffsetIndex fetch
stays over all RGs. (Step 1's current code fetches both for all RGs — the diff is:
skip the ColumnIndex range-read for non-surviving RGs, leave NONE.)

Size estimate: `scoped_page_index_size` already sums per-RG; restrict the
ColumnIndex term to `surviving_rgs`, keep the OffsetIndex term over all RGs.

---

## 3b. OffsetIndex COLUMN-scoping (better than RG-scoping it) — DRAFTED & safe

User question: "do we need the OffsetIndex for ALL columns, or just projection +
predicate?" Answer from the source: **just `predicate ∪ projection ∪ {0}`.** The
OffsetIndex is dereferenced in exactly three places (DF54 / parquet58):

| Consumer | Columns | Source |
|---|---|---|
| Read (`InMemoryRowGroup::fetch_ranges`) | **projected** only (`projection.leaf_included(idx)`) | `in_memory_row_group.rs:80-81` |
| Prune (`PagesPruningStatistics::try_new`) | the **predicate** column only (`offset_index[rg][parquet_column_index]`) | `page_filter.rs:512-519` |
| Metric (`total_pages_in_group`) | **column 0** (`offset_index[rg].first()`) — feeds only the page-skip COUNTER, not the produced RowSelection | `page_filter.rs:255-260, 359-361` |

So: real OffsetIndex needed for `predicate ∪ projection`; add **column 0** to keep
the page-skip metric identical to stock DF (stock DF also keys it off col 0). Any
other column gets an empty placeholder — never projected, never predicate, never
0, so never dereferenced. This is column-scoping, orthogonal to RG-scoping, and is
the bigger win on the 402-col textbench schema (project a handful → tiny offset set).

**Implemented (DRAFT, not wired):**
- `ScopedKey` gains `offset_cols` (empty = all columns / Step 1).
- `load_scoped_page_index_cols(.., offset_cols)`: the fn defensively unions in
  predicate cols + col 0, clamps/sorts/dedups; if the union covers all columns it
  COLLAPSES to the empty "all columns" key (shares the Step-1 entry).
- `build_scoped_page_index` decodes the OffsetIndex only for `off_cols`, scatters
  to absolute positions, empty placeholders elsewhere; size estimate follows.
- 4 unit tests: offset real only for the scoped set; projected non-predicate read
  works; bytes reduced; full-coverage collapse.

**Wiring (TODO, same projection-availability split as RG-scoping):**
- Listing path: projection is known in the optimizer (`config.projected_schema()` /
  FileScanConfig) → resolve to parquet leaf indices, pass to
  `load_scoped_page_index_cols`. Easy.
- Indexed path: projection (`read_projection = output ∪ predicate_columns`) is
  known in `scan()`, AFTER the `indexed_executor` augmentation site. Thread it to
  the augmentation site, OR keep the indexed path on all-columns OffsetIndex (call
  `load_scoped_page_index`) until threaded. Cross-path sharing requires BOTH paths
  to pass the SAME `offset_cols` for the same query — so wire both together, or
  accept divergent keys (separate entries) until both are wired.

This combines with RG-scoping: a future `load_scoped_page_index_scoped(parquet_cols,
surviving_rgs, offset_cols)` would do both at once (CI RG-scoped, OI col-scoped).

## 4. Step 3 (RG-scope the OffsetIndex) — likely NOT safe; decision pending

To RG-scope the OffsetIndex we'd need every RG with an empty OffsetIndex to be
guaranteed-unscanned by DF. We can't guarantee that on the **listing** path: DF
owns the access plan and decides scanned RGs after the index is loaded; even if
our footer-stats survivor set ⊇ DF's, the RGs we *omit* are ones DF also prunes —
so in theory an omitted RG is never scanned. **But** the panic in §1e fires at
prune time (`data_page_row_counts`) for any RG in `access_plan.row_group_indexes()`,
and that set = our-superset minus bloom/range. The omitted RGs are not in it, so
they're never iterated... which suggests OffsetIndex RG-scoping to the SAME
`surviving_rgs` set might actually be safe (the omitted RGs are exactly the
footer-pruned ones DF also drops before page-pruning AND before read).

**This needs a dedicated test to confirm**, because it hinges on "DF's
access-plan RG set ⊆ our footer-stats survivor set" being airtight including the
`prune_by_range`/`prune_by_limit`/initial-plan edge cases (mod.rs:896-916,
1090-1097). If confirmed, Step 3 = scope BOTH indexes to `surviving_rgs` (drop the
"OffsetIndex all-RG" special-case). If not, Step 2 is the final state.

**Recommendation:** ship Step 2 (ColumnIndex RG-scoped, OffsetIndex all-RG) which
is unambiguously safe and captures the heap win. Treat Step 3 as a separate,
test-gated follow-up with an adversarial "DF scans an RG we omitted" repro.

---

## 5. Test plan (Rust unit + stats-backed IT)

Rust (`page_index_loader` tests):
1. `surviving_row_groups` matches a known footer-stats prune on a 4-RG fixture
   (`four_rg_parquet`): `id >= 25` keeps only the RGs whose min/max overlap.
2. `load_scoped_page_index_rgs`: ColumnIndex real on surviving RGs, `NONE` on
   pruned RGs; OffsetIndex real on ALL RGs (the §1e safety invariant).
3. Pruning parity: `PagePruner` on the RG-scoped metadata gives the SAME
   RowSelection as the full index, for a surviving RG.
4. **Adversarial (the §4 question):** force a NONE-ColumnIndex RG to be page-pruned
   by DF and assert no panic + correct rows (drive a real `DataSourceExec` like
   `factory_at_construction_executes_through_scoped_reader`).
5. Cross-path key identity: `surviving_row_groups` + `parquet_cols` identical for
   the same predicate regardless of which path computes it.

IT (`ScopedPageIndexCacheIT`, stats-backed, restart for clean baseline):
- A selective predicate (`AdvEngineID = <rare>`) should yield a SMALLER scoped
  `memory_bytes` than a non-selective one (`AdvEngineID > 0`) over the same file
  set — proving RG-scoping shrank the ColumnIndex. (Both correct vs known counts.)
- Re-run = pure hit (entries/bytes flat), as Step 1.
- Cross-path: listing `AdvEngineID=<rare>` then indexed `match(URL,..) AND
  AdvEngineID=<rare>` HITS (same `surviving_rgs` key).

---

## 6. Files to touch (Step 2)
- `rust/src/indexed_table/page_index_loader.rs`: add `surviving_row_groups`,
  `load_scoped_page_index_rgs`; RG-gate the ColumnIndex in `build_scoped_page_index`;
  extend `ScopedKey` with `surviving_rgs`; size estimate.
- `rust/src/shard_scoped_reader.rs`: compute `surviving_row_groups` (needs the
  predicate — already held as `self.predicate`, currently `#[allow(dead_code)]`)
  and call `load_scoped_page_index_rgs`.
- `rust/src/indexed_executor.rs`: same — compute survivors from the bool-tree
  predicate (or reuse `StatsPruneTree.rg_can_match`) and call the `_rgs` variant.
- Tests as in §5.

Keep `load_scoped_page_index` (all-RG) as the no-predicate-stats fallback.
