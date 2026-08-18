# Keyword Doc-Values Design: Tiered SortedSet Ordinals for the Parquet Codec

Status: implemented, uncommitted (pending review) · Scope: `sandbox/plugins/parquet-data-format` read side only

## 1. Problem

The composite (Parquet + Lucene sidecar) index stores keyword values in Parquet and serves them
through a `DocValuesProducer` that synthesizes `SortedDocValues` / `SortedSetDocValues`. Every
OpenSearch consumer of keyword doc values — sort, `terms` aggregation (global-ordinals mode),
`cardinality`, `composite` — assumes Lucene's ordinal contract:

- `ordValue(doc)` returns the doc's rank in the **sorted set of distinct values** of the segment
- `lookupOrd(ord)` / `lookupTerm(key)` / `getValueCount()` / `termsEnum()` work globally

Parquet has no such structure: values are pages of bytes in row order. The previous
implementation (`OrdinalTable`) materialized the **entire column per query** to fabricate
ordinals — with per-row native reads that re-decoded pages from the row-group start, i.e.
O(N²) page decodes. On a 100M-row column, keyword sort/terms queries hung for minutes,
uncancellably (Q24 `SELECT *` >300s).

## 2. Constraints

1. **No OpenSearch core changes** — everything lives in the codec plugin.
2. **No write-side changes** — index files are immutable; node-local, rebuildable read-side
   artifacts are acceptable.
3. **Never materialize the full column per query.**
4. **Correct or loud** — a path that cannot be correct must throw with guidance
   (`execution_hint: map`), never return silently wrong numbers.
5. **Cache everything cacheable** — segment-lifetime amortization, full participation in
   OpenSearch's filter/fielddata/global-ordinals caches.

## 3. Architecture: three tiers

Selection happens per (segment, field) in `ParquetDocValuesLeafReader.withDictionaryOrdinals`:

```
keyword field?  ──no──►  Tier 0 (streaming; global ops fail fast)
      │yes
terms ≤ dictionary budget (65536)?  ──yes──►  Tier A (heap dictionary)
      │no
coverage verified?  ──yes──►  Tier B (disk-backed uninverted ordinals)
      │no (refusal cached)
Tier 0 (streaming; global ops fail fast toward execution_hint:map)
```

### Tier 0 — Streaming iterators (always available, fail-fast)

`ParquetSortedDocValues` / `ParquetSortedSetDocValues` (`codec/iter/`)

- Forward cursor over Parquet pages via the DataFusion reader; the "ordinal" is a per-doc
  fiction (doc id / doc-encoded), values are zero-copy `BytesRef` views into the resident page.
- Serves everything that only needs **per-document values**: fetch (`docvalue_fields`),
  `execution_hint: map` aggregations, scripted access.
- Anything needing real global ordinals (`getValueCount`, `lookupTerm`, stale-ord resolution)
  throws `UnsupportedOperationException` naming `execution_hint: map`. Correct-or-throw.

### Tier A — Dictionary-rank ordinals (low cardinality: ≤ 65536 terms)

`TermDictionary`, `TermDictionaryCache`, `ParquetDictionarySortedDocValues`

- The Lucene sidecar's `TermsEnum` for the field **is** the sorted distinct-value set. Load it
  once per segment — O(distinct), not O(rows) — into a heap dictionary.
- Per-doc ordinal = binary search of the streamed value against the dictionary. Full
  `SortedDocValues` contract; global-ordinals terms aggs, cardinality, composite all work.
- Node-level cache keyed by segment core key; closed-listener eviction; byte budget
  (`parquet.docvalues.dictionary.cache_bytes`, 64 MB) with serve-uncached-on-overflow;
  `INELIGIBLE` sentinel for over-budget fields.
- A value present in Parquet but absent from the dictionary throws `IllegalStateException`
  (loud), which guards against index/DV divergence.

### Tier B — Disk-backed uninverted ordinals (high cardinality)

`UninvertedOrdinals`, `UninvertedOrdinalsCache`, `ParquetUninvertedSortedDocValues`

- **Build (once per segment):** one sequential sweep of the sidecar's terms and postings assigns
  each doc its term's rank into a packed array (`PackedInts`, +1-shifted, 0 = missing), spilled
  via `DirectWriter` to a memory-mapped file `parquet-ords-<segmentId>-<field>.ord` under
  `$TMPDIR/opensearch-parquet-ords/` (tmp + rename). Keyed by the segment's **stable id**, so it
  survives restarts — reopened segments map the file instead of rebuilding. Builds are
  serialized node-wide; cancellation is polled per checkpoint interval.
- **Read:** `ordValue(doc)` = one packed read from the mapped file (`DirectReader`) — no Parquet
  decode on the ordinal path at all. Only touched pages are resident (Lucene `.dvd` economics:
  ~23 bits/doc for 8.5M terms ≈ 288 MB on disk, working-set in RAM).
- **Term resolution** (`lookupOrd`, `lookupTerm`): sparse heap checkpoints every 1024 terms +
  bounded `TermsEnum` scan. Three access paths with distinct economics:
  - *Current-doc value* (`lookupOrd(ordValue())` — map-hint terms, cardinality hashing): served
    zero-copy from the streaming reader's resident page, never via the terms index.
  - *Ascending ord walks* (bucket keys): a stateful `TermCursor` per consumer amortizes to one
    sequential enum pass.
  - *`termsEnum()`* (global `OrdinalMap` construction): the sidecar's real enum wrapped with
    ordinal tracking (`OrdTrackingTermsEnum`), because BlockTree does not implement
    `TermsEnum.ord()` and the default `lookupOrd`-based fallback is quadratic.
- Node-level cache mirrors Tier A: segment core key → field → instance, closed-listener
  eviction (also closes the mmap), refusals cached so failed verification never rescans.

## 4. Correctness safeguards

### 4.1 Keyword gate

Ordinal tiers rank Parquet **values** against sidecar **terms**; these only coincide for
untokenized fields. A `text` field's terms are analyzer tokens — ordinals derived from them
would sort/group docs by tokens, silently wrong. `withDictionaryOrdinals` therefore only
engages when the mapper types the field as `keyword`. Text fields stay on Tier 0 (map-mode
aggregation, bytes-comparator sort — correct, ordinal-free).

### 4.2 Coverage verification (Tier B)

Postings only contain **indexed** values. A stored value that was never indexed
(`ignore_above` truncation, analyzer drops) becomes ordinal-missing and silently undercounts
every aggregation — observed in production benchmarking as a 103-doc deficit on a URL bucket
(514,881 vs PPL's 514,984). At every load, the assigned-ordinal count (one sequential mapped
scan) must **exactly equal** the Parquet column's non-null row count (page-index null
statistics, `ParquetDocValuesProducer.nonNullRowCount`). Any mismatch — or missing null
stats — refuses the tier loudly; the refusal is cached and consumers get Tier 0's fail-fast.

### 4.3 Producer lifecycle (`SharedProducerRegistry`)

OpenSearch caches (fielddata, global ordinals, filter cache) retain doc-values instances
beyond the request-scoped producer's life. A node-level registry maps segment core key →
segment-lifetime producer (closed by the core's closed-listener); request wrappers reroute to
it when their producer is closed. This restored `getCoreCacheHelper()` delegation — the
global-ordinals `OrdinalMap` is built **once per segment** and cached by OpenSearch itself.
Native cursors carry a `Cleaner`-based backstop.

## 5. Measured results (ClickBench, 100M rows, single node)

| Operation on `OriginalURL` (8.51M distinct) | Before | After |
|---|---|---|
| sort | hang (>300 s, uncancellable) | 5.7 s first (build) / **0.75 s** |
| terms agg, global-ordinals | hang | 8.5 s first (OrdinalMap) / **0.2–1.6 s** |
| terms agg, map hint (full scan) | >300 s | **8.8 s** |
| cardinality (full index) | >120 s | **1.4 s** |
| composite | hang | **0.2 s** |
| global-ordinals vs map doc_counts | silently divergent risk | **verified identical** (incl. `sum_other_doc_count`) |

Low-cardinality fields (Tier A) and text fields were verified unregressed; all 255 plugin unit
tests green.

## 6. Settings

| Setting | Default | Meaning |
|---|---|---|
| `parquet.docvalues.dictionary.max_terms` | 65536 | Tier A eligibility threshold (dynamic) |
| `parquet.docvalues.dictionary.cache_bytes` | 64 MB | Tier A node cache budget (dynamic) |

## 7. Known limitations / future work

1. **Tier B build transient heap**: the packed build buffer is maxDoc × bits (~275 MB at
   100M × 23 bits) held during the one-time sweep. Mitigated by node-wide build serialization;
   a circuit-breaker reservation or two-pass streaming spill would harden it.
2. **No disk budget** on the ords directory yet; files are per-segment and reclaimed only by
   eviction-on-close deletion of the node temp dir.
3. **Cancellation** during a Tier B build is thread-interrupt-based, not task-cancel-based.
4. **Text-field ordinals**: deliberately unsupported (gate, §4.1). If needed, a value-based
   linear builder (page-batched Parquet scan — cf. upstream commit `484b704d5`) could feed a
   fourth tier using the same cache/verification skeleton.
5. **Multi-valued keywords** (`SortedSetDocValues` with repeated values) currently route through
   the singleton wrap of the single-valued convention; true multi-valued high-cardinality
   fields would need a doc-to-ords list layout in the ord file.
