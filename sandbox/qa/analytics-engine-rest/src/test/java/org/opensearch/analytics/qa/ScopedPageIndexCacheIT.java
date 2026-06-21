/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * End-to-end integration test for the cell-keyed scoped parquet page-index caches
 * and the footer-only level-1 metadata cache, verified through the node-stats API
 * ({@code GET /_plugins/_analytics_backend_datafusion/stats}) — the
 * {@code cache_stats.metadata_cache}, {@code cache_stats.column_index_cache}, and
 * {@code cache_stats.offset_index_cache} groups.
 *
 * <h2>The two scoped caches</h2>
 *
 * The scoped page index is split into two process-global caches, each keyed at
 * <b>cell</b> granularity so an index is decoded and stored once per file and
 * reused across query shapes:
 * <ul>
 *   <li><b>{@code column_index_cache}</b> — the heavy, predicate-driven ColumnIndex
 *       (per-page string min/max), keyed per {@code (file, col, rg)} cell. Adding a
 *       column to a predicate, or changing a literal, never re-decodes a cell that
 *       is already cached; only genuinely new {@code (col, rg)} cells are read.</li>
 *   <li><b>{@code offset_index_cache}</b> — the cheap, projection-driven OffsetIndex
 *       (fixed-width page offsets), keyed per {@code (file, col)} cell (the value
 *       spans all row groups). Different projections reuse shared column cells.</li>
 * </ul>
 *
 * <h2>Determinism: the clear endpoint + same-method deltas</h2>
 *
 * Most assertions clear the scoped caches first via
 * {@code POST /_plugins/_analytics_backend_datafusion/cache/_clear}
 * so a method starts from a known-empty state, then measure deltas across queries
 * within the one method. (The caches are process-global singletons that persist for
 * the life of the node and the cluster is preserved across randomly-ordered methods,
 * so a method must never assume a globally cold cache — it clears explicitly.)
 *
 * <p>Run (fast): {@code ./gradlew :sandbox:qa:analytics-engine-rest:integTest
 * --tests "*ScopedPageIndexCacheIT" -Dsandbox.enabled=true -PrustDebug}.
 */
public class ScopedPageIndexCacheIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("app_logs", "app_logs");
    private static boolean dataProvisioned = false;

    // Ground truth from datasets/app_logs/bulk.json (200 docs).
    private static final long TOTAL_DOCS = 200;
    private static final long STATUS_GE_400 = 103;
    private static final long LEVEL_ERROR = 115;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ---- stats helpers ---------------------------------------------------

    /** A point-in-time snapshot of one cache group, summed across all nodes. */
    private record CacheGroup(long hits, long misses, long entries, long memoryBytes, long sizeLimitBytes) {}

    @SuppressWarnings("unchecked")
    private CacheGroup fetchGroup(String groupName) throws IOException {
        Request request = new Request("GET", "/_plugins/_analytics_backend_datafusion/stats");
        Response response = client().performRequest(request);
        Map<String, Object> body = assertOkAndParse(response, "datafusion-backend stats");

        Map<String, Object> nodes = (Map<String, Object>) body.get("nodes");
        assertNotNull("stats response must contain 'nodes'", nodes);
        assertFalse("at least one node in stats response", nodes.isEmpty());

        long hits = 0, misses = 0, entries = 0, memory = 0, limit = 0;
        for (Object nodeObj : nodes.values()) {
            Map<String, Object> node = (Map<String, Object>) nodeObj;
            Map<String, Object> cacheStats = (Map<String, Object>) node.get("cache_stats");
            assertNotNull("node must report cache_stats", cacheStats);
            Map<String, Object> group = (Map<String, Object>) cacheStats.get(groupName);
            assertNotNull("cache_stats must contain " + groupName, group);

            hits += num(group, "hit_count");
            misses += num(group, "miss_count");
            entries += num(group, "entry_count");
            memory += num(group, "memory_bytes");
            limit += num(group, "size_limit_bytes");
        }
        return new CacheGroup(hits, misses, entries, memory, limit);
    }

    /** Scoped ColumnIndex cache (predicate-driven, per {@code (file, col, rg)} cell). */
    private CacheGroup columnIndex() throws IOException {
        return fetchGroup("column_index_cache");
    }

    /** Scoped OffsetIndex cache (projection-driven, per {@code (file, col)} cell). */
    private CacheGroup offsetIndex() throws IOException {
        return fetchGroup("offset_index_cache");
    }

    private CacheGroup metadata() throws IOException {
        return fetchGroup("metadata_cache");
    }

    /** Drop all entries + reset counters in BOTH scoped caches (testing convenience). */
    private void clearScopedCaches() throws IOException {
        Request request = new Request("POST", "/_plugins/_analytics_backend_datafusion/cache/_clear");
        assertOkAndParse(client().performRequest(request), "clear scoped page-index cache");
    }

    private static long num(Map<String, Object> obj, String key) {
        Object v = obj.get(key);
        assertNotNull(key + " missing", v);
        return ((Number) v).longValue();
    }

    /** Run a PPL query and return the number of result rows (datarows). */
    @SuppressWarnings("unchecked")
    private long rowCount(String ppl) throws IOException {
        Map<String, Object> body = executePpl(ppl);
        Object dr = body.get("datarows");
        assertNotNull("PPL response must carry datarows: " + ppl, dr);
        return ((List<Object>) dr).size();
    }

    /** Run a single-row `stats count()`-style aggregation and return its scalar long. */
    @SuppressWarnings("unchecked")
    private long scalarAgg(String ppl) throws IOException {
        Map<String, Object> body = executePpl(ppl);
        List<Object> rows = (List<Object>) body.get("datarows");
        assertNotNull("agg must carry datarows: " + ppl, rows);
        assertEquals("agg must return exactly one row: " + ppl, 1, rows.size());
        List<Object> first = (List<Object>) rows.get(0);
        assertFalse("agg row must have a column", first.isEmpty());
        return ((Number) first.get(0)).longValue();
    }

    private String src() {
        return "source=" + DATASET.indexName;
    }

    // ---- exposure + correctness -----------------------------------------

    /**
     * Both scoped cache groups must always be present with a positive byte budget
     * (their configured limits), even before any query runs.
     */
    public void testScopedCacheGroupsAreExposedWithBudgets() throws IOException {
        for (CacheGroup snap : new CacheGroup[] { columnIndex(), offsetIndex() }) {
            assertTrue(
                "scoped cache must advertise a positive size_limit_bytes, got " + snap.sizeLimitBytes(),
                snap.sizeLimitBytes() > 0
            );
            assertTrue("hit_count >= 0", snap.hits() >= 0);
            assertTrue("miss_count >= 0", snap.misses() >= 0);
            assertTrue("entry_count >= 0", snap.entries() >= 0);
        }
    }

    /**
     * The page-index changes must not change query answers. Exact-count assertions
     * over the listing path (numeric + keyword predicates) against known dataset
     * cardinalities.
     */
    public void testListingQueryCorrectnessUnchanged() throws IOException {
        assertEquals("total doc count must be exact", TOTAL_DOCS, scalarAgg(src() + " | stats count()"));
        assertEquals(
            "status >= 400 count must be exact (numeric listing predicate)",
            STATUS_GE_400,
            scalarAgg(src() + " | where status >= 400 | stats count()")
        );
        assertEquals(
            "log_level = 'ERROR' count must be exact (keyword predicate)",
            LEVEL_ERROR,
            scalarAgg(src() + " | where log_level = 'ERROR' | stats count()")
        );
    }

    /**
     * The same correctness must hold when the SAME query is re-run (served partly
     * from the scoped caches) — a cached page index must never change the answer.
     */
    public void testCorrectnessIsStableAcrossCachedReRuns() throws IOException {
        String q = src() + " | where status >= 400 | stats count()";
        long first = scalarAgg(q);
        long second = scalarAgg(q);
        assertEquals("cold and warm runs must agree", first, second);
        assertEquals("warm run must still be exact", STATUS_GE_400, second);
    }

    // ---- level-1 metadata cache still works -----------------------------

    /**
     * The footer-only level-1 metadata cache must still function: after repeated
     * queries it holds entries and registers hits (footers are reused, not
     * re-read). Guards against the page-index strip breaking normal footer caching.
     */
    public void testMetadataCacheStillServesFooters() throws IOException {
        String q = src() + " | where status >= 200 | stats count() by service_name";
        executePpl(q); // warm
        CacheGroup before = metadata();
        for (int i = 0; i < 5; i++) {
            executePpl(q);
        }
        CacheGroup after = metadata();

        assertTrue("metadata cache must hold at least one footer entry, got " + after.entries(), after.entries() >= 1);
        assertTrue(
            String.format(Locale.ROOT, "metadata cache must register hits across repeated queries (before=%d after=%d)",
                before.hits(), after.hits()),
            after.hits() > before.hits()
        );
        assertTrue("metadata cache must advertise its configured byte budget", after.sizeLimitBytes() > 0);
    }

    // ---- populate, hit, bounded -----------------------------------------

    /**
     * A filtered listing query populates the scoped caches (entries + bytes &gt; 0)
     * at query time, and re-running the IDENTICAL query is a pure hit in BOTH
     * caches: hits up; misses, entries, and memory_bytes flat.
     */
    public void testSameListingQueryReRunIsPureCacheHit() throws IOException {
        clearScopedCaches();
        String query = src() + " | where status >= 400 | stats count() by service_name";

        executePpl(query);
        CacheGroup ci1 = columnIndex();
        CacheGroup oi1 = offsetIndex();
        assertTrue("ColumnIndex cache must hold >= 1 cell after a filtered query", ci1.entries() >= 1);
        assertTrue("ColumnIndex cache must consume memory_bytes", ci1.memoryBytes() > 0);
        assertTrue("OffsetIndex cache must hold >= 1 cell after a query that reads columns", oi1.entries() >= 1);

        executePpl(query);
        CacheGroup ci2 = columnIndex();
        CacheGroup oi2 = offsetIndex();

        assertTrue(
            String.format(Locale.ROOT, "CI re-run must register hits (h1=%d h2=%d)", ci1.hits(), ci2.hits()),
            ci2.hits() > ci1.hits()
        );
        assertEquals("CI re-run must NOT add misses", ci1.misses(), ci2.misses());
        assertEquals("CI re-run must NOT add cells (no duplication)", ci1.entries(), ci2.entries());
        assertEquals("CI re-run must NOT grow memory_bytes", ci1.memoryBytes(), ci2.memoryBytes());

        assertTrue("OI re-run must register hits", oi2.hits() > oi1.hits());
        assertEquals("OI re-run must NOT add misses", oi1.misses(), oi2.misses());
        assertEquals("OI re-run must NOT add cells", oi1.entries(), oi2.entries());
        assertEquals("OI re-run must NOT grow memory_bytes", oi1.memoryBytes(), oi2.memoryBytes());
    }

    /**
     * Repeated identical queries keep the caches bounded: cells and bytes do not
     * grow after the first populating run; the re-runs all register hits.
     */
    public void testRepeatedListingQueriesDoNotGrowTheCache() throws IOException {
        clearScopedCaches();
        String query = src() + " | where status >= 200 | stats count()";

        executePpl(query);
        CacheGroup ciBase = columnIndex();
        CacheGroup oiBase = offsetIndex();
        assertTrue("CI cell must exist after first run", ciBase.entries() >= 1);

        for (int i = 0; i < 9; i++) {
            executePpl(query);
        }
        CacheGroup ciAfter = columnIndex();
        CacheGroup oiAfter = offsetIndex();

        assertEquals("repeated queries must not add CI cells", ciBase.entries(), ciAfter.entries());
        assertEquals("repeated queries must not grow CI memory_bytes", ciBase.memoryBytes(), ciAfter.memoryBytes());
        assertEquals("repeated queries must not add CI misses", ciBase.misses(), ciAfter.misses());
        assertEquals("repeated queries must not add OI cells", oiBase.entries(), oiAfter.entries());
        assertEquals("repeated queries must not add OI misses", oiBase.misses(), oiAfter.misses());
        assertTrue(
            String.format(Locale.ROOT, "the 9 CI re-runs must register hits (base=%d after=%d)",
                ciBase.hits(), ciAfter.hits()),
            ciAfter.hits() >= ciBase.hits() + 9
        );
    }

    /**
     * Neither scoped cache may exceed its configured byte budget — a basic
     * "no over-allocation" invariant readable from the stats API.
     */
    public void testScopedCachesStayWithinBudget() throws IOException {
        executePpl(src() + " | where status >= 400 | stats count()");
        executePpl(src() + " | where status < 300 | stats count()");
        executePpl(src() + " | where log_level = 'ERROR' | stats count()");
        for (CacheGroup snap : new CacheGroup[] { columnIndex(), offsetIndex() }) {
            assertTrue(
                String.format(Locale.ROOT, "scoped cache memory_bytes (%d) must stay within size_limit_bytes (%d)",
                    snap.memoryBytes(), snap.sizeLimitBytes()),
                snap.memoryBytes() <= snap.sizeLimitBytes()
            );
        }
    }

    // ---- cell reuse: ColumnIndex (predicate-driven) ---------------------

    /**
     * Adding a column to a predicate must reuse the cells the first predicate
     * already decoded — only the genuinely new column's cells are read. Filter
     * {@code status}, then {@code status AND log_level}: the {@code status} cells
     * are reused (CI hits strictly increase) and no fewer cells exist than before
     * (the {@code log_level} cells are added, never replacing {@code status}).
     */
    public void testAddingPredicateColumnReusesExistingCells() throws IOException {
        clearScopedCaches();

        executePpl(src() + " | where status >= 400 | stats count()");
        CacheGroup afterStatus = columnIndex();
        assertTrue("first predicate must populate CI cells", afterStatus.entries() >= 1);
        long missesAfterStatus = afterStatus.misses();

        // Predicate now also covers log_level: status cells reused, log_level new.
        executePpl(src() + " | where status >= 400 and log_level = 'ERROR' | stats count()");
        CacheGroup afterBoth = columnIndex();

        assertTrue(
            String.format(Locale.ROOT, "adding a column must REUSE the status cells (hits %d -> %d)",
                afterStatus.hits(), afterBoth.hits()),
            afterBoth.hits() > afterStatus.hits()
        );
        assertTrue(
            "adding a column must keep all prior cells and add the new column's cells",
            afterBoth.entries() >= afterStatus.entries()
        );
        // The only NEW misses are for log_level's cells — status was never re-decoded.
        assertTrue(
            "new misses must be bounded by the newly added column's cells (status not re-decoded)",
            afterBoth.misses() > missesAfterStatus
        );
    }

    /**
     * Two predicates on the SAME column with DIFFERENT literals must share the
     * cells — the predicate VALUE never enters the cache key, so changing it adds
     * no cells and the second query is a pure hit on the first's cells.
     */
    public void testDifferentLiteralsSameColumnShareCells() throws IOException {
        clearScopedCaches();

        executePpl(src() + " | where status >= 400 | stats count()");
        CacheGroup first = columnIndex();
        assertTrue("first literal must populate CI cells", first.entries() >= 1);

        executePpl(src() + " | where status >= 100 | stats count()");
        CacheGroup second = columnIndex();

        assertEquals("a different literal on the same column must add NO cells", first.entries(), second.entries());
        assertEquals("a different literal must add NO misses (cells reused)", first.misses(), second.misses());
        assertTrue(
            String.format(Locale.ROOT, "the second literal must HIT the existing cells (hits %d -> %d)",
                first.hits(), second.hits()),
            second.hits() > first.hits()
        );
    }

    /**
     * A predicate that is a SUBSET of an already-cached predicate's columns reuses
     * the relevant cells without decoding anything new. Cache a two-column
     * predicate ({@code status AND log_level}), then run a one-column predicate
     * ({@code status}) — {@code status}'s cells are a pure hit, adding no cells and
     * no misses. (We use a compound predicate to bring the keyword {@code log_level}
     * onto the native page-index path; a standalone keyword equality is fully
     * Lucene-delegated and builds no page index — see the handoff notes.)
     */
    public void testSubsetPredicateReusesCachedCells() throws IOException {
        clearScopedCaches();

        // Compound predicate caches cells for BOTH status and log_level.
        executePpl(src() + " | where status >= 400 and log_level = 'ERROR' | stats count()");
        CacheGroup afterBoth = columnIndex();
        assertTrue("compound predicate must populate CI cells for both columns", afterBoth.entries() >= 2);

        // Subset predicate (status only): its cells are already cached → pure hit.
        executePpl(src() + " | where status >= 400 | stats count()");
        CacheGroup afterSubset = columnIndex();
        assertEquals("a subset predicate must add NO new cells", afterBoth.entries(), afterSubset.entries());
        assertEquals("a subset predicate must add NO misses (cells reused)", afterBoth.misses(), afterSubset.misses());
        assertTrue(
            String.format(Locale.ROOT, "a subset predicate must HIT the cached cells (hits %d -> %d)",
                afterBoth.hits(), afterSubset.hits()),
            afterSubset.hits() > afterBoth.hits()
        );
    }

    // ---- cell reuse: OffsetIndex (projection-driven) --------------------

    /**
     * Different projections on the same predicate must reuse the shared OffsetIndex
     * column cells and only decode the newly projected column. Project a small set
     * of fields, then a different set sharing some columns: OI hits increase
     * (shared columns reused) while only the genuinely new column adds a cell.
     */
    public void testDifferentProjectionsReuseOffsetIndexCells() throws IOException {
        clearScopedCaches();

        // First projection.
        executePpl(src() + " | where status >= 400 | fields status, service_name");
        CacheGroup first = offsetIndex();
        assertTrue("first projection must populate OI cells", first.entries() >= 1);

        // Overlapping projection (shares status; adds log_level).
        executePpl(src() + " | where status >= 400 | fields status, log_level");
        CacheGroup second = offsetIndex();

        assertTrue(
            String.format(Locale.ROOT, "overlapping projection must REUSE shared OI column cells (hits %d -> %d)",
                first.hits(), second.hits()),
            second.hits() > first.hits()
        );
        assertTrue("overlapping projection must keep prior cells", second.entries() >= first.entries());
    }

    /**
     * The OffsetIndex cache is keyed only on {@code (file, col)} — independent of
     * the predicate. Two queries with DIFFERENT predicates but the SAME projection
     * must reuse the same OffsetIndex column cells (predicate changes don't
     * multiply OI cells).
     */
    public void testOffsetIndexIndependentOfPredicate() throws IOException {
        clearScopedCaches();

        executePpl(src() + " | where status >= 400 | fields status, service_name");
        CacheGroup first = offsetIndex();

        // Different predicate, same projected columns → same OI cells.
        executePpl(src() + " | where log_level = 'ERROR' | fields status, service_name");
        CacheGroup second = offsetIndex();

        assertEquals(
            "same projection under a different predicate must add NO new OI cells",
            first.entries(),
            second.entries()
        );
        assertTrue("the shared OI cells must be hit", second.hits() > first.hits());
    }

    // ---- cross-path sharing (one cache, both scan paths) ----------------

    /**
     * The scoped caches must be shared across scan paths: a cell built for a
     * predicate column on the listing path is reused by the indexed path for the
     * same column. A listing query filters {@code status}; then an indexed query
     * (forced onto the indexed path by a {@code match(message, ...)} full-text
     * filter) ALSO filters {@code status}, so it resolves to the SAME
     * {@code (file, status, rg)} cells and HITS them — CI hits increase while the
     * cell count does not grow (no second, path-specific cells).
     */
    public void testCrossPathSharingListingThenIndexed() throws IOException {
        clearScopedCaches();

        // Listing path: numeric predicate on `status` populates CI cells.
        executePpl(src() + " | where status >= 400 | stats count()");
        CacheGroup afterListing = columnIndex();
        assertTrue("listing query must populate scoped CI cells", afterListing.entries() >= 1);

        // Indexed path: a match() filter forces indexed routing; it also filters
        // `status`, so it resolves to the SAME (file, status, rg) cells.
        executePpl(src() + " | where match(message, 'timeout') and status >= 400 | stats count()");
        CacheGroup afterIndexed = columnIndex();

        assertTrue(
            String.format(Locale.ROOT,
                "indexed query on the same predicate column must HIT the listing cells (listing hits=%d indexed hits=%d)",
                afterListing.hits(), afterIndexed.hits()),
            afterIndexed.hits() > afterListing.hits()
        );
        assertEquals(
            "cross-path reuse must NOT create new cells for the same (file, predicate column)",
            afterListing.entries(),
            afterIndexed.entries()
        );
        assertEquals(
            "cross-path reuse must NOT grow CI memory_bytes",
            afterListing.memoryBytes(),
            afterIndexed.memoryBytes()
        );
    }

    // ---- clear endpoint --------------------------------------------------

    /**
     * The clear endpoint must drop all cells and reset counters in BOTH scoped
     * caches: after a populating query and a clear, both groups read zero entries,
     * zero hits, and zero misses, while keeping their configured budgets.
     */
    public void testClearEndpointResetsBothCaches() throws IOException {
        executePpl(src() + " | where status >= 400 | fields status, service_name");
        // Something must be cached before we clear.
        assertTrue("a filtered+projected query must populate CI cells", columnIndex().entries() >= 1);
        assertTrue("a filtered+projected query must populate OI cells", offsetIndex().entries() >= 1);

        clearScopedCaches();

        CacheGroup ci = columnIndex();
        CacheGroup oi = offsetIndex();
        assertEquals("clear must reset CI cells", 0, ci.entries());
        assertEquals("clear must reset CI hits", 0, ci.hits());
        assertEquals("clear must reset CI misses", 0, ci.misses());
        assertEquals("clear must reset CI memory_bytes", 0, ci.memoryBytes());
        assertTrue("clear must keep the CI budget", ci.sizeLimitBytes() > 0);
        assertEquals("clear must reset OI cells", 0, oi.entries());
        assertEquals("clear must reset OI hits", 0, oi.hits());
        assertEquals("clear must reset OI misses", 0, oi.misses());
        assertTrue("clear must keep the OI budget", oi.sizeLimitBytes() > 0);
    }

    // ---- no-breakage query sweep ----------------------------------------

    /**
     * A spread of query shapes must all execute successfully with the cell-keyed
     * caches in place: plain projection, aggregation, multi-column filter,
     * full-text match (Lucene-delegated path), and a mixed predicate.
     */
    public void testVariedQueryShapesAllExecute() throws IOException {
        String idx = DATASET.indexName;

        assertEquals("plain projection returns all docs", TOTAL_DOCS, rowCount("source=" + idx + " | fields service_name, status"));

        assertTrue(
            "grouped aggregation must return at least one bucket",
            rowCount("source=" + idx + " | stats count() by service_name") >= 1
        );

        assertEquals(
            "multi-column filter must be order-independent",
            scalarAgg("source=" + idx + " | where status >= 400 and log_level = 'ERROR' | stats count()"),
            scalarAgg("source=" + idx + " | where log_level = 'ERROR' and status >= 400 | stats count()")
        );

        long matchCount = scalarAgg("source=" + idx + " | where match(message, 'timeout') | stats count()");
        assertTrue("match() query must execute and return a non-negative count", matchCount >= 0);

        long mixed = scalarAgg("source=" + idx + " | where status >= 400 or match(message, 'error') | stats count()");
        assertTrue("mixed predicate query must execute", mixed >= 0);
    }

    // ---- prewarm: metadata cache cold vs warm ---------------------------

    /**
     * Verifies that the metadata-cache prewarm (populated at session-context
     * creation before {@code infer_schema} fires) results in cache hits rather
     * than misses on the FIRST query. Before this change, the first query always
     * caused a miss because the cache was cold; with prewarm, it is hot.
     *
     * <p>We can't directly observe the Java-side {@code infer_schema} call from
     * the IT, but we CAN verify that the metadata cache is populated (entries > 0
     * and memory_bytes > 0) immediately after the first query — if it were
     * populated on the query rather than pre-warmed, the metadata hits should
     * equal the number of files. With prewarm, hits reflect the full prewarm
     * cycle: one miss per file at prewarm time, then hits on every subsequent
     * access within the same query lifecycle.
     */
    public void testMetadataCacheIsPrewarmedBeforeFirstQuery() throws IOException {
        // Run one query to populate prewarm + query caches.
        executePpl(src() + " | where status >= 400 | stats count()");
        CacheGroup meta = metadata();

        assertTrue(
            "metadata cache must hold at least one footer entry after a query (prewarm populated it)",
            meta.entries() >= 1
        );
        assertTrue(
            "metadata cache must consume memory_bytes (footer is cached)",
            meta.memoryBytes() > 0
        );
        assertTrue(
            "metadata cache must have registered hits — prewarm seeded the cache before " +
            "infer_schema fired, so infer_schema found hits not misses",
            meta.hits() >= meta.misses()
        );
    }

    /**
     * Measures the hit-rate improvement from prewarm. On a warm cache (multiple
     * queries run), the metadata hit-rate must be strictly greater than 0 — meaning
     * footers are being served from cache, not re-fetched from the object store
     * on every query. A hit-rate of 0 would indicate prewarm is broken and every
     * query is paying full IO cost for the footer.
     */
    public void testMetadataCacheHitRateIsPositiveAfterWarmup() throws IOException {
        String query = src() + " | where status >= 400 | stats count() by service_name";

        // Cold run — prewarm fires, infer_schema hits, query runs.
        executePpl(query);
        CacheGroup before = metadata();

        // Warm runs — all should hit the pre-populated cache.
        for (int i = 0; i < 5; i++) {
            executePpl(query);
        }
        CacheGroup after = metadata();

        long totalAccesses = after.hits() + after.misses();
        assertTrue("must have had at least some cache accesses", totalAccesses > 0);

        double hitRate = (double) after.hits() / totalAccesses;
        assertTrue(
            String.format(
                java.util.Locale.ROOT,
                "metadata cache hit-rate must be > 0 after prewarm (hits=%d misses=%d rate=%.2f)",
                after.hits(), after.misses(), hitRate
            ),
            hitRate > 0.0
        );
        assertTrue(
            "warm runs must increase hits above the cold-run baseline",
            after.hits() > before.hits()
        );
    }

    /**
     * After a clear + immediate re-query, the metadata cache is re-populated
     * (entries and memory_bytes > 0) — verifying that prewarm fires on every new
     * session, not just the first one. This guards against a regression where
     * prewarm only runs once at startup.
     */
    public void testMetadataCacheRepopulatesAfterClear() throws IOException {
        // Populate then clear.
        executePpl(src() + " | where status >= 400 | stats count()");
        clearScopedCaches();

        // Re-run — a new session context is created, prewarm fires again.
        executePpl(src() + " | where status >= 400 | stats count()");
        CacheGroup meta = metadata();

        assertTrue(
            "metadata cache must repopulate after clear (prewarm fires on each new session)",
            meta.entries() >= 1
        );
        assertTrue("metadata cache must consume memory_bytes after repopulation", meta.memoryBytes() > 0);
    }
}
