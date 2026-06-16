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
 * End-to-end integration test for the unified scoped parquet page-index cache and
 * the footer-only level-1 metadata cache, verified through the node-stats API
 * ({@code GET /_plugins/_analytics_backend_datafusion/stats}) — the
 * {@code cache_stats.metadata_cache} and {@code cache_stats.scoped_page_index_cache}
 * groups.
 *
 * <p>This suite is deliberately broad: it checks query <b>correctness</b> (not
 * just cache counters), that the <b>level-1 metadata cache still works</b> after
 * the page-index strip, the full <b>scoped-cache</b> hit/miss/size story, and that
 * a spread of query shapes (aggregations, multi-column filters, full-text
 * {@code match}) all still execute. The goal is to prove the page-index changes
 * broke nothing.
 *
 * <h2>Why assertions are same-method, twice-run deltas</h2>
 *
 * The scoped cache is a <b>process-global singleton</b> that persists for the life
 * of the node; the cluster is preserved across methods which run in randomized
 * order; there is no reset endpoint; and provisioning triggers a refresh that
 * warms the (footer-only) metadata cache. So a method must NOT assume a cold
 * cache. The robust signal is measured within one method: run a query, snapshot,
 * run the SAME query again, snapshot — the second run must be a pure hit.
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

    private CacheGroup scoped() throws IOException {
        return fetchGroup("scoped_page_index_cache");
    }

    private CacheGroup metadata() throws IOException {
        return fetchGroup("metadata_cache");
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

    // ---- exposure + correctness -----------------------------------------

    /**
     * The scoped page-index cache group must always be present with a positive
     * byte budget (its configured 64mb limit), even before any query runs.
     */
    public void testScopedCacheGroupIsExposedWithBudget() throws IOException {
        CacheGroup snap = scoped();
        assertTrue(
            "scoped_page_index_cache must advertise a positive size_limit_bytes, got " + snap.sizeLimitBytes(),
            snap.sizeLimitBytes() > 0
        );
        assertTrue("hit_count >= 0", snap.hits() >= 0);
        assertTrue("miss_count >= 0", snap.misses() >= 0);
        assertTrue("entry_count >= 0", snap.entries() >= 0);
    }

    /**
     * The page-index changes must not change query answers. Exact-count
     * assertions over the listing path (numeric + keyword predicates) against
     * known dataset cardinalities.
     */
    public void testListingQueryCorrectnessUnchanged() throws IOException {
        assertEquals(
            "total doc count must be exact",
            TOTAL_DOCS,
            scalarAgg("source=" + DATASET.indexName + " | stats count()")
        );
        assertEquals(
            "status >= 400 count must be exact (numeric listing predicate)",
            STATUS_GE_400,
            scalarAgg("source=" + DATASET.indexName + " | where status >= 400 | stats count()")
        );
        assertEquals(
            "log_level = 'ERROR' count must be exact (keyword predicate)",
            LEVEL_ERROR,
            scalarAgg("source=" + DATASET.indexName + " | where log_level = 'ERROR' | stats count()")
        );
    }

    /**
     * The same correctness must hold when the SAME query is re-run (served partly
     * from the scoped cache) — a cached page index must never change the answer.
     */
    public void testCorrectnessIsStableAcrossCachedReRuns() throws IOException {
        String q = "source=" + DATASET.indexName + " | where status >= 400 | stats count()";
        long first = scalarAgg(q);
        long second = scalarAgg(q);
        assertEquals("cold and warm runs must agree", first, second);
        assertEquals("warm run must still be exact", STATUS_GE_400, second);
    }

    // ---- level-1 metadata cache still works -----------------------------

    /**
     * The footer-only level-1 metadata cache must still function: after repeated
     * queries it holds entries and registers hits (footers are reused, not
     * re-read). This guards against the page-index strip accidentally breaking
     * normal footer caching.
     */
    public void testMetadataCacheStillServesFooters() throws IOException {
        String q = "source=" + DATASET.indexName + " | where status >= 200 | stats count() by service_name";
        // Warm.
        executePpl(q);
        CacheGroup before = metadata();
        // Several more runs — footers must come from cache, driving hits up.
        for (int i = 0; i < 5; i++) {
            executePpl(q);
        }
        CacheGroup after = metadata();

        assertTrue(
            "metadata cache must hold at least one footer entry, got " + after.entries(),
            after.entries() >= 1
        );
        assertTrue(
            String.format(Locale.ROOT, "metadata cache must register hits across repeated queries (before=%d after=%d)",
                before.hits(), after.hits()),
            after.hits() > before.hits()
        );
        assertTrue(
            "metadata cache must advertise its configured byte budget",
            after.sizeLimitBytes() > 0
        );
    }

    // ---- scoped cache: populate, hit, bounded ---------------------------

    /**
     * A filtered listing query populates the scoped cache (entry + bytes &gt; 0)
     * at query time, and re-running the IDENTICAL query is a pure hit: hits up;
     * misses, entries, and memory_bytes flat. Measures hits, misses, AND size.
     */
    public void testSameListingQueryReRunIsPureCacheHit() throws IOException {
        String query = "source=" + DATASET.indexName + " | where status >= 400 | stats count() by service_name";

        executePpl(query);
        CacheGroup afterFirst = scoped();
        assertTrue("scoped cache must hold >= 1 entry after a listing query", afterFirst.entries() >= 1);
        assertTrue("populated scoped cache must consume memory_bytes", afterFirst.memoryBytes() > 0);

        executePpl(query);
        CacheGroup afterSecond = scoped();

        assertTrue(
            String.format(Locale.ROOT, "re-run must register hits (h1=%d m1=%d h2=%d m2=%d)",
                afterFirst.hits(), afterFirst.misses(), afterSecond.hits(), afterSecond.misses()),
            afterSecond.hits() > afterFirst.hits()
        );
        assertEquals("re-run must NOT add misses", afterFirst.misses(), afterSecond.misses());
        assertEquals("re-run must NOT add entries (no duplication)", afterFirst.entries(), afterSecond.entries());
        assertEquals("re-run must NOT grow memory_bytes (no over-alloc)", afterFirst.memoryBytes(), afterSecond.memoryBytes());
    }

    /**
     * Repeated identical queries keep the cache bounded: entries and bytes do not
     * grow after the first populating run; the re-runs all register hits.
     */
    public void testRepeatedListingQueriesDoNotGrowTheCache() throws IOException {
        String query = "source=" + DATASET.indexName + " | where status >= 200 | stats count()";

        executePpl(query);
        CacheGroup baseline = scoped();
        assertTrue("entry must exist after first run", baseline.entries() >= 1);

        for (int i = 0; i < 9; i++) {
            executePpl(query);
        }
        CacheGroup after = scoped();

        assertEquals("repeated queries must not add entries", baseline.entries(), after.entries());
        assertEquals("repeated queries must not grow memory_bytes", baseline.memoryBytes(), after.memoryBytes());
        assertEquals("repeated queries must not add misses", baseline.misses(), after.misses());
        assertTrue(
            String.format(Locale.ROOT, "the 9 re-runs must register hits (baseline=%d after=%d)",
                baseline.hits(), after.hits()),
            after.hits() >= baseline.hits() + 9
        );
    }

    /**
     * The scoped cache must never exceed its configured byte budget — a basic
     * "no over-allocation" invariant readable from the stats API.
     */
    public void testScopedCacheStaysWithinBudget() throws IOException {
        // Exercise a few distinct predicate shapes to build whatever entries the
        // listing path produces, then assert occupancy <= budget.
        executePpl("source=" + DATASET.indexName + " | where status >= 400 | stats count()");
        executePpl("source=" + DATASET.indexName + " | where status < 300 | stats count()");
        CacheGroup snap = scoped();
        assertTrue(
            String.format(Locale.ROOT, "scoped cache memory_bytes (%d) must stay within size_limit_bytes (%d)",
                snap.memoryBytes(), snap.sizeLimitBytes()),
            snap.memoryBytes() <= snap.sizeLimitBytes()
        );
    }

    // ---- no-breakage query sweep ----------------------------------------

    /**
     * A spread of query shapes must all execute successfully (HTTP 200, parseable
     * datarows) with the page-index changes in place: plain projection,
     * aggregation, multi-column filter, full-text match (Lucene-delegated path),
     * and a mixed predicate. This is the broad "nothing is broken" guard.
     */
    public void testVariedQueryShapesAllExecute() throws IOException {
        String idx = DATASET.indexName;

        // Plain projection (no predicate).
        assertEquals("plain projection returns all docs", TOTAL_DOCS, rowCount("source=" + idx + " | fields service_name, status"));

        // Aggregation with grouping.
        assertTrue(
            "grouped aggregation must return at least one bucket",
            rowCount("source=" + idx + " | stats count() by service_name") >= 1
        );

        // Multi-column native predicate (listing path).
        assertEquals(
            "multi-column filter must be exact",
            scalarAgg("source=" + idx + " | where status >= 400 and log_level = 'ERROR' | stats count()"),
            scalarAgg("source=" + idx + " | where log_level = 'ERROR' and status >= 400 | stats count()")
        );

        // Full-text match — exercises the Lucene-delegated path; must not error.
        // (Count is data-dependent; we only assert it executes and is non-negative.)
        long matchCount = scalarAgg("source=" + idx + " | where match(message, 'timeout') | stats count()");
        assertTrue("match() query must execute and return a non-negative count", matchCount >= 0);

        // Mixed: native predicate + full-text, then aggregate.
        long mixed = scalarAgg("source=" + idx + " | where status >= 400 or match(message, 'error') | stats count()");
        assertTrue("mixed predicate query must execute", mixed >= 0);
    }
}
