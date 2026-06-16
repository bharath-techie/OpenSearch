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
import java.util.Locale;
import java.util.Map;

/**
 * End-to-end integration test for the unified scoped parquet page-index cache,
 * verified through the node-stats API
 * ({@code GET /_plugins/_analytics_backend_datafusion/stats}) — specifically the
 * {@code cache_stats.scoped_page_index_cache} group.
 *
 * <p>This IT exercises the <b>listing-table scan path</b> (a numeric predicate
 * DataFusion evaluates natively over parquet, NOT delegated to Lucene) and
 * asserts the empirically-observable cache behaviour.
 *
 * <h2>Why the assertions are written as same-method, twice-run deltas</h2>
 *
 * The scoped cache is a <b>process-global singleton</b> that persists for the
 * life of the node, and the test cluster is preserved across methods, which run
 * in randomized order. There is no REST endpoint to reset it. On top of that,
 * provisioning a dataset triggers a refresh that warms the metadata cache. So a
 * test must NOT assume the cache is cold when it starts — an entry for a given
 * predicate column may already exist from an earlier method.
 *
 * <p>The robust, order-independent signal is therefore measured <em>within</em>
 * one method: run a query, snapshot, run the <em>same</em> query again, snapshot.
 * After the first run the file's scoped entry is guaranteed present, so the
 * second run must be a pure cache hit — hits increase while misses, entries, and
 * memory_bytes stay flat. That measures all three (hits / misses / size) without
 * depending on global history.
 *
 * <p>Run (fast): {@code ./gradlew :sandbox:qa:analytics-engine-rest:integTest
 * --tests "*ScopedPageIndexCacheIT" -Dsandbox.enabled=true -PrustDebug}.
 */
public class ScopedPageIndexCacheIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("app_logs", "app_logs");
    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ---- stats helpers ---------------------------------------------------

    /** A point-in-time snapshot of the scoped page-index cache, summed across all nodes. */
    private record ScopedCacheSnapshot(long hits, long misses, long entries, long memoryBytes, long sizeLimitBytes) {
        long lookups() {
            return hits + misses;
        }
    }

    @SuppressWarnings("unchecked")
    private ScopedCacheSnapshot fetchScopedCache() throws IOException {
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
            Map<String, Object> scoped = (Map<String, Object>) cacheStats.get("scoped_page_index_cache");
            assertNotNull("cache_stats must contain scoped_page_index_cache", scoped);

            hits += num(scoped, "hit_count");
            misses += num(scoped, "miss_count");
            entries += num(scoped, "entry_count");
            memory += num(scoped, "memory_bytes");
            limit += num(scoped, "size_limit_bytes");
        }
        return new ScopedCacheSnapshot(hits, misses, entries, memory, limit);
    }

    private static long num(Map<String, Object> obj, String key) {
        Object v = obj.get(key);
        assertNotNull("scoped_page_index_cache missing field '" + key + "'", v);
        return ((Number) v).longValue();
    }

    // ---- tests -----------------------------------------------------------

    /**
     * The scoped page-index cache group must always be present in the stats
     * response and advertise a positive byte budget (its configured limit), even
     * before any query has populated it.
     */
    public void testScopedCacheGroupIsExposedWithBudget() throws IOException {
        ScopedCacheSnapshot snap = fetchScopedCache();
        assertTrue(
            "scoped_page_index_cache must advertise a positive size_limit_bytes, got " + snap.sizeLimitBytes(),
            snap.sizeLimitBytes() > 0
        );
        assertTrue("hit_count must be >= 0", snap.hits() >= 0);
        assertTrue("miss_count must be >= 0", snap.misses() >= 0);
        assertTrue("entry_count must be >= 0", snap.entries() >= 0);
    }

    /**
     * A filtered listing-path query populates the scoped cache (after at least one
     * run there is a non-empty entry consuming bytes), and re-running the IDENTICAL
     * query is served entirely from cache: hits increase while misses, entries, and
     * memory_bytes stay flat — the "predictable, no duplication / no
     * over-allocation" guarantee. Measures hits, misses, and size together.
     */
    public void testSameListingQueryReRunIsPureCacheHit() throws IOException {
        // A numeric predicate on `status` stays on the native listing path (it is
        // not delegated to Lucene), so it flows through ScopedPageIndexOptimizer
        // and the scoped reader. The scoped-cache key is (file, predicate-columns)
        // — value-independent — so any `status` predicate maps to the same entry.
        String query = "source=" + DATASET.indexName + " | where status >= 400 | stats count() by service_name";

        // Run #1 — guarantees the file's scoped entry is present afterwards
        // (whether this run was a cold miss or already warm from a prior method).
        executePpl(query);
        ScopedCacheSnapshot afterFirst = fetchScopedCache();

        assertTrue(
            "after a listing query the scoped cache must hold at least one entry, got " + afterFirst.entries(),
            afterFirst.entries() >= 1
        );
        assertTrue(
            "a populated scoped cache must consume memory_bytes, got " + afterFirst.memoryBytes(),
            afterFirst.memoryBytes() > 0
        );

        // Run #2 — identical query. The entry is already cached, so this run must
        // be a pure hit: hits up, everything else flat.
        executePpl(query);
        ScopedCacheSnapshot afterSecond = fetchScopedCache();

        assertTrue(
            String.format(
                Locale.ROOT,
                "re-running the same listing query must register cache hits (run1 lookups: h=%d m=%d; run2: h=%d m=%d)",
                afterFirst.hits(),
                afterFirst.misses(),
                afterSecond.hits(),
                afterSecond.misses()
            ),
            afterSecond.hits() > afterFirst.hits()
        );
        assertEquals(
            "re-running the same query must NOT add scoped-cache misses",
            afterFirst.misses(),
            afterSecond.misses()
        );
        assertEquals(
            "re-running the same query must NOT add entries (no duplication)",
            afterFirst.entries(),
            afterSecond.entries()
        );
        assertEquals(
            "re-running the same query must NOT grow memory_bytes (no over-allocation)",
            afterFirst.memoryBytes(),
            afterSecond.memoryBytes()
        );
    }

    /**
     * The cache stays bounded under repeated identical queries: ten runs of the
     * same listing query produce ten lookups but the entry set and the resident
     * bytes do not grow after the first populating run. This is the strongest
     * "predictable, no over-allocation" assertion the stats API can make from REST.
     */
    public void testRepeatedListingQueriesDoNotGrowTheCache() throws IOException {
        String query = "source=" + DATASET.indexName + " | where status >= 200 | stats count()";

        // Populate once, then take the steady-state baseline.
        executePpl(query);
        ScopedCacheSnapshot baseline = fetchScopedCache();
        assertTrue("entry must exist after first run", baseline.entries() >= 1);

        for (int i = 0; i < 9; i++) {
            executePpl(query);
        }
        ScopedCacheSnapshot after = fetchScopedCache();

        assertEquals(
            "repeated identical queries must not add entries",
            baseline.entries(),
            after.entries()
        );
        assertEquals(
            "repeated identical queries must not grow memory_bytes",
            baseline.memoryBytes(),
            after.memoryBytes()
        );
        assertEquals(
            "repeated identical queries must not add misses",
            baseline.misses(),
            after.misses()
        );
        assertTrue(
            String.format(Locale.ROOT, "the 9 re-runs must register hits (baseline=%d after=%d)",
                baseline.hits(), after.hits()),
            after.hits() >= baseline.hits() + 9
        );
    }
}
