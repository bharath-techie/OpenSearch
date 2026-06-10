/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for PER-SCAN filter routing in a single-shard SELF-UNION whose
 * two arms each carry their own delegated WHERE.
 *
 * <p>Shape under test (the union analog of the self-join {@code ComplexJoins} Q1):
 * <pre>
 *   source=security_logs | where event_type="A" | stats count() as c
 *   | append [ source=security_logs | where event_type="B" | stats count() as c ]
 * </pre>
 * Both arms scan the SAME single-shard index, so the whole pipeline collapses into ONE
 * data-node fragment with two scans. Each scan must apply its OWN delegated WHERE.
 *
 * <p>The regression this guards against: when both scans of one fragment shared a single
 * filter, both arms computed the same count (the self-join symptom was
 * {@code failed_attempts == suspicious_count} for every row). Here that bug would make
 * both rows report the same count instead of the two distinct event-type counts.
 *
 * <p>{@code security_logs} is provisioned single-shard (see its {@code mapping.json}); the
 * {@code event_type} field is a {@code keyword}, so {@code where event_type="..."}
 * delegates to Lucene — exercising the delegated per-scan path, not just predicate-only.
 *
 * <p>Counts are fixed by the dataset's {@code bulk.json}:
 * {@code authentication_failure} = 79, {@code suspicious_activity} = 38.
 */
public class SelfUnionDelegationIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = SecurityLogsTestHelper.DATASET;

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /**
     * Self-union, two arms with DISTINCT delegated WHEREs over the same single-shard index.
     * Each arm's {@code stats count()} must reflect ITS OWN filter: {@code authentication_failure}
     * → 79, {@code suspicious_activity} → 38. The two output rows therefore carry distinct counts;
     * a shared-filter regression would make them equal.
     */
    public void testSelfUnionDistinctDelegatedFiltersPerArm() throws IOException {
        List<Long> counts = countsFor(
            "source=" + DATASET.indexName + " | where event_type=\"authentication_failure\" | stats count() as c"
                + " | append [ source=" + DATASET.indexName + " | where event_type=\"suspicious_activity\" | stats count() as c ]"
        );
        assertEquals("self-union must produce two count rows (one per arm)", 2, counts.size());
        // Arm arrival order at the coordinator's Union is non-deterministic, so assert the
        // multiset. The point is that the two arms applied DIFFERENT filters → DIFFERENT counts.
        assertTrue(
            "arms must carry their own delegated filter → distinct counts {79, 38}, got " + counts,
            counts.contains(79L) && counts.contains(38L)
        );
    }

    /**
     * Same shape with a more selective second arm to make a shared-filter regression
     * unmistakable: {@code privilege_escalation} = 42 vs {@code suspicious_activity} = 38.
     * If both scans shared the first arm's filter, both rows would read 42.
     */
    public void testSelfUnionSecondArmNotMaskedByFirst() throws IOException {
        List<Long> counts = countsFor(
            "source=" + DATASET.indexName + " | where event_type=\"privilege_escalation\" | stats count() as c"
                + " | append [ source=" + DATASET.indexName + " | where event_type=\"suspicious_activity\" | stats count() as c ]"
        );
        assertEquals(2, counts.size());
        assertTrue("expected distinct counts {42, 38}, got " + counts, counts.contains(42L) && counts.contains(38L));
    }

    /** Run a two-row self-union and return the single {@code c} count column from each row. */
    @SuppressWarnings("unchecked")
    private List<Long> countsFor(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, rows);
        return rows.stream()
            .map(r -> ((Number) r.get(0)).longValue())
            .sorted()
            .toList();
    }
}
