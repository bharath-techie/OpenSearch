/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.rewrite;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MVQueryRewriterTests extends OpenSearchTestCase {

    private static final String DEF = "source=logs | stats count() as cnt, sum(latency_ms) as total by status";

    public void testExactMatch() {
        MVQueryRewriter.ViewDef def = new MVQueryRewriter.ViewDef("logs_view", DEF, true);
        assertSame(def, MVQueryRewriter.match(DEF, List.of(def)));
    }

    public void testWhitespaceInsensitiveMatch() {
        MVQueryRewriter.ViewDef def = new MVQueryRewriter.ViewDef("v", DEF, true);
        assertSame(
            def,
            MVQueryRewriter.match("  source=logs |  stats count() as cnt,   sum(latency_ms) as total by status ", List.of(def))
        );
    }

    public void testNonMatchingQueryNotMatched() {
        MVQueryRewriter.ViewDef def = new MVQueryRewriter.ViewDef("v", DEF, true);
        assertNull(MVQueryRewriter.match("source=logs | stats count() as cnt by region", List.of(def)));
        assertNull(MVQueryRewriter.match("source=logs | head 5", List.of(def)));
    }

    public void testAnyAggregateShapeMatches() {
        // No mergeability gating at match time: the engine-side rewrite (spec-driven)
        // decides; avg and sketches are first-class.
        String def = "source=logs | stats avg(latency_ms) as a, distinct_count_approx(host) as dc by status";
        MVQueryRewriter.ViewDef view = new MVQueryRewriter.ViewDef("v", def, true);
        assertSame(view, MVQueryRewriter.match(def, List.of(view)));
    }

    public void testViewDefsFromDocsFiltersMalformed() {
        Map<String, Object> good = Map.of("view", "v1", "query", DEF, "partition_by", Map.of("field", "ts"));
        Map<String, Object> missingQuery = new HashMap<>();
        missingQuery.put("view", "v2");
        List<MVQueryRewriter.ViewDef> defs = MVQueryRewriter.toViewDefs(List.of(good, missingQuery));
        assertEquals(1, defs.size());
        assertEquals("v1", defs.get(0).view());
        assertTrue(defs.get(0).partitioned());
    }
}
