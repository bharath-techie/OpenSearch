/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Routes the api_metrics PPL queries (the shapes that exposed the scoped-OffsetIndex
 * "provided output is too small for the decompressed data" bug) through the INDEXED
 * table path by injecting a {@code match()} clause, and verifies they all succeed.
 *
 * <p>The listing path was fixed in {@code ScopedPageIndexOptimizer} by deriving the scan's
 * read set from the projection's underlying column references instead of the projected output
 * field names. The indexed/match path computes its read set differently (logical-plan walk in
 * {@code indexed_executor.rs}); this test confirms the same case()/expression-projection shapes
 * work there too.
 *
 * <p>{@code match(cluster, 'webactivity')} matches every row in the dataset (all docs have
 * {@code cluster = webactivity}), so injecting it does not change query results — it only forces
 * the query onto the Lucene-indexed executor that scans parquet via the scoped page index.
 */
public class IndexedPathCaseAggregationIT extends AnalyticsRestTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Dataset DATASET = ApiMetricsTestHelper.DATASET;

    private boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (!provisioned) {
            DatasetProvisioner.provision(client(), DATASET);
            provisioned = true;
        }
    }

    public void testApiMetricsQueriesOnIndexedPath() throws Exception {
        List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(DATASET, "ppl");
        assertFalse("No PPL queries discovered", queryNumbers.isEmpty());

        List<String> failures = new ArrayList<>();
        for (int n : queryNumbers) {
            String original = DatasetProvisioner.loadResource(DATASET.queryResourcePath("ppl", "ppl", n)).trim();
            String indexed = injectMatch(original);
            try {
                Request request = new Request("POST", "/_plugins/_ppl");
                request.setJsonEntity("{\"query\": \"" + escapeJson(indexed) + "\"}");
                Response response = client().performRequest(request);
                JsonNode root = MAPPER.readTree(EntityUtils.toString(response.getEntity()));
                // A successful PPL response carries a schema; a failure carries an "error" block.
                if (root.has("error")) {
                    failures.add("Q" + n + " [" + indexed + "] returned error: " + root.get("error").toString());
                }
            } catch (Exception e) {
                failures.add("Q" + n + " [" + indexed + "] failed: " + e.getMessage());
            }
        }
        if (!failures.isEmpty()) {
            fail("indexed-path api_metrics query failures (" + failures.size() + " of " + queryNumbers.size() + "):\n"
                + String.join("\n", failures));
        }
    }

    /**
     * Insert {@code match(cluster, 'webactivity')} as the first filter so the query routes
     * through the indexed executor. If the query already starts with {@code | where <pred>},
     * AND the match into that predicate; otherwise add a fresh {@code | where match(...)} stage
     * right after {@code source=...}.
     */
    private static String injectMatch(String ppl) {
        String match = "match(cluster, 'webactivity')";
        int firstPipe = ppl.indexOf('|');
        if (firstPipe < 0) {
            return ppl + " | where " + match;
        }
        String source = ppl.substring(0, firstPipe).trim();
        String pipeline = ppl.substring(firstPipe); // starts with '|'

        // Split the pipeline into stages on '|'. Stage 0 is empty (string starts with '|').
        String[] stages = pipeline.split("\\|");
        String firstStage = stages.length > 1 ? stages[1].trim() : "";
        if (firstStage.startsWith("where ")) {
            // AND the match into the existing first-stage predicate only (not the whole pipeline).
            String pred = firstStage.substring("where ".length()).trim();
            StringBuilder sb = new StringBuilder(source);
            sb.append(" | where ").append(match).append(" and (").append(pred).append(')');
            for (int i = 2; i < stages.length; i++) {
                sb.append(" | ").append(stages[i].trim());
            }
            return sb.toString();
        }
        // First stage isn't a where: prepend a fresh match filter before it.
        return source + " | where " + match + " " + pipeline.trim();
    }
}
