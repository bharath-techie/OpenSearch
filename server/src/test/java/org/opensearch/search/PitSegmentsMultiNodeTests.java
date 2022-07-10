/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.junit.After;
import org.junit.Before;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.PitSegmentsAction;
import org.opensearch.action.admin.indices.segments.PitSegmentsRequest;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.ExecutionException;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

/**
 * Multi node integration tests for PIT segments operation
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 3)
public class PitSegmentsMultiNodeTests extends OpenSearchIntegTestCase {

    @Before
    public void setupIndex() throws ExecutionException, InterruptedException {
        for (int i = 0; i < 4; i++) {
            String indexName = "index" + i;
            createIndex(indexName, Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 0).build());
            client().prepareIndex(indexName).setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).execute().get();
            ensureGreen();
        }
    }

    @After
    public void clearIndex() {
        for (int i = 0; i < 4; i++) {
            String indexName = "index" + i;
            client().admin().indices().prepareDelete(indexName).get();
        }
    }

    public void testPitSegments() throws Exception {
        IndicesSegmentResponse indicesSegmentResponse;
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[]{"index1"});
        client().execute(CreatePitAction.INSTANCE, request).get();
        indicesSegmentResponse = client().execute(PitSegmentsAction.INSTANCE, new PitSegmentsRequest()).get();
        assertTrue(indicesSegmentResponse.getShardFailures() == null || indicesSegmentResponse.getShardFailures().length == 0);


    }
}
