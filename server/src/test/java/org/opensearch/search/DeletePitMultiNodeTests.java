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
import org.opensearch.action.search.*;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class DeletePitMultiNodeTests extends OpenSearchIntegTestCase {

    @Before
    public void setupIndex() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).execute().get();
        ensureGreen();
    }

    @After
    public void clearIndex() {
        client().admin().indices().prepareDelete("index").get();
    }

    private CreatePITResponse createPitOnIndex(String index) throws ExecutionException, InterruptedException {
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { index });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        return execute.get();
    }

    public void testDeletePit() throws Exception {
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
        List<String> pitIds = new ArrayList<>();
        pitIds.add(pitResponse.getId());
        execute = client().execute(CreatePITAction.INSTANCE, request);
        pitResponse = execute.get();
        pitIds.add(pitResponse.getId());
        DeletePITRequest deletePITRequest = new DeletePITRequest(pitIds);
        ActionFuture<DeletePITResponse> deleteExecute = client().execute(DeletePITAction.INSTANCE, deletePITRequest);
        DeletePITResponse deletePITResponse = deleteExecute.get();
        assertTrue(deletePITResponse.isSucceeded());
    }

    public void testDeletePitWhileNodeDrop() throws Exception {
        CreatePITResponse pitResponse = createPitOnIndex("index");
        createIndex("index1", Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build());
        client().prepareIndex("index1").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).execute().get();
        ensureGreen();
        List<String> pitIds = new ArrayList<>();
        pitIds.add(pitResponse.getId());
        CreatePITResponse pitResponse1 = createPitOnIndex("index1");
        pitIds.add(pitResponse1.getId());
        DeletePITRequest deletePITRequest = new DeletePITRequest(pitIds);
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<DeletePITResponse> execute = client().execute(DeletePITAction.INSTANCE, deletePITRequest);
                DeletePITResponse deletePITResponse = execute.get();
                assertEquals(false, deletePITResponse.isSucceeded());
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen();
        ActionFuture<DeletePITResponse> execute = client().execute(DeletePITAction.INSTANCE, deletePITRequest);
        DeletePITResponse deletePITResponse = execute.get();
        assertEquals(true, deletePITResponse.isSucceeded());
        client().admin().indices().prepareDelete("index1").get();
    }

    public void testDeleteAllPitsWhileNodeDrop() throws Exception {
        createPitOnIndex("index");
        createIndex("index1", Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build());
        client().prepareIndex("index1").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).execute().get();
        ensureGreen();
        DeletePITRequest deletePITRequest = new DeletePITRequest("_all");
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<DeletePITResponse> execute = client().execute(DeletePITAction.INSTANCE, deletePITRequest);
                DeletePITResponse deletePITResponse = execute.get();
                assertEquals(false, deletePITResponse.isSucceeded());
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen();
        ActionFuture<DeletePITResponse> execute = client().execute(DeletePITAction.INSTANCE, deletePITRequest);
        DeletePITResponse deletePITResponse = execute.get();
        assertEquals(true, deletePITResponse.isSucceeded());
        client().admin().indices().prepareDelete("index1").get();
    }

    public void testDeleteWhileSearch() throws Exception {
        CreatePITResponse pitResponse = createPitOnIndex("index");
        ensureGreen();
        List<String> pitIds = new ArrayList<>();
        pitIds.add(pitResponse.getId());
        DeletePITRequest deletePITRequest = new DeletePITRequest(pitIds);
        for(int i=0; i<4; i++) {
            client().prepareSearch()
                    .setSize(2)
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                    .execute();
        }
        int count = 0;
        Thread[] threads = new Thread[5];
        CountDownLatch latch = new CountDownLatch(threads.length);
        final AtomicBoolean deleted = new AtomicBoolean(false);

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                    for (int j=0; j<30; j++) {
                        client().prepareSearch()
                                .setSize(2)
                                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                                .execute().get();
                    }
                } catch (Exception e) {
                    if(deleted.get() == true) {
                        if (!e.getMessage().contains("all shards failed"))
                            throw new AssertionError(e);
                        return;
                    }
                    throw new AssertionError(e);
                }
            });
            threads[i].setName("opensearch[node_s_0][search]");
            threads[i].start();
        }
        ActionFuture<DeletePITResponse> execute = client().execute(DeletePITAction.INSTANCE, deletePITRequest);
        DeletePITResponse deletePITResponse = execute.get();
        deleted.set(true);
        assertEquals(true, deletePITResponse.isSucceeded());

        for (Thread thread : threads) {
            thread.join();
        }
    }

}
