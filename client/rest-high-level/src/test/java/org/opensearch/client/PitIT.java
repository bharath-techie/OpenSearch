/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.*;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.junit.Before;
import org.opensearch.monitor.jvm.JvmInfo;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests point in time API with rest high level client
 */
public class PitIT extends OpenSearchRestHighLevelClientTestCase {

    @Before
    public void indexDocuments() throws IOException {
        Request doc1 = new Request(HttpPut.METHOD_NAME, "/index/_doc/1");
        doc1.setJsonEntity("{\"type\":\"type1\", \"id\":1, \"num\":10, \"num2\":50}");
        client().performRequest(doc1);
        Request doc2 = new Request(HttpPut.METHOD_NAME, "/index/_doc/2");
        doc2.setJsonEntity("{\"type\":\"type1\", \"id\":2, \"num\":20, \"num2\":40}");
        client().performRequest(doc2);
        Request doc3 = new Request(HttpPut.METHOD_NAME, "/index/_doc/3");
        doc3.setJsonEntity("{\"type\":\"type1\", \"id\":3, \"num\":50, \"num2\":35}");
        client().performRequest(doc3);
        Request doc4 = new Request(HttpPut.METHOD_NAME, "/index/_doc/4");
        doc4.setJsonEntity("{\"type\":\"type2\", \"id\":4, \"num\":100, \"num2\":10}");
        client().performRequest(doc4);
        Request doc5 = new Request(HttpPut.METHOD_NAME, "/index/_doc/5");
        doc5.setJsonEntity("{\"type\":\"type2\", \"id\":5, \"num\":100, \"num2\":10}");
        client().performRequest(doc5);
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));
    }

    public void testCreateAndDeletePit() throws IOException {
        CreatePitRequest pitRequest = new CreatePitRequest(new TimeValue(1, TimeUnit.DAYS), true, "index");
        CreatePitResponse createPitResponse = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        assertTrue(createPitResponse.getId() != null);
        assertEquals(1, createPitResponse.getTotalShards());
        assertEquals(1, createPitResponse.getSuccessfulShards());
        assertEquals(0, createPitResponse.getFailedShards());
        assertEquals(0, createPitResponse.getSkippedShards());
        GetAllPitNodesResponse getAllPitResponse = highLevelClient().getAllPits(RequestOptions.DEFAULT);
        List<String> pits = getAllPitResponse.getPitInfos().stream().map(r -> r.getPitId()).collect(Collectors.toList());
        assertTrue(pits.contains(createPitResponse.getId()));
        List<String> pitIds = new ArrayList<>();
        pitIds.add(createPitResponse.getId());
        DeletePitRequest deletePitRequest = new DeletePitRequest(pitIds);
        DeletePitResponse deletePitResponse = execute(deletePitRequest, highLevelClient()::deletePit, highLevelClient()::deletePitAsync);
        assertTrue(deletePitResponse.getDeletePitResults().get(0).isSuccessful());
        assertTrue(deletePitResponse.getDeletePitResults().get(0).getPitId().equals(createPitResponse.getId()));
    }

    public void testMaxRunningSearches() throws Exception {
        MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();
        logger.info("USED size here : {}", MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed()/1024/1024);
        logger.info("heap size of env[{}]", JvmInfo.jvmInfo().getMem().getHeapMax());
        try {
            int numThreads = 50;
            List<Thread> threadsList = new LinkedList<>();
            logger.info(threadsList.size());
            CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
            for (int i = 0; i < numThreads; i++) {
                threadsList.add(new Thread(() -> {
                    try {
                        SearchRequest validRequest = new SearchRequest();
                        validRequest.indices("index");
                        logger.info("USED size before : {}", MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed() / 1024 / 1024);
                        //client

                        SearchResponse searchResponse = execute(validRequest, highLevelClient()::search, highLevelClient()::searchAsync);
                        assertNotNull(searchResponse);
                        logger.info("USED size after : {}", MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed() / 1024 / 1024);
                    } catch (IOException e) {
                        fail("submit request failed");
                    } finally {
                        try {

                            barrier.await();
                        } catch (Exception e) {
                            fail();
                        }
                    }
                }
                ));
            }
            threadsList.forEach(Thread::start);
            barrier.await();
            for (Thread thread : threadsList) {
                logger.info("USED size thread : {}", MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed() / 1024 / 1024);
                thread.join();
            }


            //updateClusterSettings(AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.getKey(), 0);
            threadsList.clear();
            AtomicInteger numFailures = new AtomicInteger();
            for (int i = 0; i < numThreads; i++) {
                threadsList.add(new Thread(() -> {
                    try {
                        SearchRequest validRequest = new SearchRequest();
                        //validRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
                        SearchResponse searchResponse = execute(validRequest, highLevelClient()::search, highLevelClient()::searchAsync);
                    } catch (Exception e) {
                        assertTrue(e instanceof ResponseException);
                        assertThat(e.getMessage(), containsString("Trying to create too many concurrent searches"));
                        numFailures.getAndIncrement();

                    } finally {
                        try {
                            numFailures.getAndIncrement();
                            barrier.await();
                        } catch (Exception e) {
                            fail();
                        }
                    }
                }
                ));
            }
            threadsList.forEach(Thread::start);
            barrier.await();
            for (Thread thread : threadsList) {
                thread.join();
            }
            assertEquals(numFailures.get(), 50);
        } catch (Exception e) {
            logger.info("========== EXCEPTION : " + e.getMessage());
            logger.info("============== USED SIZE : " + MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed() / 1024 / 1024);
        }
    }

    public void testDeleteAllAndListAllPits() throws IOException, InterruptedException {
        CreatePitRequest pitRequest = new CreatePitRequest(new TimeValue(1, TimeUnit.DAYS), true, "index");
        CreatePitResponse pitResponse = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        CreatePitResponse pitResponse1 = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        assertTrue(pitResponse.getId() != null);
        assertTrue(pitResponse1.getId() != null);
        DeletePitResponse deletePitResponse = highLevelClient().deleteAllPits(RequestOptions.DEFAULT);
        for (DeletePitInfo deletePitInfo : deletePitResponse.getDeletePitResults()) {
            assertTrue(deletePitInfo.isSuccessful());
        }
        pitResponse = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        pitResponse1 = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);
        assertTrue(pitResponse.getId() != null);
        assertTrue(pitResponse1.getId() != null);
        GetAllPitNodesResponse getAllPitResponse = highLevelClient().getAllPits(RequestOptions.DEFAULT);

        List<String> pits = getAllPitResponse.getPitInfos().stream().map(r -> r.getPitId()).collect(Collectors.toList());
        assertTrue(pits.contains(pitResponse.getId()));
        assertTrue(pits.contains(pitResponse1.getId()));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ActionListener<DeletePitResponse> deletePitListener = new ActionListener<>() {
            @Override
            public void onResponse(DeletePitResponse response) {
                countDownLatch.countDown();
                for (DeletePitInfo deletePitInfo : response.getDeletePitResults()) {
                    assertTrue(deletePitInfo.isSuccessful());
                }
            }

            @Override
            public void onFailure(Exception e) {
                countDownLatch.countDown();
                if (!(e instanceof OpenSearchStatusException)) {
                    throw new AssertionError("Delete all failed");
                }
            }
        };
        final CreatePitResponse pitResponse3 = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);

        ActionListener<GetAllPitNodesResponse> getPitsListener = new ActionListener<GetAllPitNodesResponse>() {
            @Override
            public void onResponse(GetAllPitNodesResponse response) {
                List<String> pits = response.getPitInfos().stream().map(r -> r.getPitId()).collect(Collectors.toList());
                assertTrue(pits.contains(pitResponse3.getId()));
            }

            @Override
            public void onFailure(Exception e) {
                if (!(e instanceof OpenSearchStatusException)) {
                    throw new AssertionError("List all PITs failed", e);
                }
            }
        };
        highLevelClient().getAllPitsAsync(RequestOptions.DEFAULT, getPitsListener);
        highLevelClient().deleteAllPitsAsync(RequestOptions.DEFAULT, deletePitListener);
        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
        // validate no pits case
        getAllPitResponse = highLevelClient().getAllPits(RequestOptions.DEFAULT);
        assertTrue(getAllPitResponse.getPitInfos().size() == 0);
        highLevelClient().deleteAllPitsAsync(RequestOptions.DEFAULT, deletePitListener);
    }
}
