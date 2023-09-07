/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class PerformanceCollectorServiceTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private PerformanceCollectorService collector;
    private ThreadPool threadpool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadpool = new TestThreadPool("performance_collector_tests");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadpool
        );
        collector = new PerformanceCollectorService(clusterService);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadpool.shutdownNow();
    }

    public void testNodeStats() throws Exception {
        collector.addNodePerfStatistics("node1", 99, 98, 97,
            System.currentTimeMillis());
        Map<String, PerformanceCollectorService.NodePerformanceStatistics> nodeStats = collector.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertEquals(99.0, nodeStats.get("node1").cpuPercent ,0.0);
        assertEquals(98.0, nodeStats.get("node1").ioUtilizationPercent, 0.0);
        assertEquals(97.0, nodeStats.get("node1").memoryPercent, 0.0);

        Optional<PerformanceCollectorService.NodePerformanceStatistics> nodePerformanceStatistics =
            collector.getNodeStatistics("node1");

        assertNotNull(nodePerformanceStatistics.get());
        assertEquals(99.0, nodePerformanceStatistics.get().cpuPercent, 0.0);
        assertEquals(98.0, nodePerformanceStatistics.get().ioUtilizationPercent, 0.0);
        assertEquals(97.0, nodePerformanceStatistics.get().memoryPercent, 0.0);

        nodePerformanceStatistics =
            collector.getNodeStatistics("node2");
        assertTrue(nodePerformanceStatistics.isEmpty());
    }

    /*
     * Test that concurrently adding values and removing nodes does not cause exceptions
     */
    public void testConcurrentAddingAndRemoving() throws Exception {
        String[] nodes = new String[] { "a", "b", "c", "d" };

        final CountDownLatch latch = new CountDownLatch(5);

        Runnable f = () -> {
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                fail("should not be interrupted");
            }
            for (int i = 0; i < randomIntBetween(100, 200); i++) {
                if (randomBoolean()) {
                    collector.removeNode(randomFrom(nodes));
                }
                collector.addNodePerfStatistics(
                    randomFrom(nodes),
                    randomIntBetween(1, 100),
                    randomIntBetween(1, 100),
                    randomIntBetween(1, 100),
                    System.currentTimeMillis()
                );
            }
        };

        Thread t1 = new Thread(f);
        Thread t2 = new Thread(f);
        Thread t3 = new Thread(f);
        Thread t4 = new Thread(f);

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        latch.countDown();
        t1.join();
        t2.join();
        t3.join();
        t4.join();

        final Map<String, PerformanceCollectorService.NodePerformanceStatistics> nodeStats = collector.getAllNodeStatistics();
        logger.info("--> got stats: {}", nodeStats);
        for (String nodeId : nodes) {
            if (nodeStats.containsKey(nodeId)) {
                assertThat(nodeStats.get(nodeId).memoryPercent, greaterThan(0.0));
                assertThat(nodeStats.get(nodeId).cpuPercent, greaterThan(0.0));
                assertThat(nodeStats.get(nodeId).ioUtilizationPercent, greaterThan(0.0));
            }
        }
    }

    public void testNodeRemoval() throws Exception {
        collector.addNodePerfStatistics("node1", randomIntBetween(1, 100), randomIntBetween(1, 100),
            randomIntBetween(1, 100), System.currentTimeMillis());
        collector.addNodePerfStatistics("node2", randomIntBetween(1, 100), randomIntBetween(1, 100),
            randomIntBetween(1, 100), System.currentTimeMillis());

        ClusterState previousState = ClusterState.builder(new ClusterName("cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9200), "node1"))
                    .add(DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9201), "node2"))
            )
            .build();
        ClusterState newState = ClusterState.builder(previousState)
            .nodes(DiscoveryNodes.builder(previousState.nodes()).remove("node2"))
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, previousState);

        collector.clusterChanged(event);
        final Map<String, PerformanceCollectorService.NodePerformanceStatistics> nodeStats = collector.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertFalse(nodeStats.containsKey("node2"));
    }
}
