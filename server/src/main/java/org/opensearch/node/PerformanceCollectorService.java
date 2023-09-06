/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ConcurrentCollections;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * This collects node level performance statistics such as cpu, memory, IO of each node and makes it available for
 * coordinator node to aid in throttling, ranking etc
 */
public class PerformanceCollectorService implements ClusterStateListener {
    private final ConcurrentMap<String, PerformanceCollectorService.NodePerformanceStatistics> nodeIdToPerfStats =
        ConcurrentCollections.newConcurrentMap();

    public PerformanceCollectorService(ClusterService clusterService) {
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesRemoved()) {
            for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                removeNode(removedNode.getId());
            }
        }
    }

    void removeNode(String nodeId) {
        nodeIdToPerfStats.remove(nodeId);
    }

    public void addNodePerfStatistics(String nodeId, double cpuUsage, double ioUtilization, double memoryUsage,
                                      long timestamp) {
        nodeIdToPerfStats.compute(nodeId, (id, ns) -> {
            if (ns == null) {
                return new PerformanceCollectorService.NodePerformanceStatistics(
                    nodeId, cpuUsage, ioUtilization, memoryUsage, timestamp);
            } else {
                ns.cpuPercent = cpuUsage;
                ns.memoryPercent = memoryUsage;
                ns.ioUtilizationPercent = ioUtilization;
                ns.timestamp = timestamp;
                return ns;
            }
        });
    }

    public Map<String, PerformanceCollectorService.NodePerformanceStatistics> getAllNodeStatistics() {
        Map<String, NodePerformanceStatistics> nodeStats = new HashMap<>(nodeIdToPerfStats.size());
        nodeIdToPerfStats.forEach((k, v) -> { nodeStats.put(k, new PerformanceCollectorService.NodePerformanceStatistics(v)); });
        return nodeStats;
    }

    /**
     * Optionally return a {@code NodeStatistics} for the given nodeid, if
     * response information exists for the given node. Returns an empty
     * {@code Optional} if the node was not found.
     */
    public Optional<NodePerformanceStatistics> getNodeStatistics(final String nodeId) {
        return Optional.ofNullable(nodeIdToPerfStats.get(nodeId)).map(ns -> new NodePerformanceStatistics(ns));
    }

    public static class NodePerformanceStatistics {
        final String nodeId;
        long timestamp;
        double cpuPercent;
        double ioUtilizationPercent;
        double memoryPercent;

        public NodePerformanceStatistics(
            String nodeId,
            double cpuPercent,
            double ioUtilizationPercent,
            double memoryPercent,
            long timestamp
        ) {
            this.nodeId = nodeId;
            this.cpuPercent = cpuPercent;
            this.ioUtilizationPercent = ioUtilizationPercent;
            this.memoryPercent = memoryPercent;
            this.timestamp = timestamp;
        }

        NodePerformanceStatistics(NodePerformanceStatistics nodeStats) {
            this(
                nodeStats.nodeId,
                nodeStats.cpuPercent,
                nodeStats.ioUtilizationPercent,
                nodeStats.memoryPercent,
                nodeStats.timestamp
            );
        }
    }

}
