/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.throttling.tracker.AverageDiskStats;

import java.io.IOException;
import java.util.Locale;

/**
 * This represents the performance stats of a node along with the timestamp at which the stats object was created
 * in the respective node
 */
public class NodePerformanceStatistics implements Writeable {
    final String nodeId;
    long timestamp;
    double cpuUtilizationPercent;
    double memoryUtilizationPercent;

    AverageDiskStats averageDiskStats;

    public NodePerformanceStatistics(String nodeId, double cpuUtilizationPercent, double memoryUtilizationPercent,
                                     AverageDiskStats averageDiskStats, long timestamp) {
        this.nodeId = nodeId;
        this.cpuUtilizationPercent = cpuUtilizationPercent;
        this.memoryUtilizationPercent = memoryUtilizationPercent;
        this.averageDiskStats = averageDiskStats;
        this.timestamp = timestamp;
    }

    public NodePerformanceStatistics(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.cpuUtilizationPercent = in.readDouble();
        this.memoryUtilizationPercent = in.readDouble();
        this.averageDiskStats = new AverageDiskStats(in);
        this.timestamp = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeDouble(this.cpuUtilizationPercent);
        out.writeDouble(this.memoryUtilizationPercent);
        this.averageDiskStats.writeTo(out);
        out.writeLong(this.timestamp);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("NodePerformanceStatistics[");
        sb.append(nodeId).append("](");
        sb.append("CPU utilization percent: ").append(String.format(Locale.ROOT, "%.1f", cpuUtilizationPercent));
        sb.append(", Memory utilization percent: ").append(String.format(Locale.ROOT, "%.1f", memoryUtilizationPercent));
        sb.append(", Timestamp: ").append(timestamp);
        sb.append(")");
        return sb.toString();
    }

    NodePerformanceStatistics(NodePerformanceStatistics nodePerformanceStatistics) {
        this(
            nodePerformanceStatistics.nodeId,
            nodePerformanceStatistics.cpuUtilizationPercent,
            nodePerformanceStatistics.memoryUtilizationPercent,
            nodePerformanceStatistics.averageDiskStats,
            nodePerformanceStatistics.timestamp
        );
    }

    public double getMemoryUtilizationPercent() {
        return memoryUtilizationPercent;
    }

    public double getCpuUtilizationPercent() {
        return cpuUtilizationPercent;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
