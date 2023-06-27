/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.admissioncontroller;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Node perf stats
 */
public class NodePerfStats implements Writeable {
    public double cpuPercentAvg;
    public double memoryPercentAvg;
    public double ioPercentAvg;

    public NodePerfStats(StreamInput in) throws IOException {
        this.cpuPercentAvg = in.readDouble();
        this.memoryPercentAvg = in.readDouble();
        this.ioPercentAvg = in.readDouble();
    }

    public NodePerfStats(double cpuPercentAvg, double memoryPercentAvg, double ioPercentAvg) {
        this.cpuPercentAvg = cpuPercentAvg;
        this.memoryPercentAvg = memoryPercentAvg;
        this.ioPercentAvg = ioPercentAvg;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(cpuPercentAvg);
        out.writeDouble(memoryPercentAvg);
        out.writeDouble(ioPercentAvg);
    }
}
