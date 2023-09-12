/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.tracker;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class AverageDiskStats implements Writeable {
    private final double readIopsAverage;
    private final double writeIopsAverage;
    private final double readKbAverage;
    private final double writeKbAverage;
    private final double readLatencyAverage;
    private final double writeLatencyAverage;
    private final double ioUtilizationPercent;

    public AverageDiskStats(double readIopsAverage, double writeIopsAverage, double readKbAverage, double writeKbAverage,
                            double readLatencyAverage, double writeLatencyAverage, double ioUtilizationPercent) {
        this.readIopsAverage = readIopsAverage;
        this.writeIopsAverage = writeIopsAverage;
        this.readKbAverage = readKbAverage;
        this.writeKbAverage = writeKbAverage;
        this.readLatencyAverage = readLatencyAverage;
        this.writeLatencyAverage = writeLatencyAverage;
        this.ioUtilizationPercent = ioUtilizationPercent;
    }

    public AverageDiskStats(StreamInput in) throws IOException {
        this.readIopsAverage = in.readDouble();
        this.readKbAverage = in.readDouble();
        this.readLatencyAverage = in.readDouble();
        this.writeIopsAverage = in.readDouble();
        this.writeKbAverage = in.readDouble();
        this.writeLatencyAverage = in.readDouble();
        this.ioUtilizationPercent = in.readDouble();
    }

    public double getIoUtilizationPercent() {
        return ioUtilizationPercent;
    }

    public double getReadIopsAverage() {
        return readIopsAverage;
    }

    public double getReadKbAverage() {
        return readKbAverage;
    }

    public double getReadLatencyAverage() {
        return readLatencyAverage;
    }

    public double getWriteIopsAverage() {
        return writeIopsAverage;
    }

    public double getWriteKbAverage() {
        return writeKbAverage;
    }

    public double getWriteLatencyAverage() {
        return writeLatencyAverage;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("io_stats");
        builder.field("read_iops_average", String.format(Locale.ROOT, "%.1f", readIopsAverage ));
        builder.field("write_iops_average", String.format(Locale.ROOT, "%.1f", writeIopsAverage));
        builder.field("read_throughput_average", String.format(Locale.ROOT, "%.1f", readKbAverage));
        builder.field("write_throughput_average", String.format(Locale.ROOT, "%.1f", writeKbAverage));
        builder.field("read_latency_average", String.format(Locale.ROOT, "%.8f", readLatencyAverage));
        builder.field("write_latency_average", String.format(Locale.ROOT, "%.8f", writeLatencyAverage));
        builder.field("io_utilization_percent", String.format(Locale.ROOT, "%.3f", ioUtilizationPercent));
        builder.endObject();
        return builder;
    }
}
