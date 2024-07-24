/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.meta;

/**
 * Holds the pair of metric name and it's associated stat
 *
 * @opensearch.experimental
 */
public class MetricEntry {

    private final int metricFieldNumber;
    private final int metricStatOrdinal;

    public MetricEntry(int metricFieldNumber, int metricStatOrdinal) {
        this.metricFieldNumber = metricFieldNumber;
        this.metricStatOrdinal = metricStatOrdinal;
    }

    public int getMetricFieldNumber() {
        return metricFieldNumber;
    }

    public int getMetricStatOrdinal() {
        return metricStatOrdinal;
    }
}
