/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;

/**
 * Holds the pair of metric name and it's associated stat
 *
 * @opensearch.experimental
 */
public class MetricEntry {

    private final String metricName;
    private final MetricStat metricStat;

    public MetricEntry(String metricName, MetricStat metricStat) {
        this.metricName = metricName;
        this.metricStat = metricStat;
    }

    public String getMetricName() {
        return metricName;
    }

    public MetricStat getMetricStat() {
        return metricStat;
    }
}
