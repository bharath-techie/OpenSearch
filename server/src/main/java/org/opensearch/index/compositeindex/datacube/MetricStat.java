/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Supported metric types for composite index
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public enum MetricStat {
    VALUE_COUNT("value_count", 0),
    AVG("avg", 1),
    SUM("sum", 2),
    MIN("min", 3),
    MAX("max", 4),
    DOC_COUNT("doc_count", 5, true);

    private final String typeName;
    private final int metricOrdinal;

    // System field stats cannot be used as input for user metric types
    private final boolean isSystemFieldStat;

    MetricStat(String typeName, int metricOrdinal, boolean isSystemFieldStat) {
        this.typeName = typeName;
        this.metricOrdinal = metricOrdinal;
        this.isSystemFieldStat = isSystemFieldStat;
    }

    MetricStat(String typeName, int metricOrdinal) {
        this.typeName = typeName;
        this.metricOrdinal = metricOrdinal;
        this.isSystemFieldStat = false;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getMetricOrdinal() {
        return metricOrdinal;
    }

    public static MetricStat fromTypeName(String typeName) {
        for (MetricStat metric : MetricStat.values()) {
            // prevent system fields to be entered as user input
            if (metric.getTypeName().equalsIgnoreCase(typeName) && metric.isSystemFieldStat == false) {
                return metric;
            }
        }
        throw new IllegalArgumentException("Invalid metric stat: " + typeName);
    }

    public static MetricStat fromMetricOrdinal(int metricOrdinal) {
        for (MetricStat metric : MetricStat.values()) {
            if (metric.getMetricOrdinal() == metricOrdinal) {
                return metric;
            }
        }
        throw new IllegalArgumentException("Invalid metric stat: " + metricOrdinal);
    }
}
