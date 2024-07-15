/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

/**
 * A value aggregator that pre-aggregates on the input values for a specific type of aggregation.
 *
 * @opensearch.experimental
 */
public interface ValueAggregator1 {

    /**
     * Returns the type of the aggregation.
     */
    MetricStat getAggregationType();

    /**
     * Returns the data type of the aggregated value.
     */
    StarTreeNumericType getAggregatedValueType();

    /**
     * Returns the initial aggregated value.
     */
    double getInitialAggregatedValueForSegmentDocValue(long segmentDocValue, StarTreeNumericType starTreeNumericType);

    /**
     * Applies a segment doc value to the current aggregated value.
     */
    double mergeAggregatedValueAndSegmentValue(double value, long segmentDocValue, StarTreeNumericType starTreeNumericType);

    /**
     * Applies an aggregated value to the current aggregated value.
     */
    double mergeAggregatedValues(double value, double aggregatedValue);

    /**
     * Clones an aggregated value.
     */
    double getInitialAggregatedValue(double value);

    /**
     * Returns the maximum size in bytes of the aggregated values seen so far.
     */
    int getMaxAggregatedValueByteSize();

    /**
     * Converts an aggregated value into a Long type.
     */
    long toLongValue(double value);

    /**
     * Converts an aggregated value from a Long type.
     */
    double toStarTreeNumericTypeValue(long rawValue, StarTreeNumericType type);
}
