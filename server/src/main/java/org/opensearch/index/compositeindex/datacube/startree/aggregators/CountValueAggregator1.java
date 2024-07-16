/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

/**
 * Count value aggregator for star tree
 *
 * @opensearch.experimental
 */
public class CountValueAggregator1 implements ValueAggregator1 {
    public static final StarTreeNumericType VALUE_AGGREGATOR_TYPE = StarTreeNumericType.DOUBLE;

    @Override
    public MetricStat getAggregationType() {
        return MetricStat.COUNT;
    }

    @Override
    public StarTreeNumericType getAggregatedValueType() {
        return VALUE_AGGREGATOR_TYPE;
    }

    @Override
    public double getInitialAggregatedValueForSegmentDocValue(long segmentDocValue, StarTreeNumericType starTreeNumericType) {
        return 1L;
    }

    @Override
    public double getInitialAggregatedValueForSegmentDocValue(double segmentDocValue,
        StarTreeNumericType starTreeNumericType) {
        if(segmentDocValue == 0)
            return 1.0;
        else
            return segmentDocValue;
    }

    @Override
    public double mergeAggregatedValueAndSegmentValue(double value, long segmentDocValue, StarTreeNumericType starTreeNumericType) {
        return value + 1;
    }

    @Override
    public double mergeAggregatedValueAndSegmentValue(double value, double segmentDocValue,
        StarTreeNumericType starTreeNumericType) {
        return value + segmentDocValue;
    }

    @Override
    public double mergeAggregatedValues(double value, double aggregatedValue) {
        return value + aggregatedValue;
    }

    @Override
    public double getInitialAggregatedValue(double value) {
        return value;
    }

    @Override
    public int getMaxAggregatedValueByteSize() {
        return Double.BYTES;
    }

    @Override
    public long toLongValue(double value) {
        try {
            return NumericUtils.doubleToSortableLong(value);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert " + value + " to sortable long", e);
        }
    }

    @Override
    public double toStarTreeNumericTypeValue(long value, StarTreeNumericType type) {
        try {
            return type.getDoubleValue(value);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert " + value + " to sortable aggregation type", e);
        }
    }
}
