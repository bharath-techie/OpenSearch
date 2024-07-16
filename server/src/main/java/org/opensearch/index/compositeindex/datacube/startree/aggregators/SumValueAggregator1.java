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
import org.opensearch.search.aggregations.metrics.CompensatedSum;

/**
 * Sum value aggregator for star tree
 *
 * @opensearch.experimental
 */
public class SumValueAggregator1 implements ValueAggregator1 {

    public static final StarTreeNumericType VALUE_AGGREGATOR_TYPE = StarTreeNumericType.DOUBLE;
    private double sum = 0;
    private double compensation = 0;
    private CompensatedSum kahanSummation = new CompensatedSum(0, 0);

    @Override
    public MetricStat getAggregationType() {
        return MetricStat.SUM;
    }

    @Override
    public StarTreeNumericType getAggregatedValueType() {
        return VALUE_AGGREGATOR_TYPE;
    }

    @Override
    public double getInitialAggregatedValueForSegmentDocValue(long segmentDocValue, StarTreeNumericType starTreeNumericType) {
        kahanSummation.reset(0, 0);
        kahanSummation.add(starTreeNumericType.getDoubleValue(segmentDocValue));
        compensation = kahanSummation.delta();
        sum = kahanSummation.value();
        return kahanSummation.value();
    }

    @Override
    public double getInitialAggregatedValueForSegmentDocValue(double segmentDocValue,
        StarTreeNumericType starTreeNumericType) {
        kahanSummation.reset(0, 0);
        kahanSummation.add(segmentDocValue);
        compensation = kahanSummation.delta();
        sum = kahanSummation.value();
        return kahanSummation.value();
    }

    @Override
    public double mergeAggregatedValueAndSegmentValue(double value, long segmentDocValue, StarTreeNumericType starTreeNumericType) {
        assert kahanSummation.value() == value;
        kahanSummation.reset(sum, compensation);
        kahanSummation.add(starTreeNumericType.getDoubleValue(segmentDocValue));
        compensation = kahanSummation.delta();
        sum = kahanSummation.value();
        return kahanSummation.value();
    }

    @Override
    public double mergeAggregatedValueAndSegmentValue(double value, double segmentDocValue,
        StarTreeNumericType starTreeNumericType) {
        assert kahanSummation.value() == value;
        kahanSummation.reset(sum, compensation);
        kahanSummation.add(segmentDocValue);
        compensation = kahanSummation.delta();
        sum = kahanSummation.value();
        return kahanSummation.value();
    }

    @Override
    public double mergeAggregatedValues(double value, double aggregatedValue) {
        assert kahanSummation.value() == aggregatedValue;
        kahanSummation.reset(sum, compensation);
        kahanSummation.add(value);
        compensation = kahanSummation.delta();
        sum = kahanSummation.value();
        return kahanSummation.value();
    }

    @Override
    public double getInitialAggregatedValue(double value) {
        kahanSummation.reset(0, 0);
        kahanSummation.add(value);
        compensation = kahanSummation.delta();
        sum = kahanSummation.value();
        return kahanSummation.value();
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
