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
public class SumValueAggregator implements ValueAggregator<Double> {

    public static final StarTreeNumericType STAR_TREE_NUMERIC_TYPE = StarTreeNumericType.DOUBLE;

    @Override
    public MetricStat getAggregationType() {
        return MetricStat.SUM;
    }

    @Override
    public StarTreeNumericType getStarTreeNumericType() {
        return STAR_TREE_NUMERIC_TYPE;
    }

    @Override
    public Double getInitialAggregatedValue(Long segmentDocValue, StarTreeNumericType starTreeNumericType) {
        return starTreeNumericType.getDoubleValue(segmentDocValue);
    }

    @Override
    public Double applySegmentRawValue(Double value, Long segmentDocValue, StarTreeNumericType starTreeNumericType) {
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        kahanSummation.add(value);
        kahanSummation.add(starTreeNumericType.getDoubleValue(segmentDocValue));
        return kahanSummation.value();
    }

    @Override
    public Double applyAggregatedValue(Double value, Double aggregatedValue) {
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        kahanSummation.add(value);
        kahanSummation.add(aggregatedValue);
        return kahanSummation.value();
    }

    @Override
    public Double getAggregatedValue(Double value) {
        return value;
    }

    @Override
    public int getMaxAggregatedValueByteSize() {
        return Double.BYTES;
    }

    @Override
    public Long toLongValue(Double value) {
        try {
            return NumericUtils.doubleToSortableLong(value);
        } catch (IllegalArgumentException | NullPointerException | IllegalStateException e) {
            throw new IllegalArgumentException("Cannot convert " + value + " to sortable long", e);
        }
    }

    @Override
    public Double toStarTreeNumericTypeValue(Long value, StarTreeNumericType type) {
        try {
            return type.getDoubleValue(value);
        } catch (IllegalArgumentException | NullPointerException | IllegalStateException e) {
            throw new IllegalArgumentException("Cannot convert " + value + " to sortable aggregation type", e);
        }
    }
}
