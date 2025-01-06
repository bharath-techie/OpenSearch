/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.DoubleArray;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeQueryHelper;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.StarTreeBucketCollector;
import org.opensearch.search.aggregations.StarTreePreComputeCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeFilter;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeQueryHelper.getStarTreeValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeQueryHelper.getSupportedStarTree;

/**
 * Aggregate all docs into a single sum value
 *
 * @opensearch.internal
 */
public class SumAggregator extends NumericMetricsAggregator.SingleValue implements StarTreePreComputeCollector {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;

    private DoubleArray sums;
    private DoubleArray compensations;

    SumAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        // TODO: stop expecting nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        this.format = valuesSourceConfig.format();
        if (valuesSource != null) {
            sums = context.bigArrays().newDoubleArray(1, true);
            compensations = context.bigArrays().newDoubleArray(1, true);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null || (parent != null && subAggregators.length == 0)) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        CompositeIndexFieldInfo supportedStarTree = getSupportedStarTree(this.context);
        if (supportedStarTree != null) {
            return getStarTreeCollector(ctx, sub, supportedStarTree);
        }
        return getDefaultLeafCollector(ctx, sub);
    }

    private LeafBucketCollector getDefaultLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                sums = bigArrays.grow(sums, bucket + 1);
                compensations = bigArrays.grow(compensations, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    double sum = sums.get(bucket);
                    double compensation = compensations.get(bucket);
                    kahanSummation.reset(sum, compensation);

                    for (int i = 0; i < valuesCount; i++) {
                        double value = values.nextValue();
                        kahanSummation.add(value);
                    }

                    compensations.set(bucket, kahanSummation.delta());
                    sums.set(bucket, kahanSummation.value());
                }
            }
        };
    }

    public LeafBucketCollector getStarTreeCollector(LeafReaderContext ctx, LeafBucketCollector sub, CompositeIndexFieldInfo starTree)
        throws IOException {
        final CompensatedSum kahanSummation = new CompensatedSum(sums.get(0), 0);
        if (parent != null && subAggregators.length == 0) {
        }

        return StarTreeQueryHelper.getStarTreeLeafCollector(
            context,
            valuesSource,
            ctx,
            sub,
            starTree,
            MetricStat.SUM.getTypeName(),
            value -> kahanSummation.add(NumericUtils.sortableLongToDouble(value)),
            () -> sums.set(0, kahanSummation.value())
        );
    }

    public StarTreeBucketCollector getStarTreeBucketCollector(LeafReaderContext ctx, CompositeIndexFieldInfo starTree) throws IOException {
        return new StarTreeBucketCollector() {
            {
                this.starTreeValues = getStarTreeValues(ctx, starTree);
                this.matchingDocsBitSet = StarTreeFilter.getPredicateValueToFixedBitSetMap(starTreeValues, "@timestamp_month");
                this.setSubCollectors();
            }
            // final StarTreeValues starTreeValues = getStarTreeValues(ctx, starTree);
            // assert starTreeValues != null;

            // FixedBitSet matchingDocsBitSet = StarTreeFilter.getPredicateValueToFixedBitSetMap(starTreeValues, "@timestamp_month");

            SortedNumericStarTreeValuesIterator metricValuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues
                .getMetricValuesIterator("startree1_status_sum_metric");

            public void setSubCollectors() throws IOException {
                return;
            }

            @Override
            public void collectStarEntry(int starTreeEntryBit, long bucket) throws IOException {
                sums = context.bigArrays().grow(sums, bucket + 1);
                // Advance the valuesIterator to the current bit
                if (!metricValuesIterator.advanceExact(starTreeEntryBit)) {
                    return; // Skip if no entries for this document
                }
                double metricValue = NumericUtils.sortableLongToDouble(metricValuesIterator.nextValue());

                double sum = sums.get(bucket);

                // sums = context.bigArrays().grow(sums, bucket + 1);
                sums.set(bucket, metricValue + sum);
            }

        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= sums.size()) {
            return 0.0;
        }
        return sums.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalSum(name, sums.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSum(name, 0.0, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(sums, compensations);
    }
    //
    // public StarTreeBucketCollector getStarTreeBucketCollector(LeafReaderContext ctx, LeafBucketCollector sub, CompositeIndexFieldInfo
    // starTree) throws IOException {
    // return new StarTreeBucketCollector() {
    // StarTreeValues starTreeValues = getStarTreeValues(ctx, starTree);
    // // assert starTreeValues != null;
    //
    // // FixedBitSet matchingDocsBitSet = StarTreeFilter.getPredicateValueToFixedBitSetMap(starTreeValues, "@timestamp_month");
    //
    // SortedNumericStarTreeValuesIterator metricValuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues
    // .getMetricValuesIterator("startree1_status_sum_metric");
    //
    // @Override
    // public void setSubCollectors() throws IOException {
    // return;
    // }
    //
    // @Override
    // public void collectStarEntry(int starTreeEntryBit, long bucket) throws IOException {
    // sums = context.bigArrays().grow(sums, bucket + 1);
    // // Advance the valuesIterator to the current bit
    // if (!metricValuesIterator.advanceExact(starTreeEntryBit)) {
    // return; // Skip if no entries for this document
    // }
    // double metricValue = NumericUtils.sortableLongToDouble(metricValuesIterator.nextValue());
    //
    // double sum = sums.get(bucket);
    //
    // // sums = context.bigArrays().grow(sums, bucket + 1);
    // sums.set(bucket, metricValue + sum);
    // }
    // };
    // }
}
