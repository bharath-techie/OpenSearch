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

package org.opensearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Nullable;
import org.opensearch.common.Rounding;
import org.opensearch.common.lease.Releasables;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeQueryHelper;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.filterrewrite.DateHistogramAggregatorBridge;
import org.opensearch.search.aggregations.bucket.filterrewrite.FilterRewriteOptimizationContext;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.opensearch.search.aggregations.metrics.CompensatedSum;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeFilter;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeQueryHelper.getStarTreeFilteredValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeQueryHelper.getStarTreeValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeQueryHelper.getSupportedStarTree;
import static org.opensearch.search.aggregations.bucket.filterrewrite.DateHistogramAggregatorBridge.segmentMatchAll;

/**
 * An aggregator for date values. Every date is rounded down using a configured
 * {@link Rounding}.
 *
 * @see Rounding
 *
 * @opensearch.internal
 */
class DateHistogramAggregator extends BucketsAggregator implements SizedBucketAggregator {
    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat formatter;
    private final Rounding rounding;
    /**
     * The rounding prepared for rewriting the data in the shard.
     */
    private final Rounding.Prepared preparedRounding;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final LongBounds extendedBounds;
    private final LongBounds hardBounds;
    private final LongKeyedBucketOrds bucketOrds;

    private final FilterRewriteOptimizationContext filterRewriteOptimizationContext;

    DateHistogramAggregator(
        String name,
        AggregatorFactories factories,
        Rounding rounding,
        Rounding.Prepared preparedRounding,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        @Nullable LongBounds extendedBounds,
        @Nullable LongBounds hardBounds,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, aggregationContext, parent, CardinalityUpperBound.MANY, metadata);
        this.rounding = rounding;
        this.preparedRounding = preparedRounding;
        this.order = order;
        order.validate(this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.hardBounds = hardBounds;
        // TODO: Stop using null here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        this.formatter = valuesSourceConfig.format();

        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), cardinality);

        DateHistogramAggregatorBridge bridge = new DateHistogramAggregatorBridge() {
            @Override
            protected boolean canOptimize() {
                return canOptimize(valuesSourceConfig);
            }

            @Override
            protected void prepare() throws IOException {
                buildRanges(context);
            }

            @Override
            protected Rounding getRounding(long low, long high) {
                return rounding;
            }

            @Override
            protected Rounding.Prepared getRoundingPrepared() {
                return preparedRounding;
            }

            @Override
            protected long[] processHardBounds(long[] bounds) {
                return super.processHardBounds(bounds, hardBounds);
            }

            @Override
            protected Function<Long, Long> bucketOrdProducer() {
                return (key) -> bucketOrds.add(0, preparedRounding.round((long) key));
            }
        };
        filterRewriteOptimizationContext = new FilterRewriteOptimizationContext(bridge, parent, subAggregators.length, context);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        boolean optimized = filterRewriteOptimizationContext.tryOptimize(ctx, this::incrementBucketDocCount, segmentMatchAll(context, ctx));
        if (optimized) throw new CollectionTerminatedException();

        SortedNumericDocValues values = valuesSource.longValues(ctx);
        CompositeIndexFieldInfo supportedStarTree = getSupportedStarTree(this.context);
        if (supportedStarTree != null) {
            StarTreeValues starTreeValues = getStarTreeValues(ctx, supportedStarTree);
            assert starTreeValues != null;

            Map<Long, FixedBitSet> matchingDocsBitSet = StarTreeFilter.getPredicateValueToFixedBitSetMap(starTreeValues, "@timestamp_month");

            assert (matchingDocsBitSet.size() == 3);
//            return getStarTreeLeafCollector(ctx, sub, supportedStarTree);

            // TODO: get doc_coun as well here other than sums (hard-coded right now)
            Map<Long, Double> sumMap = getPredicateValueToSumMap(starTreeValues, matchingDocsBitSet, "startree1_status_sum_metric");

            assert (sumMap.size() == 3);

            // Here we have the sumMap which has all sums
            // To extract this info to sub-collector / sub-aggregator buckets is the blocker rn.

        }

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    int valuesCount = values.docValueCount();

                    long previousRounded = Long.MIN_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        long rounded = preparedRounding.round(value);
                        assert rounded >= previousRounded;
                        if (rounded == previousRounded) {
                            continue;
                        }
                        if (hardBounds == null || hardBounds.contain(rounded)) {
                            long bucketOrd = bucketOrds.add(owningBucketOrd, rounded);
                            if (bucketOrd < 0) { // already seen
                                bucketOrd = -1 - bucketOrd;
                                collectExistingBucket(sub, doc, bucketOrd);
                            } else {
                                collectBucket(sub, doc, bucketOrd);
                            }
                        }
                        previousRounded = rounded;
                    }
                }
            }
        };
    }

    // ignore this method
    public LeafBucketCollector getStarTreeLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub, CompositeIndexFieldInfo starTree)
            throws IOException {
        StarTreeValues starTreeValues = getStarTreeValues(ctx, starTree);
        assert starTreeValues != null;

        // Field for the date to group by calendar interval (e.g., month)
        String dateField = ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName();
        String sumMetricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                starTree.getField(), "status", MetricStat.SUM.getTypeName()
        );

        // Date values iterator for each date bucket (e.g., month)
        SortedNumericStarTreeValuesIterator dateValuesIterator =
                (SortedNumericStarTreeValuesIterator) starTreeValues.getMetricValuesIterator(dateField);
        SortedNumericStarTreeValuesIterator sumValuesIterator =
                (SortedNumericStarTreeValuesIterator) starTreeValues.getMetricValuesIterator(sumMetricName);

        FixedBitSet matchedDocIds = getStarTreeFilteredValues(context, ctx, starTreeValues);
        assert matchedDocIds != null;

        Map<Long, CompensatedSum> bucketSums = new HashMap<>();
        int numBits = matchedDocIds.length();

        if (numBits > 0) {
            for (int bit = matchedDocIds.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS;
                 bit = (bit + 1 < numBits) ? matchedDocIds.nextSetBit(bit + 1) : DocIdSetIterator.NO_MORE_DOCS) {

                if (!dateValuesIterator.advanceExact(bit) || !sumValuesIterator.advanceExact(bit)) continue;

                // Get the unique calendar interval value for this document
                long bucketId = dateValuesIterator.nextValue();

                for (int i = 0; i < sumValuesIterator.entryValueCount(); i++) {
                    long metricValue = sumValuesIterator.nextValue();

                    if (hardBounds == null || hardBounds.contain(bucketId)) {
                        bucketSums.computeIfAbsent(bucketId, k -> new CompensatedSum(0.0, 0))
                                .add(NumericUtils.sortableLongToDouble(metricValue));
                    }
                }
            }
        }

        // Transfer the results from bucketSums to the `sums` array
        int idx = 0;
        for (Map.Entry<Long, CompensatedSum> entry : bucketSums.entrySet()) {
//            sums.set(idx++, entry.getValue().value()); // Assuming `sums` is a pre-allocated array
        }

        return new LeafBucketCollectorBase(sub, valuesSource.doubleValues(ctx)) {
            @Override
            public void collect(int doc, long bucket) {
                throw new CollectionTerminatedException();
            }
        };
    }

    public static Map<Long, Double> getPredicateValueToSumMap(
            StarTreeValues starTreeValues,
            Map<Long, FixedBitSet> predicateValueToBitSet,
            String sumField // The field to compute the sum from
    ) throws IOException {

        Map<Long, Double> predicateValueToSum = new HashMap<>();

        // Iterate over each entry in the predicateValueToBitSet map
        for (Map.Entry<Long, FixedBitSet> entry : predicateValueToBitSet.entrySet()) {
            Long predicateValue = entry.getKey();
            FixedBitSet bitSet = entry.getValue();

            double sum = 0;

            // Get the doc values for the sumField
            SortedNumericStarTreeValuesIterator valuesIterator =
                    (SortedNumericStarTreeValuesIterator) starTreeValues.getMetricValuesIterator(sumField);

            // Iterate over the matching documents in the FixedBitSet
            int numBits = bitSet.length();
            if (numBits > 0) {
                for (int bit = bitSet.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = (bit + 1 < numBits)
                        ? bitSet.nextSetBit(bit + 1)
                        : DocIdSetIterator.NO_MORE_DOCS) {

                    if (valuesIterator.advanceExact(bit)) {
                        for (int i = 0, count = valuesIterator.entryValueCount(); i < count; i++) {
                            sum += NumericUtils.sortableLongToDouble(valuesIterator.nextValue());
                        }
                    }
                }
            }

            predicateValueToSum.put(predicateValue, sum);
        }

        return predicateValueToSum;
    }


    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForVariableBuckets(owningBucketOrds, bucketOrds, (bucketValue, docCount, subAggregationResults) -> {
            return new InternalDateHistogram.Bucket(bucketValue, docCount, keyed, formatter, subAggregationResults);
        }, (owningBucketOrd, buckets) -> {
            // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
            CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

            // value source will be null for unmapped fields
            // Important: use `rounding` here, not `shardRounding`
            InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalDateHistogram.EmptyBucketInfo(rounding.withoutOffset(), buildEmptySubAggregations(), extendedBounds)
                : null;
            return new InternalDateHistogram(
                name,
                buckets,
                order,
                minDocCount,
                rounding.offset(),
                emptyBucketInfo,
                formatter,
                keyed,
                metadata()
            );
        });
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
            ? new InternalDateHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
            : null;
        return new InternalDateHistogram(
            name,
            Collections.emptyList(),
            order,
            minDocCount,
            rounding.offset(),
            emptyBucketInfo,
            formatter,
            keyed,
            metadata()
        );
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("total_buckets", bucketOrds.size());
        filterRewriteOptimizationContext.populateDebugInfo(add);
    }

    /**
     * @return the size of the bucket in specified units, or 1.0 if unitSize is null
     */
    @Override
    public double bucketSize(long bucket, Rounding.DateTimeUnit unitSize) {
        if (unitSize != null) {
            return preparedRounding.roundingSize(bucketOrds.get(bucket), unitSize);
        } else {
            return 1.0;
        }
    }
}
