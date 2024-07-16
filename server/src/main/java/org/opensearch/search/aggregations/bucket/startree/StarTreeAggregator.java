/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.startree;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregator;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class StarTreeAggregator extends BucketsAggregator implements SingleBucketAggregator {

    private Map<String, Double> sumMap = new HashMap<>();
    private Map<String, Integer> indexMap = new HashMap<>();

    final StarTree[] _starTrees;

    private List<String> fieldCols;

    private List<String> metrics;

    final InternalStarTree.Factory starTreeFactory;

    private static final Logger logger = LogManager.getLogger(StarTreeAggregator.class);

    public StarTreeAggregator(
        String name,
        AggregatorFactories factories,
        InternalStarTree.Factory starTreeFactory,
        StarTree[] starTrees,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        List<String> fieldCols,
        List<String> metrics
    ) throws IOException {
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);
        this._starTrees = starTrees;
        this.starTreeFactory = starTreeFactory;
        this.fieldCols = fieldCols;
        this.metrics = metrics;
    }

    public static class StarTree implements Writeable, ToXContentObject {
        public static final ParseField KEY_FIELD = new ParseField("key");

        protected final String key;

        public StarTree(String key) {
            this.key = key;
        }

        /**
         * Read from a stream.
         */
        public StarTree(StreamInput in) throws IOException {
            key = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
        }

        public String getKey() {
            return this.key;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (key != null) {
                builder.field(KEY_FIELD.getPreferredName(), key);
            }
            builder.endObject();
            return builder;
        }

        public static final ConstructingObjectParser<StarTree, Void> PARSER = new ConstructingObjectParser<>("startree", arg -> {
            String key = (String) arg[0];
            return new StarTree(key);
        });

        static {
            PARSER.declareField(optionalConstructorArg(), (p, c) -> p.text(), KEY_FIELD, ObjectParser.ValueType.DOUBLE);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            StarTree other = (StarTree) obj;
            return Objects.equals(key, other.key);
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {

        return buildAggregationsForFixedBucketCount(
            owningBucketOrds,
            indexMap.size(),
            (offsetInOwningOrd, docCount, subAggregationResults) -> {
                // TODO : make this better
                String key = "";
                for (Map.Entry<String, Integer> entry : indexMap.entrySet()) {
                    if (offsetInOwningOrd == entry.getValue()) {
                        key = entry.getKey();
                        break;
                    }
                }

                // return starTreeFactory.createBucket(key, docCount, subAggregationResults);
                return new InternalStarTree.Bucket(key, sumMap.get(key), subAggregationResults);
            },
            buckets -> create(name, buckets, metadata())
        );
    }

    public InternalStarTree create(String name, List<InternalStarTree.Bucket> ranges, Map<String, Object> metadata) {
        return new InternalStarTree(name, ranges, metadata);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalStarTree(name, new ArrayList(), new HashMap<>());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        //StarTreeAggregatedValues values = (StarTreeAggregatedValues) ctx.reader().getAggregatedDocValues();
        SegmentReader reader = Lucene.segmentReader(ctx.reader());

        if(!(reader.getDocValuesReader() instanceof CompositeIndexReader)) return null;
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
        List<CompositeIndexFieldInfo> fiList = starTreeDocValuesReader.getCompositeIndexFields();
        StarTreeValues values = (StarTreeValues) starTreeDocValuesReader.getCompositeIndexValues(fiList.get(0));
        final AtomicReference<StarTreeValues> aggrVal = new AtomicReference<>(null);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if(aggrVal.get() == null) {
                    CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
                    List<CompositeIndexFieldInfo> fiList = starTreeDocValuesReader.getCompositeIndexFields();
                    StarTreeValues values = (StarTreeValues) starTreeDocValuesReader.getCompositeIndexValues(fiList.get(0));
                    aggrVal.set(values);
                }
                StarTreeValues aggrVals = aggrVal.get();
                List<SortedNumericDocValues> fieldColToDocValuesMap = new ArrayList<>();

                // TODO : validations
                for (String field : fieldCols) {
                    fieldColToDocValuesMap.add((SortedNumericDocValues) aggrVals.getDimensionDocValuesIteratorMap().get(field));
                }
                // Another hardcoding
                SortedNumericDocValues dv = (SortedNumericDocValues) aggrVals.getMetricDocValuesIteratorMap().get(metrics.get(0));
                if (dv.advanceExact(doc)) {
                    long v = dv.nextValue();
                    double val = NumericUtils.sortableLongToDouble(v);
                    //System.out.println(val);
                    // TODO : do optimization for sorted numeric doc vals ?
                    // if (fieldColToDocValuesMap.size() == 1 ) {
                    //final int valuesCount = dv.docValueCount();
//                    for (int i = 0; i < valuesCount; i++) {
//                        val1 += dv.nextValue();
//                    }
                    // }

                    String key = getKey(fieldColToDocValuesMap, doc);
                    //System.out.println(key);
                    if(key.equals("") ) {
                        return;
                    }
                    if (indexMap.containsKey(key)) {
                        sumMap.put(key, sumMap.getOrDefault(key, 0.0) + val);
                    } else {
                        indexMap.put(key, indexMap.size());
                        sumMap.put(key, val);
                    }
                    collectBucket(sub, doc, subBucketOrdinal(bucket, indexMap.get(key)));
                }
            }
        };

    }

    private String getKey(List<SortedNumericDocValues> dimensionsKeyList, int doc) throws IOException {
        StringJoiner sj = new StringJoiner("-");
        for (SortedNumericDocValues dim : dimensionsKeyList) {
            dim.advanceExact(doc);
            long val = dim.nextValue();
            sj.add("" + val);
        }
        return sj.toString();
    }

    private long subBucketOrdinal(long owningBucketOrdinal, int keyOrd) {
        return owningBucketOrdinal * indexMap.size() + keyOrd;
    }
}
