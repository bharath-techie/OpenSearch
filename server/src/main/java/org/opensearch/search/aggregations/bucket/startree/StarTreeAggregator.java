/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.startree;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.StarTreeReader;
import org.opensearch.index.codec.startree.codec.StarTreeAggregatedValues;
import org.opensearch.index.codec.startree.codec.StarTreeDocValuesReader;
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

    private Map<String, Long> sumMap = new HashMap<>();
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
        //logger.info("BUILD AGGREGATIONS SIZE : {}", owningBucketOrds.length);
        //logger.info("owningBucketOrds : {}, indexmap size :{}", owningBucketOrds.length, indexMap.size());
        return buildAggregationsForFixedBucketCount(
            owningBucketOrds,
            indexMap.size(),
            (offsetInOwningOrd, docCount, subAggregationResults) -> {
                // TODO : make this better
                String key = "";
                for (Map.Entry<String, Integer> entry : indexMap.entrySet()) {
                    //logger.info("Key: {}, value :{}", entry.getKey(), entry.getValue());
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

        if(!(reader.getDocValuesReader() instanceof StarTreeReader)) return null;
        StarTreeReader starTreeDocValuesReader = (StarTreeReader) reader.getDocValuesReader();
        StarTreeAggregatedValues values = starTreeDocValuesReader.getStarTreeValues();
        // TODO : reader.getSegmentInfo().info.getAttributes();
        final AtomicReference<StarTreeAggregatedValues> aggrVal = new AtomicReference<>(null);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                String segord = "";
                if(aggrVal.get() == null) {
                    StarTreeReader starTreeDocValuesReader = (StarTreeReader) reader.getDocValuesReader();
                    StarTreeAggregatedValues values = starTreeDocValuesReader.getStarTreeValues();
                    aggrVal.set(values);
                    final SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                    SegmentCommitInfo info = segmentReader.getSegmentInfo();
                    segord = info.info.name;
                }
                StarTreeAggregatedValues aggrVals = aggrVal.get();
                List<SortedNumericDocValues> fieldColToDocValuesMap = new ArrayList<>();
                List<SortedSetDocValues> keywordFieldColToDocValuesMap = new ArrayList<>();

                // TODO : validations
                for (String field : fieldCols) {
                    if(aggrVals.dimensionValues.containsKey(field)) {
                        fieldColToDocValuesMap.add(aggrVals.dimensionValues.get(field));
                    }
                    if(aggrVals.keywordDimValues.containsKey(field)) {
                        keywordFieldColToDocValuesMap.add(aggrVals.keywordDimValues.get(field));
                    }
                }
                // Another hardcoding
                SortedNumericDocValues dv = aggrVals.metricValues.get(metrics.get(0));
                if (dv.advanceExact(doc)) {
                    long val = dv.nextValue();
                    // TODO : do optimization for sorted numeric doc vals ?
                    // if (fieldColToDocValuesMap.size() == 1 ) {
                    //final int valuesCount = dv.docValueCount();
//                    for (int i = 0; i < valuesCount; i++) {
//                        val1 += dv.nextValue();
//                    }
                    // }

                    String key = getKey(fieldColToDocValuesMap, keywordFieldColToDocValuesMap, doc, segord);
                    if(key.equals("") ) {
                        return;
                    }
                    if (indexMap.containsKey(key)) {
                        sumMap.put(key, sumMap.getOrDefault(key, 0l) + val);
                    } else {
                        indexMap.put(key, indexMap.size());
                        sumMap.put(key, dv.nextValue());
                    }
                    collectBucket(sub, doc, subBucketOrdinal(bucket, indexMap.get(key)));
                }
            }
        };

    }

    private String getKey(List<SortedNumericDocValues> dimensionsKeyList, List<SortedSetDocValues> keywordList, int doc, String segord) throws IOException {
        StringJoiner sj = new StringJoiner("-");
        for (SortedNumericDocValues dim : dimensionsKeyList) {
            dim.advanceExact(doc);
            long val = dim.nextValue();
            sj.add("" + val);
        }
        for(SortedSetDocValues keyword : keywordList) {
            keyword.advanceExact(doc);
            long val = keyword.nextOrd();
            BytesRef  encoded = keyword.lookupOrd(val);
            // TODO : toString() might not work here - how to handle it generically
            //sj.add("" + keyword.lookupOrd(val).toString());
            InetAddress address = InetAddressPoint.decode(
                Arrays.copyOfRange(encoded.bytes, encoded.offset, encoded.offset + encoded.length)
            );
            //logger.info("Aggregator ::: Doc:{}, ord : {}, address : {}, segName : {}", doc, val, InetAddresses.toAddrString(address), segord);
            sj.add("" + InetAddresses.toAddrString(address));
        }
        return sj.toString();
    }

    private long subBucketOrdinal(long owningBucketOrdinal, int keyOrd) {
        return owningBucketOrdinal * indexMap.size() + keyOrd;
    }
}
