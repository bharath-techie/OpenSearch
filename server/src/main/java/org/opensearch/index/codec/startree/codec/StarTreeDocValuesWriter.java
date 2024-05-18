/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.startree.codec;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.codec.StarTreeReader;
import org.opensearch.index.codec.startree.builder.BaseSingleTreeBuilder;
import org.opensearch.index.codec.startree.builder.OffHeapSingleTreeBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Custom star tree doc values writer
 * */
public class StarTreeDocValuesWriter extends DocValuesConsumer {

    private DocValuesConsumer delegate;
    private final SegmentWriteState state;

    // TODO : should we make all of this final ?

    List<String> dimensionsSplitOrder;

    Map<String, SortedNumericDocValues> dimensionReaders;

    Map<String, SortedSetDocValues> textDimensionReaders;
    BaseSingleTreeBuilder builder;
    IndexOutput data;

    DocValuesConsumer docValuesConsumer;
    public static final String DATA_CODEC = "Lucene90DocValuesData";
    public static final String META_CODEC = "Lucene90DocValuesMetadata";
    private static final Logger logger = LogManager.getLogger(StarTreeDocValuesWriter.class);

    Map<String, Set<String>> starTreeFieldMap = new HashMap<>();
    private Set<String> starTreeFields = new HashSet<>();

    private boolean isMerge = false;


    public StarTreeDocValuesWriter(DocValuesConsumer delegate, SegmentWriteState segmentWriteState) throws IOException {
        this.delegate = delegate;
        this.state = segmentWriteState;
        dimensionReaders = new HashMap<>();
        textDimensionReaders = new HashMap<>();
        dimensionsSplitOrder = new ArrayList<>();
        docValuesConsumer = new Lucene90DocValuesConsumerCopy(state, DATA_CODEC, "sttd", META_CODEC, "sttm");

        // Assume we can get data cube fields [ dims + metrics ] from these attributes
        //segmentWriteState.segmentInfo.getAttributes();


        starTreeFields.add("timestamp");
        starTreeFields.add("status");
        starTreeFields.add("clientip");
        starTreeFieldMap.put("field1", starTreeFields);
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        // TODO : check for attributes
        // if(field.attributes().containsKey("dimensions") ||
        // field.attributes().containsKey("metric") ) {
        // dimensionReaders.put(field.name, valuesProducer.getNumeric(field));
        // }
        delegate.addNumericField(field, valuesProducer);
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addBinaryField(field, valuesProducer);
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedField(field, valuesProducer);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedNumericField(field, valuesProducer);
        // TODO: remove hardcoding
        if (field.name.equalsIgnoreCase("@timestamp")) {
            // logger.info("Adding timestamp fields");
            dimensionReaders.put("minute_dim", valuesProducer.getSortedNumeric(field));
            dimensionReaders.put("hour_dim", valuesProducer.getSortedNumeric(field));
            dimensionReaders.put("day_dim", valuesProducer.getSortedNumeric(field));
            dimensionReaders.put("month_dim", valuesProducer.getSortedNumeric(field));
            dimensionReaders.put("year_dim", valuesProducer.getSortedNumeric(field));

            for(Map.Entry<String, Set<String>> fieldEntry : starTreeFieldMap.entrySet()) {
                fieldEntry.getValue().remove("timestamp");
            }
        }
        if (field.name.contains("status")) {
            // TODO : change this metric type
            dimensionReaders.put(field.name + "_dim", valuesProducer.getSortedNumeric(field));
            dimensionReaders.put(field.name + "_sum_metric", valuesProducer.getSortedNumeric(field));
            for(Map.Entry<String, Set<String>> fieldEntry : starTreeFieldMap.entrySet()) {
                fieldEntry.getValue().remove("status");
            }
        }
        for(Map.Entry<String, Set<String>> fieldEntry : starTreeFieldMap.entrySet()) {
            // If we have indexed all the doc values fields to build star tree, then we can index star tree
            if(fieldEntry.getValue().isEmpty()) {
                aggregateWithoutLucene(fieldEntry.getKey());
            }
        }
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedSetField(field, valuesProducer);
        textDimensionReaders.put(field.name + "_dim", valuesProducer.getSortedSet(field));
        for(Map.Entry<String, Set<String>> fieldEntry : starTreeFieldMap.entrySet()) {
            fieldEntry.getValue().remove(field.name);
        }
        for(Map.Entry<String, Set<String>> fieldEntry : starTreeFieldMap.entrySet()) {
            if(fieldEntry.getValue().isEmpty()) {
                aggregateWithoutLucene(fieldEntry.getKey());
            }
        }

    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        // TODO : check if class variable will cause concurrency issues
        isMerge = true;
        super.merge(mergeState);
        isMerge = false;
        mergeAggregatedValues(mergeState);
    }

    public void mergeAggregatedValues(MergeState mergeState) throws IOException {
        List<StarTreeAggregatedValues> aggrList = new ArrayList<>();
        List<String> dimNames = new ArrayList<>();
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
//            DocValuesProducer producer = mergeState.docValuesProducers[i];
//            Object obj = producer.getAggregatedDocValues();
//            StarTreeAggregatedValues starTree = (StarTreeAggregatedValues) obj;

            StarTreeReader producer = (StarTreeReader) mergeState.docValuesProducers[i];
            StarTreeAggregatedValues starTree = producer.getStarTreeValues();

            dimNames = starTree.dimensionValues.keySet().stream().collect(Collectors.toList());
            aggrList.add(starTree);
        }
        long startTime = System.currentTimeMillis();
        builder = new OffHeapSingleTreeBuilder(
            data,
            dimNames,
            dimensionReaders,
            textDimensionReaders,
            state.segmentInfo.maxDoc(),
            docValuesConsumer,
            state
        );
        builder.build(aggrList, mergeState);
        logger.info("Finished merging star-tree in ms : {}", (System.currentTimeMillis() - startTime));
    }

    //@Override
    public void createStarTree() throws IOException {
        if(isMerge) return;
        long startTime = System.currentTimeMillis();
        builder = new OffHeapSingleTreeBuilder(
            data,
            dimensionsSplitOrder,
            dimensionReaders,
            textDimensionReaders,
            state.segmentInfo.maxDoc(),
            docValuesConsumer,
            state
        );
        builder.build();
        logger.info("Finished building star-tree in ms : {}", (System.currentTimeMillis() - startTime));
    }

    public void aggregateWithoutLucene(String field) throws IOException {
        // TODO : Assume with field we can build different star tree fields
        if(starTreeFieldMap.containsKey(field))
            createStarTree();
        starTreeFieldMap.remove(field);
    }

    @Override
    public void close() throws IOException {
        if (delegate != null) {
            delegate.close();
        }
        if (docValuesConsumer != null) {
            docValuesConsumer.close();
        }
        if (builder != null) {
            builder.close();
        }
    }
}
