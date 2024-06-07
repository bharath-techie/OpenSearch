/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.compositeindex.startree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.opensearch.index.compositeindex.startree.StarTreeReader;
import org.opensearch.index.compositeindex.startree.StarTreeValues;


public class StarTreeDocValuesWriter extends DocValuesConsumer {
    private DocValuesConsumer delegate;
    private final SegmentWriteState state;

    // TODO : should we make all of this final ?

    List<String> dimensionsSplitOrder;

    Map<String, SortedNumericDocValues> dimensionReaders;

    Map<String, SortedSetDocValues> textDimensionReaders;
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
        mergeStarTreeFields(mergeState);
    }

    public void mergeStarTreeFields(MergeState mergeState) throws IOException {
        Set<String> starTreeFields = new HashSet<>();
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            StarTreeReader producer = (StarTreeReader) mergeState.docValuesProducers[i];
            starTreeFields.addAll(producer.getStarTreeFields());
        }
        for(String field : starTreeFields) {
            mergeStarTreeField(field, mergeState.docValuesProducers);
        }
    }

    private void mergeStarTreeField(String field, DocValuesProducer[] docValuesProducers) throws IOException {
        StarTreeValues values = new StarTreeValues();
        List<StarTreeValues> starTreeValuesList = new ArrayList<>();
        for(DocValuesProducer producer : docValuesProducers) {
            StarTreeReader starTreeReader = (StarTreeReader) producer;
            starTreeValuesList.add(starTreeReader.getStarTreeValues(field));
        }
        // TODO
        // build(starTreeValueslist)
    }
    public void createStarTree() throws IOException {
        if(isMerge) return;
        long startTime = System.currentTimeMillis();
        // build star tree
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
    }
}
