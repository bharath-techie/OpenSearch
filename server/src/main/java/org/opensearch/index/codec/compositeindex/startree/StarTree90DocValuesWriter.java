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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.compositeindex.startree.StarTreeReader;
import org.opensearch.index.compositeindex.startree.StarTreeValues;


public class StarTree90DocValuesWriter extends DocValuesConsumer {
    private DocValuesConsumer delegate;
    private final SegmentWriteState state;
    IndexOutput data;

    DocValuesConsumer docValuesConsumer;
    public static final String DATA_CODEC = "Lucene90DocValuesData";
    public static final String META_CODEC = "Lucene90DocValuesMetadata";
    private static final Logger logger = LogManager.getLogger(StarTree90DocValuesWriter.class);

    Map<String, Set<String>> starTreeFieldMap = new HashMap<>();

    private boolean isMerge = false;


    public StarTree90DocValuesWriter(DocValuesConsumer delegate, SegmentWriteState segmentWriteState) throws IOException {
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
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedSetField(field, valuesProducer);
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
