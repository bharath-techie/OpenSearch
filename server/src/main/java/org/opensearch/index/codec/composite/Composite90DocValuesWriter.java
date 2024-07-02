/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreesBuilder;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.StarTreeMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class write the star tree index and star tree doc values
 * based on the doc values structures of the original index
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Composite90DocValuesWriter extends DocValuesConsumer {
    private final DocValuesConsumer delegate;
    private final SegmentWriteState state;
    private final MapperService mapperService;
    private MergeState mergeState = null;
    private final Set<CompositeMappedFieldType> compositeMappedFieldTypes;
    private final Set<String> compositeFieldSet;

    private final Map<String, DocValuesProducer> fieldProducerMap = new HashMap<>();

    public Composite90DocValuesWriter(DocValuesConsumer delegate, SegmentWriteState segmentWriteState, MapperService mapperService)
        throws IOException {

        this.delegate = delegate;
        this.state = segmentWriteState;
        this.mapperService = mapperService;
        this.compositeMappedFieldTypes = mapperService.getCompositeFieldTypes();
        compositeFieldSet = new HashSet<>();
        for (CompositeMappedFieldType type : compositeMappedFieldTypes) {
            compositeFieldSet.addAll(type.fields());
        }
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
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
        // Perform this only during flush flow
        if (mergeState == null) {
            createCompositeIndicesIfPossible(valuesProducer, field);
        }
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedSetField(field, valuesProducer);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private void createCompositeIndicesIfPossible(DocValuesProducer valuesProducer, FieldInfo field) throws IOException {
        if (compositeFieldSet.isEmpty()) return;
        if (compositeFieldSet.contains(field.name)) {
            fieldProducerMap.put(field.name, valuesProducer);
            compositeFieldSet.remove(field.name);
        }
        // we have all the required fields to build composite fields
        if (compositeFieldSet.isEmpty()) {
            for (CompositeMappedFieldType mappedType : compositeMappedFieldTypes) {
                if (mappedType instanceof StarTreeMapper.StarTreeFieldType) {
                    // TODO : Call StarTree builder
                }
            }
            StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService);
            starTreesBuilder.build(fieldProducerMap);
        }
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        // TODO : check if class variable will cause concurrency issues
        this.mergeState = mergeState;
        super.merge(mergeState);
        // TODO : handle merge star tree
        mergeStarTreeFields(mergeState);
        // mergeStarTreeFields(mergeState);
    }

    private void mergeStarTreeFields(MergeState mergeState) throws IOException {
        Map<String, List<CompositeIndexValues>> compositeIndexValuesPerField = new HashMap<>();
        Map<String, CompositeIndexFieldInfo> compositeIndexFieldInfoMap = new HashMap<>();
        for(int i=0; i < mergeState.docValuesProducers.length; i++) {
            CompositeIndexReader reader = (CompositeIndexReader) mergeState.docValuesProducers[i];
            List<CompositeIndexFieldInfo> compositeFieldInfo = reader.getCompositeIndexFields();
            for(CompositeIndexFieldInfo fieldInfo : compositeFieldInfo) {
                CompositeIndexValues compositeIndexValues = reader.getCompositeIndexValues(fieldInfo);
                if(compositeIndexValues != null) {
                    List<CompositeIndexValues> fieldsList =
                        compositeIndexValuesPerField.getOrDefault(fieldInfo.getField(), Collections.emptyList());
                    fieldsList.add(compositeIndexValues);
                    compositeIndexValuesPerField.put(fieldInfo.getField(), fieldsList);
                    compositeIndexFieldInfoMap.put(fieldInfo.getField(), fieldInfo);
                }
            }
        }
        StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService);
        starTreesBuilder.build(compositeIndexValuesPerField, compositeIndexFieldInfoMap);
    }

//    public void mergeAggregatedValues(MergeState mergeState) throws IOException {
//        List<StarTreeAggregatedValues> aggrList = new ArrayList<>();
//        List<String> dimNames = new ArrayList<>();
//        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
//            DocValuesProducer producer = mergeState.docValuesProducers[i];
//            Object obj = producer.getAggregatedDocValues();
//            StarTreeAggregatedValues starTree = (StarTreeAggregatedValues) obj;
//            dimNames = starTree.dimensionValues.keySet().stream().collect(Collectors.toList());
//            aggrList.add(starTree);
//        }
//        long startTime = System.currentTimeMillis();
//        builder = new OffHeapSingleTreeBuilder(
//            data,
//            dimNames,
//            dimensionReaders,
//            state.segmentInfo.maxDoc(),
//            docValuesConsumer,
//            state
//        );
//        builder.build(aggrList);
//        logger.info("Finished merging star-tree in ms : {}", (System.currentTimeMillis() - startTime));
//    }
}
