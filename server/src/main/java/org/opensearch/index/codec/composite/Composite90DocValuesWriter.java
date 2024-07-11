/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Composite99DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreesBuilder;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
    private final DocValuesConsumer composite99DocValuesConsumer;

    public IndexOutput dataOut;
    public IndexOutput metaOut;

    private final Map<String, DocValuesProducer> fieldProducerMap = new HashMap<>();
    private static final Logger logger = LogManager.getLogger(Composite90DocValuesWriter.class);

    public Composite90DocValuesWriter(DocValuesConsumer delegate, SegmentWriteState segmentWriteState, MapperService mapperService)
        throws IOException {

        boolean success = false;
        this.delegate = delegate;
        this.state = segmentWriteState;
        this.mapperService = mapperService;
        this.compositeMappedFieldTypes = mapperService.getCompositeFieldTypes();
        try {
            this.composite99DocValuesConsumer = new Composite99DocValuesConsumer(
                segmentWriteState,
                Composite90DocValuesFormat.DATA_DOC_VALUES_CODEC,
                Composite90DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
                Composite90DocValuesFormat.META_DOC_VALUES_CODEC,
                Composite90DocValuesFormat.META_DOC_VALUES_EXTENSION
            );

            String dataFileName = IndexFileNames.segmentFileName(
                segmentWriteState.segmentInfo.name,
                segmentWriteState.segmentSuffix,
                Composite90DocValuesFormat.DATA_EXTENSION
            );
            dataOut = segmentWriteState.directory.createOutput(dataFileName, segmentWriteState.context);
            CodecUtil.writeIndexHeader(
                dataOut,
                Composite90DocValuesFormat.DATA_CODEC_NAME,
                Composite90DocValuesFormat.VERSION_CURRENT,
                segmentWriteState.segmentInfo.getId(),
                segmentWriteState.segmentSuffix
            );

            String metaFileName = IndexFileNames.segmentFileName(
                segmentWriteState.segmentInfo.name,
                segmentWriteState.segmentSuffix,
                Composite90DocValuesFormat.META_EXTENSION
            );
            metaOut = segmentWriteState.directory.createOutput(metaFileName, segmentWriteState.context);
            CodecUtil.writeIndexHeader(
                metaOut,
                Composite90DocValuesFormat.META_CODEC_NAME,
                Composite90DocValuesFormat.VERSION_CURRENT,
                segmentWriteState.segmentInfo.getId(),
                segmentWriteState.segmentSuffix
            );
            compositeFieldSet = new HashSet<>();
            for (CompositeMappedFieldType type : compositeMappedFieldTypes) {
                compositeFieldSet.addAll(type.fields());
            }

            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
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
        if (delegate != null) {
            IOUtils.closeWhileHandlingException(delegate);
        }
        boolean success = false;
        try {
            if (metaOut != null) {
                metaOut.writeLong(-1); // write EOF marker
                CodecUtil.writeFooter(metaOut); // write checksum
            }
            if (dataOut != null) {
                CodecUtil.writeFooter(dataOut); // write checksum
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(dataOut, metaOut);
            } else {
                IOUtils.closeWhileHandlingException(dataOut, metaOut, composite99DocValuesConsumer);
            }
            metaOut = dataOut = null;
        }
        if (composite99DocValuesConsumer != null) {
            composite99DocValuesConsumer.close();
        }
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
                if (mappedType.getCompositeIndexType().equals(CompositeMappedFieldType.CompositeFieldType.STAR_TREE)) {
                    StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService);
                    starTreesBuilder.build(metaOut, dataOut, fieldProducerMap, composite99DocValuesConsumer);
                }
            }
        }
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        // TODO : check if class variable will cause concurrency issues
        this.mergeState = mergeState;
        super.merge(mergeState);
        mergeCompositeFields(mergeState);
    }

    /**
     * Merges composite fields from multiple segments
     *
     * @param mergeState merge state
     */
    private void mergeCompositeFields(MergeState mergeState) throws IOException {
        mergeStarTreeFields(mergeState);
    }

    /**
     * Merges star tree data fields from multiple segments
     *
     * @param mergeState merge state
     */
    private void mergeStarTreeFields(MergeState mergeState) throws IOException {
        Map<String, List<StarTreeValues>> starTreeSubsPerField = new HashMap<>();
        Map<String, StarTreeField> starTreeFieldMap = new HashMap<>();
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            CompositeIndexReader reader = null;
            if (mergeState.docValuesProducers[i] == null) {
                continue;
            }
            if (mergeState.docValuesProducers[i] instanceof CompositeIndexReader) {
                reader = (CompositeIndexReader) mergeState.docValuesProducers[i];
            } else {
                continue;
            }

            List<CompositeIndexFieldInfo> compositeFieldInfo = reader.getCompositeIndexFields();
            for (CompositeIndexFieldInfo fieldInfo : compositeFieldInfo) {
                if (fieldInfo.getType().equals(CompositeMappedFieldType.CompositeFieldType.STAR_TREE)) {
                    CompositeIndexValues compositeIndexValues = reader.getCompositeIndexValues(fieldInfo);
                    if (compositeIndexValues instanceof StarTreeValues) {
                        List<StarTreeValues> fieldsList = starTreeSubsPerField.getOrDefault(fieldInfo.getField(), new ArrayList<>());
                        if (!starTreeFieldMap.containsKey(fieldInfo.getField())) {
                            starTreeFieldMap.put(fieldInfo.getField(), ((StarTreeValues) compositeIndexValues).getStarTreeField());
                        }
                        // assert star tree configuration is same across segments
                        else {
                            assert starTreeFieldMap.get(fieldInfo.getField())
                                .equals(((StarTreeValues) compositeIndexValues).getStarTreeField());
                            logger.error("Star tree configuration is not same for segments during merge");
                        }
                        fieldsList.add((StarTreeValues) compositeIndexValues);
                        starTreeSubsPerField.put(fieldInfo.getField(), fieldsList);
                    }
                }
            }
        }
        final StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService);
        starTreesBuilder.buildDuringMerge(metaOut, dataOut, starTreeFieldMap, starTreeSubsPerField, composite99DocValuesConsumer);
    }
}
