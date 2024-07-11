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
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.StarTree99DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.index.compositeindex.CompositeIndexMetadata;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.MergeDimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricEntry;
import org.opensearch.index.compositeindex.datacube.startree.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.node.OffHeapStarTree;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTree;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reader for star tree index and star tree doc values from the segments
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Composite99DocValuesReader extends DocValuesProducer implements CompositeIndexReader {
    private static final Logger logger = LogManager.getLogger(CompositeIndexMetadata.class);

    private final DocValuesProducer delegate;
    private final IndexInput dataIn;
    private final ChecksumIndexInput metaIn;
    private final Map<String, StarTree> starTreeMap = new LinkedHashMap<>();
    private final Map<String, CompositeIndexMetadata> compositeIndexMetadataMap = new LinkedHashMap<>();
    private final Map<String, DocValuesProducer> compositeDocValuesProducerMap = new LinkedHashMap<>();
    private final List<CompositeIndexFieldInfo> compositeFieldInfos = new ArrayList<>();
    private final SegmentReadState readState;

    public Composite99DocValuesReader(DocValuesProducer producer, SegmentReadState readState) throws IOException {
        this.delegate = producer;
        this.readState = readState;

        String metaFileName = IndexFileNames.segmentFileName(
            readState.segmentInfo.name,
            readState.segmentSuffix,
            Composite99DocValuesFormat.META_EXTENSION
        );

        String dataFileName = IndexFileNames.segmentFileName(
            readState.segmentInfo.name,
            readState.segmentSuffix,
            Composite99DocValuesFormat.DATA_EXTENSION
        );
        dataIn = readState.directory.openInput(dataFileName, readState.context);
        metaIn = readState.directory.openChecksumInput(metaFileName, readState.context);
        boolean success = false;
        try {

            CodecUtil.checkIndexHeader(
                dataIn,
                Composite99DocValuesFormat.DATA_CODEC_NAME,
                Composite99DocValuesFormat.VERSION_START,
                Composite99DocValuesFormat.VERSION_CURRENT,
                readState.segmentInfo.getId(),
                readState.segmentSuffix
            );

            Throwable priorE = null;
            try {
                CodecUtil.checkIndexHeader(
                    metaIn,
                    Composite99DocValuesFormat.META_CODEC_NAME,
                    Composite99DocValuesFormat.VERSION_START,
                    Composite99DocValuesFormat.VERSION_CURRENT,
                    readState.segmentInfo.getId(),
                    readState.segmentSuffix
                );

                while (true) {
                    long magicMarker = metaIn.readLong();

                    if (magicMarker == -1) {
                        break;
                    } else if (magicMarker < 0) {
                        throw new CorruptIndexException("Unknown token encountered: " + magicMarker, metaIn);
                    }
                    CompositeIndexMetadata compositeIndexMetadata = new CompositeIndexMetadata(metaIn, magicMarker);
                    String compositeFieldName = compositeIndexMetadata.getCompositeFieldName();
                    compositeFieldInfos.add(
                        new CompositeIndexFieldInfo(compositeFieldName, compositeIndexMetadata.getCompositeFieldType())
                    );
                    switch (compositeIndexMetadata.getCompositeFieldType()) {
                        case STAR_TREE:
                            StarTreeMetadata starTreeMetadata = compositeIndexMetadata.getStarTreeMetadata();
                            StarTree starTree = new OffHeapStarTree(dataIn, starTreeMetadata);
                            starTreeMap.put(compositeFieldName, starTree);
                            compositeIndexMetadataMap.put(compositeFieldName, compositeIndexMetadata);

                            List<Integer> dimensionFieldNumbers = starTreeMetadata.getDimensionFieldNumbers();
                            List<FieldInfo> dimensions = new ArrayList<>();
                            for (Integer fieldNumber : dimensionFieldNumbers) {
                                dimensions.add(readState.fieldInfos.fieldInfo(fieldNumber));
                            }

                            StarTree99DocValuesProducer starTreeDocValuesProducer = new StarTree99DocValuesProducer(
                                readState,
                                Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
                                Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
                                Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
                                Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION,
                                dimensions,
                                starTreeMetadata.getMetricEntries(),
                                compositeFieldName
                            );
                            compositeDocValuesProducerMap.put(compositeFieldName, starTreeDocValuesProducer);

                            break;
                        default:
                            throw new CorruptIndexException("Invalid composite field type found in the file", dataIn);
                    }
                }
            } catch (Throwable t) {
                priorE = t;
            } finally {
                CodecUtil.checkFooter(metaIn, priorE);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
                IOUtils.closeWhileHandlingException(metaIn, dataIn);
            }
        }
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return delegate.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return delegate.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return delegate.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return delegate.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return delegate.getSortedSet(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
        // CodecUtil.checksumEntireFile(metaIn);
        CodecUtil.checksumEntireFile(dataIn);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        starTreeMap.clear();
        for (DocValuesProducer p : compositeDocValuesProducerMap.values()) {
            p.close();
        }
        compositeIndexMetadataMap.clear();
        compositeDocValuesProducerMap.clear();
        IOUtils.closeWhileHandlingException(metaIn, dataIn);
    }

    @Override
    public List<CompositeIndexFieldInfo> getCompositeIndexFields() {
        return compositeFieldInfos;

    }

    @Override
    public CompositeIndexValues getCompositeIndexValues(CompositeIndexFieldInfo compositeIndexFieldInfo) throws IOException {

        switch (compositeIndexFieldInfo.getType()) {
            case STAR_TREE:
                CompositeIndexMetadata compositeIndexMetadata = compositeIndexMetadataMap.get(compositeIndexFieldInfo.getField());
                StarTreeMetadata starTreeMetadata = compositeIndexMetadata.getStarTreeMetadata();
                Set<Integer> skipStarNodeCreationInDimsFieldNumbers = starTreeMetadata.getSkipStarNodeCreationInDims();
                Set<String> skipStarNodeCreationInDims = new HashSet<>();
                for (Integer fieldNumber : skipStarNodeCreationInDimsFieldNumbers) {
                    skipStarNodeCreationInDims.add(readState.fieldInfos.fieldInfo(fieldNumber).getName());
                }

                List<Integer> dimensionFieldNumbers = starTreeMetadata.getDimensionFieldNumbers();
                List<String> dimensions = new ArrayList<>();
                List<Dimension> mergeDimensions = new ArrayList<>();
                for (Integer fieldNumber : dimensionFieldNumbers) {
                    dimensions.add(readState.fieldInfos.fieldInfo(fieldNumber).getName());
                    mergeDimensions.add(new MergeDimension(readState.fieldInfos.fieldInfo(fieldNumber).name));
                }

                Map<String, Metric> starTreeMetricMap = new ConcurrentHashMap<>();
                for (MetricEntry metricEntry : starTreeMetadata.getMetricEntries()) {
                    String metricName = metricEntry.getMetricName();

                    Metric metric = starTreeMetricMap.computeIfAbsent(metricName, field -> new Metric(field, new ArrayList<>()));
                    metric.getMetrics().add(metricEntry.getMetricStat());
                }
                List<Metric> starTreeMetrics = new ArrayList<>(starTreeMetricMap.values());

                StarTreeField starTreeField = new StarTreeField(
                    compositeIndexMetadata.getCompositeFieldName(),
                    mergeDimensions,
                    starTreeMetrics,
                    new StarTreeFieldConfiguration(
                        starTreeMetadata.getMaxLeafDocs(),
                        skipStarNodeCreationInDims,
                        starTreeMetadata.getStarTreeBuildMode()
                    )
                );
                StarTreeNode rootNode = starTreeMap.get(compositeIndexFieldInfo.getField()).getRoot();
                StarTree99DocValuesProducer starTree99DocValuesProducer = (StarTree99DocValuesProducer) compositeDocValuesProducerMap.get(
                    compositeIndexMetadata.getCompositeFieldName()
                );
                Map<String, DocIdSetIterator> dimensionsDocIdSetIteratorMap = new LinkedHashMap<>();
                Map<String, DocIdSetIterator> metricsDocIdSetIteratorMap = new LinkedHashMap<>();

                for (String dimension : dimensions) {
                    dimensionsDocIdSetIteratorMap.put(
                        dimension,
                        starTree99DocValuesProducer.getSortedNumeric(
                            StarTreeHelper.fullFieldNameForStarTreeDimensionsDocValues(starTreeField.getName(), dimension)
                        )
                    );
                }

                for (MetricEntry metricEntry : starTreeMetadata.getMetricEntries()) {
                    String metricFullName = StarTreeHelper.fullFieldNameForStarTreeMetricsDocValues(
                        starTreeField.getName(),
                        metricEntry.getMetricName(),
                        metricEntry.getMetricStat().getTypeName()
                    );
                    metricsDocIdSetIteratorMap.put(metricFullName, starTree99DocValuesProducer.getSortedNumeric(metricFullName));
                }

                return new StarTreeValues(starTreeField, rootNode, dimensionsDocIdSetIteratorMap, metricsDocIdSetIteratorMap);

            default:
                throw new CorruptIndexException("Unsupported composite index field type: ", compositeIndexFieldInfo.getType().getName());
        }

    }
}
