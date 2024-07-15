/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.apache.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.time.DateUtils;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocumentModified;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo1;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.ValueAggregator1;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreeBuilder;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreesBuilder;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeBuilderUtils;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.io.IOException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeHelper.fullFieldNameForStarTreeDimensionsDocValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeHelper.fullFieldNameForStarTreeMetricsDocValues;

/**
 * Builder for star tree. Defines the algorithm to construct star-tree
 * See {@link StarTreesBuilder} for information around the construction of star-trees based on star-tree fields
 *
 * @opensearch.experimental
 */
public abstract class BaseStarTreeBuilder2 implements StarTreeBuilder, Accountable {
    private static final Logger logger = LogManager.getLogger(BaseStarTreeBuilder2.class);
    /**
     * Default value for star node
     */
    public static final long STAR_IN_DOC_VALUES_INDEX = -1;
    protected final Set<Integer> skipStarNodeCreationForDimensions;
    protected final List<MetricAggregatorInfo1> metricAggregatorInfos;
    protected final int numMetrics;
    protected final int numDimensions;
    protected int numStarTreeDocs;
    protected int totalSegmentDocs;
    protected int numStarTreeNodes;
    protected final int maxLeafDocuments;
    protected final StarTreeBuilderUtils.TreeNode rootNode = getNewNode();
    protected final StarTreeField starTreeField;
    private final MapperService mapperService;
    private final SegmentWriteState writeState;
    private final IndexOutput metaOut;
    private final IndexOutput dataOut;

    /**
     * Builds star tree based on star tree field configuration consisting of dimensions, metrics and star tree index specific configuration.
     *
     * @param starTreeField holds the configuration for the star tree
     * @param writeState    stores the segment write writeState
     * @param mapperService helps to find the original type of the field
     */
    protected BaseStarTreeBuilder2(
        IndexOutput metaOut,
        IndexOutput dataOut,
        StarTreeField starTreeField,
        SegmentWriteState writeState,
        MapperService mapperService
    ) throws IOException {

        // logger.info("Building star tree : {} maxLeaf docs : {}", starTreeField, starTreeField.getStarTreeConfig().maxLeafDocs());

        this.metaOut = metaOut;
        this.dataOut = dataOut;

        this.starTreeField = starTreeField;
        StarTreeFieldConfiguration starTreeFieldSpec = starTreeField.getStarTreeConfig();

        List<Dimension> dimensionsSplitOrder = starTreeField.getDimensionsOrder();
        this.numDimensions = dimensionsSplitOrder.size();

        this.skipStarNodeCreationForDimensions = new HashSet<>();
        this.totalSegmentDocs = writeState.segmentInfo.maxDoc();
        this.mapperService = mapperService;
        this.writeState = writeState;

        Set<String> skipStarNodeCreationForDimensions = starTreeFieldSpec.getSkipStarNodeCreationInDims();

        for (int i = 0; i < numDimensions; i++) {
            if (skipStarNodeCreationForDimensions.contains(dimensionsSplitOrder.get(i).getField())) {
                this.skipStarNodeCreationForDimensions.add(i);
            }
        }

        this.metricAggregatorInfos = generateMetricAggregatorInfos(mapperService);
        this.numMetrics = metricAggregatorInfos.size();
        this.maxLeafDocuments = starTreeFieldSpec.maxLeafDocs();
    }

    /**
     * Generates the configuration required to perform aggregation for all the metrics on a field
     *
     * @return list of MetricAggregatorInfo
     */
    public List<MetricAggregatorInfo1> generateMetricAggregatorInfos(MapperService mapperService) {
        List<MetricAggregatorInfo1> metricAggregatorInfos = new ArrayList<>();
        for (Metric metric : this.starTreeField.getMetrics()) {
            for (MetricStat metricType : metric.getMetrics()) {
                IndexNumericFieldData.NumericType numericType;
                Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper(metric.getField());
                if (fieldMapper instanceof NumberFieldMapper) {
                    numericType = ((NumberFieldMapper) fieldMapper).fieldType().numericType();
                } else {
                    logger.error("unsupported mapper type");
                    throw new IllegalStateException("unsupported mapper type");
                }

                MetricAggregatorInfo1 metricAggregatorInfo = new MetricAggregatorInfo1(
                    metricType,
                    metric.getField(),
                    starTreeField.getName(),
                    numericType
                );
                metricAggregatorInfos.add(metricAggregatorInfo);
            }
        }
        return metricAggregatorInfos;
    }

    /**
     * Generates the configuration required to perform aggregation for all the metrics on a field
     *
     * @return list of MetricAggregatorInfo
     */
    public List<SequentialDocValuesIterator> getMetricReaders(SegmentWriteState state, Map<String, DocValuesProducer> fieldProducerMap)
        throws IOException {

        List<SequentialDocValuesIterator> metricReaders = new ArrayList<>();
        for (Metric metric : this.starTreeField.getMetrics()) {
            for (MetricStat metricType : metric.getMetrics()) {
                SequentialDocValuesIterator metricReader;
                FieldInfo metricFieldInfo = state.fieldInfos.fieldInfo(metric.getField());
                // if (metricType != MetricStat.COUNT) {
                // Need not initialize the metric reader with relevant doc id set iterator for COUNT metric type
                metricReader = new SequentialDocValuesIterator(
                    fieldProducerMap.get(metricFieldInfo.name).getSortedNumeric(metricFieldInfo)
                );
                // } else {
                // metricReader = new SequentialDocValuesIterator();
                // }

                metricReaders.add(metricReader);
            }
        }
        return metricReaders;
    }

    /**
     * Builds the star tree from the original segment documents
     *
     * @param fieldProducerMap           contain s the docValues producer to get docValues associated with each field
     * @param fieldNumberAcrossStarTrees
     * @param starTreeDocValuesConsumer
     * @throws IOException when we are unable to build star-tree
     */
    public void build(
        Map<String, DocValuesProducer> fieldProducerMap,
        AtomicInteger fieldNumberAcrossStarTrees,
        DocValuesConsumer starTreeDocValuesConsumer
    ) throws IOException {
        long startTime = System.currentTimeMillis();
        // logger.info("Star-tree build is a go with star tree field {}", starTreeField.getName());

        List<SequentialDocValuesIterator> metricReaders = getMetricReaders(writeState, fieldProducerMap);
        List<Dimension> dimensionsSplitOrder = starTreeField.getDimensionsOrder();
        SequentialDocValuesIterator[] dimensionReaders = new SequentialDocValuesIterator[dimensionsSplitOrder.size()];
        for (int i = 0; i < numDimensions; i++) {
            String dimension = dimensionsSplitOrder.get(i).getField();
            FieldInfo dimensionFieldInfo = writeState.fieldInfos.fieldInfo(dimension);
            dimensionReaders[i] = new SequentialDocValuesIterator(
                fieldProducerMap.get(dimensionFieldInfo.name).getSortedNumeric(dimensionFieldInfo)
            );
        }

        Iterator<StarTreeDocumentModified> starTreeDocumentIterator = sortAndAggregateSegmentDocuments(
            totalSegmentDocs,
            dimensionReaders,
            metricReaders
        );
        logger.info("Sorting and aggregating star-tree in ms : {}", (System.currentTimeMillis() - startTime));
        // logger.info("RAM USED : {}", this.ramBytesUsed());
        build(starTreeDocumentIterator, fieldNumberAcrossStarTrees, starTreeDocValuesConsumer);
        logger.info("Finished Building star-tree in ms : {}", (System.currentTimeMillis() - startTime));
    }

    /**
     * Builds the star tree using sorted and aggregated star-tree Documents
     *
     * @param starTreeDocumentIterator   contains the sorted and aggregated documents
     * @param fieldNumberAcrossStarTrees
     * @param starTreeDocValuesConsumer
     * @throws IOException when we are unable to build star-tree
     */
    public void build(
        Iterator<StarTreeDocumentModified> starTreeDocumentIterator,
        AtomicInteger fieldNumberAcrossStarTrees,
        DocValuesConsumer starTreeDocValuesConsumer
    ) throws IOException {
        int numSegmentStarTreeDocument = totalSegmentDocs;

        while (starTreeDocumentIterator.hasNext()) {
            appendToStarTree(starTreeDocumentIterator.next());
        }
        int numStarTreeDocument = numStarTreeDocs;
        logger.info("Generated star tree docs : [{}] from segment docs : [{}]", numStarTreeDocument, numSegmentStarTreeDocument);

        if (numStarTreeDocs == 0) {
            // serialize the star tree data
            serializeStarTree(numSegmentStarTreeDocument);
            return;
        }

        constructStarTree(rootNode, 0, numStarTreeDocs);
        int numStarTreeDocumentUnderStarNode = numStarTreeDocs - numStarTreeDocument;
        logger.info(
            "Finished constructing star-tree, got [ {} ] tree nodes and [ {} ] starTreeDocument under star-node",
            numStarTreeNodes,
            numStarTreeDocumentUnderStarNode
        );

        createAggregatedDocs(rootNode);
        int numAggregatedStarTreeDocument = numStarTreeDocs - numStarTreeDocument - numStarTreeDocumentUnderStarNode;
        logger.info("Finished creating aggregated documents : {}", numAggregatedStarTreeDocument);
        logger.info("RAM USED : {}", this.ramBytesUsed());

        // Create doc values indices in disk
        createSortedDocValuesIndices(starTreeDocValuesConsumer, fieldNumberAcrossStarTrees);

        // serialize star-tree
        serializeStarTree(numSegmentStarTreeDocument);
    }

    private void serializeStarTree(int numSegmentStarTreeDocument) throws IOException {
        // serialize the star tree data
        long dataFilePointer = dataOut.getFilePointer();
        long totalStarTreeDataLength = StarTreeBuilderUtils.serializeStarTree(dataOut, rootNode, numStarTreeNodes);

        // serialize the star tree meta
        StarTreeBuilderUtils.serializeStarTreeMetadata1(
            metaOut,
            starTreeField,
            writeState,
            metricAggregatorInfos,
            numSegmentStarTreeDocument,
            dataFilePointer,
            totalStarTreeDataLength
        );
    }

    private void createSortedDocValuesIndices(DocValuesConsumer docValuesConsumer, AtomicInteger fieldNumberAcrossStarTrees)
        throws IOException {
        List<SortedNumericDocValuesWriter> dimensionWriters = new ArrayList<>();
        List<SortedNumericDocValuesWriter> metricWriters = new ArrayList<>();
        FieldInfo[] dimensionFieldInfoList = new FieldInfo[starTreeField.getDimensionsOrder().size()];
        FieldInfo[] metricFieldInfoList = new FieldInfo[metricAggregatorInfos.size()];

        for (int i = 0; i < dimensionFieldInfoList.length; i++) {
            final FieldInfo fi = new FieldInfo(
                fullFieldNameForStarTreeDimensionsDocValues(starTreeField.getName(), starTreeField.getDimensionsOrder().get(i).getField()),
                fieldNumberAcrossStarTrees.getAndIncrement(),
                false,
                false,
                true,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
                DocValuesType.SORTED_NUMERIC,
                -1,
                Collections.emptyMap(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );
            dimensionFieldInfoList[i] = fi;
            dimensionWriters.add(new SortedNumericDocValuesWriter(fi, Counter.newCounter()));
        }
        for (int i = 0; i < metricAggregatorInfos.size(); i++) {
            FieldInfo fi = new FieldInfo(
                fullFieldNameForStarTreeMetricsDocValues(
                    starTreeField.getName(),
                    metricAggregatorInfos.get(i).getField(),
                    metricAggregatorInfos.get(i).getMetricStat().getTypeName()
                ),
                fieldNumberAcrossStarTrees.getAndIncrement(),
                false,
                false,
                true,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
                DocValuesType.SORTED_NUMERIC,
                -1,
                Collections.emptyMap(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );
            metricFieldInfoList[i] = fi;
            metricWriters.add(new SortedNumericDocValuesWriter(fi, Counter.newCounter()));
        }

        for (int docId = 0; docId < numStarTreeDocs; docId++) {
            StarTreeDocumentModified starTreeDocument = getStarTreeDocumentForCreatingDocValues(docId);
            for (int i = 0; i < starTreeDocument.dims.length; i++) {
                long val = starTreeDocument.dims[i];
                // if (val != null) {
                dimensionWriters.get(i).addValue(docId, val);
                // }
            }

            for (int i = 0; i < starTreeDocument.doubleMetrics.length; i++) {
                try {
                    switch (metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType()) {
                        // case LONG:
                        // metricWriters.get(i).addValue(docId, (Long) starTreeDocument.metrics[i]);
                        // break;
                        case DOUBLE:
                            metricWriters.get(i).addValue(docId, NumericUtils.doubleToSortableLong(starTreeDocument.doubleMetrics[i]));
                            break;
                        default:
                            throw new IllegalStateException("Unknown metric doc value type");
                    }
                } catch (IllegalArgumentException e) {
                    logger.info("could not parse the value, exiting creation of star tree");
                }
            }
        }

        addStarTreeDocValueFields(docValuesConsumer, dimensionWriters, dimensionFieldInfoList, starTreeField.getDimensionsOrder().size());
        addStarTreeDocValueFields(docValuesConsumer, metricWriters, metricFieldInfoList, metricAggregatorInfos.size());
    }

    private void addStarTreeDocValueFields(
        DocValuesConsumer docValuesConsumer,
        List<SortedNumericDocValuesWriter> docValuesWriters,
        FieldInfo[] fieldInfoList,
        int fieldCount
    ) throws IOException {
        for (int i = 0; i < fieldCount; i++) {
            final int increment = i;
            DocValuesProducer docValuesProducer = new EmptyDocValuesProducer() {
                @Override
                public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
                    return docValuesWriters.get(increment).getDocValues();
                }
            };
            docValuesConsumer.addSortedNumericField(fieldInfoList[i], docValuesProducer);
        }
    }

    private long getTimeStampVal(final String fieldName, final long val) {
        long roundedDate = 0;
        long ratio = 0;
        switch (fieldName) {
            case "@timestamp1":
                ratio = ChronoField.MINUTE_OF_HOUR.getBaseUnit().getDuration().toMillis();
                roundedDate = DateUtils.roundFloor(val, ratio);
                return roundedDate;
            case "hour":
                ratio = ChronoField.HOUR_OF_DAY.getBaseUnit().getDuration().toMillis();
                roundedDate = DateUtils.roundFloor(val, ratio);
                return roundedDate;
            case "day":
                ratio = ChronoField.DAY_OF_MONTH.getBaseUnit().getDuration().toMillis();
                roundedDate = DateUtils.roundFloor(val, ratio);
                return roundedDate;
            case "month":
                roundedDate = DateUtils.roundMonthOfYear(val);
                return roundedDate;
            case "year":
                roundedDate = DateUtils.roundYear(val);
                return roundedDate;
            default:
                return val;
        }
    }

    /**
     * Adds a document to the star-tree.
     *
     * @param starTreeDocument star tree document to be added
     * @throws IOException if an I/O error occurs while adding the document
     */
    public abstract void appendStarTreeDocument(StarTreeDocumentModified starTreeDocument) throws IOException;

    /**
     * Returns the document of the given document id in the star-tree.
     *
     * @param docId document id
     * @return star tree document
     * @throws IOException if an I/O error occurs while fetching the star-tree document
     */
    public abstract StarTreeDocumentModified getStarTreeDocument(int docId) throws IOException;

    public abstract StarTreeDocumentModified getStarTreeDocumentForCreatingDocValues(int docId) throws IOException;

    /**
     * Retrieves the list of star-tree documents in the star-tree.
     *
     * @return Star tree documents
     */
    public abstract List<StarTreeDocumentModified> getStarTreeDocuments();

    /**
     * Returns the value of the dimension for the given dimension id and document in the star-tree.
     *
     * @param docId       document id
     * @param dimensionId dimension id
     * @return dimension value
     */
    public abstract long getDimensionValue(int docId, int dimensionId) throws IOException;

    /**
     * Sorts and aggregates all the documents in the segment as per the configuration, and returns a star-tree document iterator for all the
     * aggregated star-tree documents.
     *
     * @param numDocs          number of documents in the given segment
     * @param dimensionReaders List of docValues readers to read dimensions from the segment
     * @param metricReaders    List of docValues readers to read metrics from the segment
     * @return Iterator for the aggregated star-tree document
     */
    public abstract Iterator<StarTreeDocumentModified> sortAndAggregateSegmentDocuments(
        int numDocs,
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders
    ) throws IOException;

    /**
     * Generates aggregated star-tree documents for star-node. This inserts null for the star-node dimension and generates aggregated documents based on the remaining dimensions
     *
     * @param startDocId  start document id (inclusive) in the star-tree
     * @param endDocId    end document id (exclusive) in the star-tree
     * @param dimensionId dimension id of the star-node
     * @return Iterator for the aggregated star-tree documents
     */
    public abstract Iterator<StarTreeDocumentModified> generateStarTreeDocumentsForStarNode(int startDocId, int endDocId, int dimensionId)
        throws IOException;

    protected StarTreeDocumentModified getSegmentStarTreeDocument(
        int currentDocId,
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders
    ) throws IOException {
        long[] dimensions = getStarTreeDimensionsFromSegment(currentDocId, dimensionReaders);
        long[] metrics = getStarTreeMetricsFromSegment(currentDocId, metricReaders);
        return new StarTreeDocumentModified(dimensions, metrics);
    }

    protected void getSegmentStarTreeDocument(
        int currentDocId,
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders,
        StarTreeDocumentModified document
    ) throws IOException {
        getStarTreeDimensionsFromSegment(currentDocId, dimensionReaders, document.dims);
        getStarTreeMetricsFromSegment(currentDocId, metricReaders, document.longMetrics);
    }

    /**
     * Returns the dimension values for the next document from the segment
     *
     * @return dimension values for each of the star-tree dimension
     * @throws IOException when we are unable to iterate to the next doc for the given dimension readers
     */
    long[] getStarTreeDimensionsFromSegment(int currentDocId, SequentialDocValuesIterator[] dimensionReaders) throws IOException {
        long[] dimensions = new long[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            if (dimensionReaders[i] != null) {
                try {
                    dimensionReaders[i].nextDoc(currentDocId);
                } catch (IOException e) {
                    logger.error("unable to iterate to next doc", e);
                    throw new RuntimeException("unable to iterate to next doc", e);
                } catch (Exception e) {
                    logger.error("unable to read the dimension values from the segment", e);
                    throw new IllegalStateException("unable to read the dimension values from the segment", e);
                }
                dimensions[i] = dimensionReaders[i].value(currentDocId);
                dimensions[i] = getTimeStampVal(starTreeField.getDimensionsOrder().get(i).getField(), dimensions[i]);
            } else {
                throw new IllegalStateException("dimension readers are empty");
            }
        }
        return dimensions;
    }

    void getStarTreeDimensionsFromSegment(int currentDocId, SequentialDocValuesIterator[] dimensionReaders, long[] dimensions)
        throws IOException {
        for (int i = 0; i < numDimensions; i++) {
            if (dimensionReaders[i] != null) {
                try {
                    dimensionReaders[i].nextDoc(currentDocId);
                } catch (IOException e) {
                    logger.error("unable to iterate to next doc", e);
                    throw new RuntimeException("unable to iterate to next doc", e);
                } catch (Exception e) {
                    logger.error("unable to read the dimension values from the segment", e);
                    throw new IllegalStateException("unable to read the dimension values from the segment", e);
                }
                dimensions[i] = dimensionReaders[i].value(currentDocId);
                // dimensions[i] = getTimeStampVal(starTreeField.getDimensionsOrder().get(i).getField(), dimensions[i]);
            } else {
                throw new IllegalStateException("dimension readers are empty");
            }
        }
    }

    private void getStarTreeMetricsFromSegment(int currentDocId, List<SequentialDocValuesIterator> metricsReaders, long[] metrics)
        throws IOException {
        for (int i = 0; i < numMetrics; i++) {
            SequentialDocValuesIterator metricStatReader = metricsReaders.get(i);
            if (metricStatReader != null) {
                try {
                    metricStatReader.nextDoc(currentDocId);
                } catch (IOException e) {
                    logger.error("unable to iterate to next doc", e);
                    throw new RuntimeException("unable to iterate to next doc", e);
                } catch (Exception e) {
                    logger.error("unable to read the metric values from the segment", e);
                    throw new IllegalStateException("unable to read the metric values from the segment", e);
                }
                metrics[i] = metricStatReader.value(currentDocId);
            } else {
                throw new IllegalStateException("metric readers are empty");
            }
        }
    }

    /**
     * Returns the metric values for the next document from the segment
     *
     * @return metric values for each of the star-tree metric
     * @throws IOException when we are unable to iterate to the next doc for the given metric readers
     */
    private long[] getStarTreeMetricsFromSegment(int currentDocId, List<SequentialDocValuesIterator> metricsReaders) throws IOException {
        long[] metrics = new long[numMetrics];
        for (int i = 0; i < numMetrics; i++) {
            SequentialDocValuesIterator metricStatReader = metricsReaders.get(i);
            if (metricStatReader != null) {
                try {
                    metricStatReader.nextDoc(currentDocId);
                } catch (IOException e) {
                    logger.error("unable to iterate to next doc", e);
                    throw new RuntimeException("unable to iterate to next doc", e);
                } catch (Exception e) {
                    logger.error("unable to read the metric values from the segment", e);
                    throw new IllegalStateException("unable to read the metric values from the segment", e);
                }
                metrics[i] = metricStatReader.value(currentDocId);
            } else {
                throw new IllegalStateException("metric readers are empty");
            }
        }
        return metrics;
    }

    /**
     * Merges a star-tree document from the segment into an aggregated star-tree document.
     * A new aggregated star-tree document is created if the aggregated segment document is null.
     *
     * @param aggregatedSegmentDocument aggregated star-tree document
     * @param segmentDocument           segment star-tree document
     * @return merged star-tree document
     */
    protected StarTreeDocumentModified reduceSegmentStarTreeDocuments(
        StarTreeDocumentModified aggregatedSegmentDocument,
        StarTreeDocumentModified segmentDocument
    ) {
        if (aggregatedSegmentDocument == null) {
            double[] metrics = new double[numMetrics];
            for (int i = 0; i < numMetrics; i++) {
                try {
                    ValueAggregator1 metricValueAggregator = metricAggregatorInfos.get(i).getValueAggregators();
                    StarTreeNumericType starTreeNumericType = metricAggregatorInfos.get(i).getAggregatedValueType();
                    metrics[i] = metricValueAggregator.getInitialAggregatedValueForSegmentDocValue(
                        segmentDocument.longMetrics[i],
                        starTreeNumericType
                    );
                } catch (Exception e) {
                    logger.error("Cannot parse initial segment doc value", e);
                    throw new IllegalStateException("Cannot parse initial segment doc value [" + segmentDocument.longMetrics[i] + "]");
                }
            }
            return new StarTreeDocumentModified(segmentDocument.dims, metrics);
        } else {
            for (int i = 0; i < numMetrics; i++) {
                try {
                    ValueAggregator1 metricValueAggregator = metricAggregatorInfos.get(i).getValueAggregators();
                    StarTreeNumericType starTreeNumericType = metricAggregatorInfos.get(i).getAggregatedValueType();
                    aggregatedSegmentDocument.doubleMetrics[i] = metricValueAggregator.mergeAggregatedValueAndSegmentValue(
                        aggregatedSegmentDocument.doubleMetrics[i],
                        segmentDocument.longMetrics[i],
                        starTreeNumericType
                    );
                } catch (Exception e) {
                    logger.error("Cannot apply segment doc value for aggregation", e);
                    throw new IllegalStateException(
                        "Cannot apply segment doc value for aggregation [" + segmentDocument.longMetrics[i] + "]"
                    );
                }
            }
            return aggregatedSegmentDocument;
        }
    }

    protected StarTreeDocumentModified reduceSegmentStarTreeDocuments(
        StarTreeDocumentModified aggregatedSegmentDocument,
        StarTreeDocumentModified segmentDocument,
        boolean flag
    ) {
        if (flag) {
            for (int i = 0; i < numMetrics; i++) {
                try {
                    ValueAggregator1 metricValueAggregator = metricAggregatorInfos.get(i).getValueAggregators();
                    StarTreeNumericType starTreeNumericType = metricAggregatorInfos.get(i).getAggregatedValueType();
                    aggregatedSegmentDocument.doubleMetrics[i] = metricValueAggregator.getInitialAggregatedValueForSegmentDocValue(
                        segmentDocument.longMetrics[i],
                        starTreeNumericType
                    );
                } catch (Exception e) {
                    logger.error("Cannot parse initial segment doc value", e);
                    throw new IllegalStateException("Cannot parse initial segment doc value [" + segmentDocument.longMetrics[i] + "]");
                }
            }
            aggregatedSegmentDocument.dims = segmentDocument.dims;
            return aggregatedSegmentDocument;
        } else {
            for (int i = 0; i < numMetrics; i++) {
                try {
                    ValueAggregator1 metricValueAggregator = metricAggregatorInfos.get(i).getValueAggregators();
                    StarTreeNumericType starTreeNumericType = metricAggregatorInfos.get(i).getAggregatedValueType();
                    aggregatedSegmentDocument.doubleMetrics[i] = metricValueAggregator.mergeAggregatedValueAndSegmentValue(
                        aggregatedSegmentDocument.doubleMetrics[i],
                        segmentDocument.longMetrics[i],
                        starTreeNumericType
                    );
                } catch (Exception e) {
                    logger.error("Cannot apply segment doc value for aggregation", e);
                    throw new IllegalStateException(
                        "Cannot apply segment doc value for aggregation [" + segmentDocument.longMetrics[i] + "]"
                    );
                }
            }
            return aggregatedSegmentDocument;
        }
    }

    /**
     * Safely converts the metric value of object type to long.
     *
     * @param metric value of the metric
     * @return converted metric value to long
     */
    private static long getLong(Object metric) {

        Long metricValue = null;
        try {
            if (metric instanceof Long) {
                metricValue = (long) metric;
            } else if (metric != null) {
                metricValue = Long.valueOf(String.valueOf(metric));
            }
        } catch (Exception e) {
            throw new IllegalStateException("unable to cast segment metric", e);
        }

        if (metricValue == null) {
            throw new IllegalStateException("unable to cast segment metric");
        }
        return metricValue;
    }

    /**
     * Merges a star-tree document into an aggregated star-tree document.
     * A new aggregated star-tree document is created if the aggregated document is null.
     *
     * @param aggregatedDocument aggregated star-tree document
     * @param starTreeDocument   segment star-tree document
     * @return merged star-tree document
     */
    public StarTreeDocumentModified reduceStarTreeDocuments(
        StarTreeDocumentModified aggregatedDocument,
        StarTreeDocumentModified starTreeDocument
    ) {
        // aggregate the documents
        if (aggregatedDocument == null) {
            long[] dimensions = Arrays.copyOf(starTreeDocument.dims, numDimensions);
            double[] metrics = new double[numMetrics];
            for (int i = 0; i < numMetrics; i++) {
                try {
                    metrics[i] = metricAggregatorInfos.get(i)
                        .getValueAggregators()
                        .getInitialAggregatedValue(starTreeDocument.doubleMetrics[i]);
                } catch (Exception e) {
                    logger.error("Cannot get value for aggregation", e);
                    throw new IllegalStateException("Cannot get value for aggregation[" + starTreeDocument.doubleMetrics[i] + "]");
                }
            }
            return new StarTreeDocumentModified(dimensions, metrics);
        } else {
            for (int i = 0; i < numMetrics; i++) {
                try {
                    aggregatedDocument.doubleMetrics[i] = metricAggregatorInfos.get(i)
                        .getValueAggregators()
                        .mergeAggregatedValues(starTreeDocument.doubleMetrics[i], aggregatedDocument.doubleMetrics[i]);
                } catch (Exception e) {
                    logger.error("Cannot apply value to aggregated document for aggregation", e);
                    throw new IllegalStateException(
                        "Cannot apply value to aggregated document for aggregation [" + starTreeDocument.doubleMetrics[i] + "]"
                    );
                }
            }
            return aggregatedDocument;
        }
    }

    // public StarTreeDocumentModified reduceStarTreeDocuments(StarTreeDocumentModified aggregatedDocument, StarTreeDocumentModified
    // starTreeDocument, boolean flag) {
    // // aggregate the documents
    // if (flag) {
    // aggregatedDocument.dims = starTreeDocument.dims;
    // for (int i = 0; i < numMetrics; i++) {
    // try {
    // aggregatedDocument.metrics[i] = metricAggregatorInfos.get(i)
    // .getValueAggregators()
    // .getInitialAggregatedValue(starTreeDocument.doubleMetrics[i]);
    // } catch (Exception e) {
    // logger.error("Cannot get value for aggregation", e);
    // throw new IllegalStateException("Cannot get value for aggregation[" + starTreeDocument.doubleMetrics[i] + "]");
    // }
    // }
    // return aggregatedDocument;
    // } else {
    // for (int i = 0; i < numMetrics; i++) {
    // try {
    // aggregatedDocument.metrics[i] = metricAggregatorInfos.get(i)
    // .getValueAggregators()
    // .mergeAggregatedValues(starTreeDocument.doubleMetrics[i], aggregatedDocument.doubleMetrics[i]);
    // } catch (Exception e) {
    // logger.error("Cannot apply value to aggregated document for aggregation", e);
    // throw new IllegalStateException(
    // "Cannot apply value to aggregated document for aggregation [" + starTreeDocument.doubleMetrics[i] + "]"
    // );
    // }
    // }
    // return aggregatedDocument;
    // }
    // }

    /**
     * Adds a document to star-tree
     *
     * @param starTreeDocument star-tree document
     * @throws IOException throws an exception if we are unable to add the doc
     */
    private void appendToStarTree(StarTreeDocumentModified starTreeDocument) throws IOException {
        appendStarTreeDocument(starTreeDocument);
        numStarTreeDocs++;
    }

    /**
     * Returns a new star-tree node
     *
     * @return return new star-tree node
     */
    private StarTreeBuilderUtils.TreeNode getNewNode() {
        numStarTreeNodes++;
        return new StarTreeBuilderUtils.TreeNode();
    }

    /**
     * Implements the algorithm to construct a star-tree
     *
     * @param node       star-tree node
     * @param startDocId start document id
     * @param endDocId   end document id
     * @throws IOException throws an exception if we are unable to construct the tree
     */
    private void constructStarTree(StarTreeBuilderUtils.TreeNode node, int startDocId, int endDocId) throws IOException {

        int childDimensionId = node.dimensionId + 1;
        if (childDimensionId == numDimensions) {
            return;
        }

        // Construct all non-star children nodes
        node.childDimensionId = childDimensionId;
        Map<Long, StarTreeBuilderUtils.TreeNode> children = constructNonStarNodes(startDocId, endDocId, childDimensionId);
        node.children = children;

        // Construct star-node if required
        if (!skipStarNodeCreationForDimensions.contains(childDimensionId) && children.size() > 1) {
            children.put((long) StarTreeBuilderUtils.ALL, constructStarNode(startDocId, endDocId, childDimensionId));
        }

        // Further split on child nodes if required
        for (StarTreeBuilderUtils.TreeNode child : children.values()) {
            if (child.endDocId - child.startDocId > maxLeafDocuments) {
                constructStarTree(child, child.startDocId, child.endDocId);
            }
        }
    }

    /**
     * Constructs non star tree nodes
     *
     * @param startDocId  start document id (inclusive)
     * @param endDocId    end document id (exclusive)
     * @param dimensionId id of the dimension in the star tree
     * @return root node with non-star nodes constructed
     * @throws IOException throws an exception if we are unable to construct non-star nodes
     */
    private Map<Long, StarTreeBuilderUtils.TreeNode> constructNonStarNodes(int startDocId, int endDocId, int dimensionId)
        throws IOException {
        Map<Long, StarTreeBuilderUtils.TreeNode> nodes = new HashMap<>();
        int nodeStartDocId = startDocId;
        long nodeDimensionValue = getDimensionValue(startDocId, dimensionId);
        for (int i = startDocId + 1; i < endDocId; i++) {
            long dimensionValue = getDimensionValue(i, dimensionId);
            if (!(dimensionValue == nodeDimensionValue)) {
                StarTreeBuilderUtils.TreeNode child = getNewNode();
                child.dimensionId = dimensionId;
                child.dimensionValue = nodeDimensionValue;
                child.startDocId = nodeStartDocId;
                child.endDocId = i;
                nodes.put(nodeDimensionValue, child);

                nodeStartDocId = i;
                nodeDimensionValue = dimensionValue;
            }
        }
        StarTreeBuilderUtils.TreeNode lastNode = getNewNode();
        lastNode.dimensionId = dimensionId;
        lastNode.dimensionValue = nodeDimensionValue;
        lastNode.startDocId = nodeStartDocId;
        lastNode.endDocId = endDocId;
        nodes.put(nodeDimensionValue, lastNode);
        return nodes;
    }

    /**
     * Constructs star tree nodes
     *
     * @param startDocId  start document id (inclusive)
     * @param endDocId    end document id (exclusive)
     * @param dimensionId id of the dimension in the star tree
     * @return root node with star nodes constructed
     * @throws IOException throws an exception if we are unable to construct non-star nodes
     */
    private StarTreeBuilderUtils.TreeNode constructStarNode(int startDocId, int endDocId, int dimensionId) throws IOException {
        StarTreeBuilderUtils.TreeNode starNode = getNewNode();
        starNode.dimensionId = dimensionId;
        starNode.dimensionValue = StarTreeBuilderUtils.ALL;
        starNode.isStarNode = true;
        starNode.startDocId = numStarTreeDocs;
        Iterator<StarTreeDocumentModified> starTreeDocumentIterator = generateStarTreeDocumentsForStarNode(
            startDocId,
            endDocId,
            dimensionId
        );
        while (starTreeDocumentIterator.hasNext()) {
            appendToStarTree(starTreeDocumentIterator.next());
        }
        starNode.endDocId = numStarTreeDocs;
        return starNode;
    }

    /**
     * Returns aggregated star-tree document
     *
     * @param node star-tree node
     * @return aggregated star-tree documents
     * @throws IOException throws an exception upon failing to create new aggregated docs based on star tree
     */
    private StarTreeDocumentModified createAggregatedDocs(StarTreeBuilderUtils.TreeNode node) throws IOException {
        StarTreeDocumentModified aggregatedStarTreeDocument = null;
        if (node.children == null) {
            // For leaf node

            if (node.startDocId == node.endDocId - 1) {
                // If it has only one document, use it as the aggregated document
                aggregatedStarTreeDocument = getStarTreeDocument(node.startDocId);
                node.aggregatedDocId = node.startDocId;
            } else {
                // If it has multiple documents, aggregate all of them
                for (int i = node.startDocId; i < node.endDocId; i++) {
                    aggregatedStarTreeDocument = reduceStarTreeDocuments(aggregatedStarTreeDocument, getStarTreeDocument(i));
                }
                if (null == aggregatedStarTreeDocument) {
                    throw new IllegalStateException("aggregated star-tree document is null after reducing the documents");
                }
                for (int i = node.dimensionId + 1; i < numDimensions; i++) {
                    aggregatedStarTreeDocument.dims[i] = STAR_IN_DOC_VALUES_INDEX;
                }
                node.aggregatedDocId = numStarTreeDocs;
                appendToStarTree(aggregatedStarTreeDocument);
            }
        } else {
            // For non-leaf node
            if (node.children.containsKey((long) StarTreeBuilderUtils.ALL)) {
                // If it has star child, use the star child aggregated document directly
                for (StarTreeBuilderUtils.TreeNode child : node.children.values()) {
                    if (child.isStarNode) {
                        aggregatedStarTreeDocument = createAggregatedDocs(child);
                        node.aggregatedDocId = child.aggregatedDocId;
                    } else {
                        createAggregatedDocs(child);
                    }
                }
            } else {
                // If no star child exists, aggregate all aggregated documents from non-star children
                for (StarTreeBuilderUtils.TreeNode child : node.children.values()) {
                    aggregatedStarTreeDocument = reduceStarTreeDocuments(aggregatedStarTreeDocument, createAggregatedDocs(child));
                }
                if (null == aggregatedStarTreeDocument) {
                    throw new IllegalStateException("aggregated star-tree document is null after reducing the documents");
                }
                for (int i = node.dimensionId + 1; i < numDimensions; i++) {
                    aggregatedStarTreeDocument.dims[i] = STAR_IN_DOC_VALUES_INDEX;
                }
                node.aggregatedDocId = numStarTreeDocs;
                appendToStarTree(aggregatedStarTreeDocument);
            }
        }
        return aggregatedStarTreeDocument;
    }

    /**
     * Handles the dimension of date time field type
     *
     * @param fieldName name of the field
     * @param val       value of the field
     * @return returns the converted dimension of the field to a particular granularity
     */
    private long handleDateDimension(final String fieldName, final long val) {
        // TODO: handle timestamp granularity
        return val;
    }

    public void close() throws IOException {

    }

}
