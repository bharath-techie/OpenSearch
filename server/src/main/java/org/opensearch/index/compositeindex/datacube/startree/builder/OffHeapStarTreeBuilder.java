
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericTypeConverters;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Off heap implementation of star tree builder
 *
 * Segment documents are stored in single file - segment.documents for sorting and aggregation ( we create a doc id array
 * and swap doc ids in array during sorting based on the actual segment document contents in the file )
 *
 * Star tree documents are stored in multiple files as the algo is:
 * 1. Initially create a bunch of aggregated documents based on segment documents
 * 2. Sometimes, for example in generateStarTreeDocumentsForStarNode, we need to read the newly aggregated documents
 * and create aggregated star documents and append
 * 3. Repeat until we have all combinations
 *
 * So for cases , where we need to read the previously written star documents in star-tree.documents file , we close the
 * star.document file and read the values and write the derived values on a new star-tree.documents file.
 * This is because:
 *
 * We cannot keep the 'IndexOutput' open and create a 'IndexInput' to read the content as some of the recent content
 * will not be visible in the reader. So we need to 'close' the 'IndexOutput' before we create a 'IndexInput'
 * And we cannot reopen 'IndexOutput' - so we create a new file for new appends.
 *
 *
 * We keep these set of files and maintain a tracker array to track the start doc id for each file.
 *
 * Once the files reach the threshold we merge the files.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class OffHeapStarTreeBuilder extends BaseStarTreeBuilder {
    private static final Logger logger = LogManager.getLogger(OffHeapStarTreeBuilder.class);
    private static final String SEGMENT_DOC_FILE_NAME = "segment.documents";
    private static final String STAR_TREE_DOC_FILE_NAME = "star-tree.documents";
    // TODO : Should this be via settings ?
    private static final int FILE_COUNT_THRESHOLD = 10;
    private final List<Integer> starTreeDocumentOffsets;
    private int numReadableStarTreeDocuments;
    final IndexOutput segmentDocsFileOutput;
    IndexOutput starTreeDocsFileOutput;
    IndexInput starTreeDocsFileInput;
    RandomAccessInput segmentRandomInput;
    private RandomAccessInput starTreeDocsFileRandomInput;
    SegmentWriteState state;
    Map<String, Integer> fileToByteSizeMap;
    int starTreeFileCount = -1;
    int prevStartDocId = Integer.MAX_VALUE;
    int currBytes = 0;
    int docSizeInBytes = -1;
    TrackingDirectoryWrapper tmpDirectory;
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(OffHeapStarTreeBuilder.class);

    /**
     * Builds star tree based on star tree field configuration consisting of dimensions, metrics and star tree index
     * specific configuration.
     *
     * @param starTreeField holds the configuration for the star tree
     * @param state         stores the segment write state
     * @param mapperService helps to find the original type of the field
     */
    protected OffHeapStarTreeBuilder(StarTreeField starTreeField, SegmentWriteState state, MapperService mapperService) throws IOException {
        super(starTreeField, state, mapperService);
        this.state = state;
        this.tmpDirectory = new TrackingDirectoryWrapper(state.directory);
        fileToByteSizeMap = new LinkedHashMap<>(); // maintain order
        starTreeDocsFileOutput = createStarTreeDocumentsFileOutput();
        segmentDocsFileOutput = tmpDirectory.createTempOutput(SEGMENT_DOC_FILE_NAME, state.segmentSuffix, state.context);
        starTreeDocumentOffsets = new ArrayList<>();
    }

    /**
     * Creates a new star tree document temporary file to store star tree documents.
     */
    IndexOutput createStarTreeDocumentsFileOutput() throws IOException {
        starTreeFileCount++;
        return tmpDirectory.createTempOutput(STAR_TREE_DOC_FILE_NAME + starTreeFileCount, state.segmentSuffix, state.context);
    }

    @Override
    public void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException {
        int bytes = writeStarTreeDocument(starTreeDocument, starTreeDocsFileOutput);
        // System.out.println(starTreeDocument);
        if (docSizeInBytes == -1) {
            docSizeInBytes = bytes;
        }
        assert docSizeInBytes == bytes;
        starTreeDocumentOffsets.add(currBytes);
        currBytes += bytes;
    }

    @Override
    public void build(List<StarTreeValues> starTreeValuesSubs) throws IOException {
        try {
            build(mergeStarTrees(starTreeValuesSubs));
        } finally {
            IOUtils.deleteFilesIgnoringExceptions(tmpDirectory.getDelegate(), tmpDirectory.getCreatedFiles());
        }
    }

    /**
     * Sorts and aggregates the star-tree documents from multiple segments and builds star tree based on the newly
     * aggregated star-tree documents
     *
     * @param starTreeValuesSubs StarTreeValues from multiple segments
     * @return iterator of star tree documents
     */
    Iterator<StarTreeDocument> mergeStarTrees(List<StarTreeValues> starTreeValuesSubs) throws IOException {
        int docBytesLength = 0;
        int numDocs = 0;
        int[] sortedDocIds;
        try {
            for (StarTreeValues starTreeValues : starTreeValuesSubs) {
                boolean endOfDoc = false;
                List<Dimension> dimensionsSplitOrder = starTreeValues.getStarTreeField().getDimensionsOrder();
                SequentialDocValuesIterator[] dimensionReaders = new SequentialDocValuesIterator[starTreeValues.getStarTreeField()
                    .getDimensionsOrder()
                    .size()];
                for (int i = 0; i < dimensionsSplitOrder.size(); i++) {
                    String dimension = dimensionsSplitOrder.get(i).getField();
                    dimensionReaders[i] = new SequentialDocValuesIterator(starTreeValues.getDimensionDocValuesIteratorMap().get(dimension));
                }
                List<SequentialDocValuesIterator> metricReaders = new ArrayList<>();
                for (Map.Entry<String, DocIdSetIterator> metricDocValuesEntry : starTreeValues.getMetricDocValuesIteratorMap().entrySet()) {
                    metricReaders.add(new SequentialDocValuesIterator(metricDocValuesEntry.getValue()));
                }
                int currentDocId = 0;
                while (!endOfDoc) {
                    Long[] dims = new Long[starTreeValues.getStarTreeField().getDimensionsOrder().size()];
                    int i = 0;
                    for (SequentialDocValuesIterator dimensionDocValueIterator : dimensionReaders) {
                        int doc = dimensionDocValueIterator.nextDoc(currentDocId);
                        Long val = dimensionDocValueIterator.value(currentDocId);
                        // TODO : figure out how to identify a row with star tree docs here
                        endOfDoc = (doc == DocIdSetIterator.NO_MORE_DOCS);
                        if (endOfDoc) {
                            break;
                        }
                        dims[i] = val;
                        i++;
                    }
                    if (endOfDoc) {
                        break;
                    }
                    i = 0;
                    Object[] metrics = new Object[metricReaders.size()];
                    for (SequentialDocValuesIterator metricDocValuesIterator : metricReaders) {
                        metricDocValuesIterator.nextDoc(currentDocId);
                        metrics[i] = NumericUtils.sortableLongToDouble(metricDocValuesIterator.value(currentDocId));
                        i++;
                    }

                    StarTreeDocument starTreeDocument = new StarTreeDocument(dims, metrics);
                    int bytes = writeStarTreeDocument(starTreeDocument, segmentDocsFileOutput);
                    numDocs++;
                    docBytesLength = bytes;
                    currentDocId++;
                }
            }
            sortedDocIds = new int[numDocs];
            for (int i = 0; i < numDocs; i++) {
                sortedDocIds[i] = i;
            }
        } finally {
            segmentDocsFileOutput.close();
        }

        if (numDocs == 0) {
            return new ArrayList<StarTreeDocument>().iterator();
        }

        return sortDocuments(sortedDocIds, numDocs, docBytesLength, true);
    }

    private Iterator<StarTreeDocument> sortDocuments(int[] sortedDocIds, int numDocs, int docBytesLength) throws IOException {
        return sortDocuments(sortedDocIds, numDocs, docBytesLength, false);
    }

    private Iterator<StarTreeDocument> sortDocuments(int[] sortedDocIds, int numDocs, int docBytesLength, boolean isMerge)
        throws IOException {
        IndexInput segmentDocsFileInput = tmpDirectory.openInput(segmentDocsFileOutput.getName(), state.context);
        final long documentBytes = docBytesLength;
        segmentRandomInput = segmentDocsFileInput.randomAccessSlice(0, segmentDocsFileInput.length());
        if (sortedDocIds == null) {
            logger.debug("Sorted doc ids array is null");
            return new ArrayList<StarTreeDocument>().iterator();
        }
        new IntroSorter() {
            private long[] dimensions;

            @Override
            protected void swap(int i, int j) {
                int temp = sortedDocIds[i];
                sortedDocIds[i] = sortedDocIds[j];
                sortedDocIds[j] = temp;
            }

            @Override
            protected void setPivot(int i) {
                long offset = (long) sortedDocIds[i] * documentBytes;
                dimensions = new long[starTreeField.getDimensionsOrder().size()];
                try {
                    for (int j = 0; j < dimensions.length; j++) {
                        dimensions[j] = segmentRandomInput.readLong(offset + (long) j * Long.BYTES);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Sort documents failed : " + e); // TODO: handle this better
                }
            }

            @Override
            protected int comparePivot(int j) {
                long offset = (long) sortedDocIds[j] * documentBytes;
                try {
                    for (int i = 0; i < dimensions.length; i++) {
                        long dimension = segmentRandomInput.readLong(offset + (long) i * Long.BYTES);
                        if (dimensions[i] != dimension) {
                            return Long.compare(dimensions[i], dimension);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Sort documents failed : " + e); // TODO: handle this better
                }
                return 0;
            }
        }.sort(0, numDocs);

        // Create an iterator for aggregated documents
        return new Iterator<StarTreeDocument>() {
            boolean _hasNext = true;
            StarTreeDocument currentDocument;

            {
                assert sortedDocIds != null;
                currentDocument = getSegmentStarTreeDocument(sortedDocIds[0], documentBytes, isMerge);
            }

            int _docId = 1;

            @Override
            public boolean hasNext() {
                return _hasNext;
            }

            @Override
            public StarTreeDocument next() {
                StarTreeDocument next = reduceSegmentStarTreeDocuments(null, currentDocument, isMerge);
                while (_docId < numDocs) {
                    StarTreeDocument doc = null;
                    try {
                        doc = getSegmentStarTreeDocument(sortedDocIds[_docId++], documentBytes, isMerge);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                        // TODO : handle this block better - how to handle exceptions ?
                    }
                    if (!Arrays.equals(doc.dimensions, next.dimensions)) {
                        currentDocument = doc;
                        return next;
                    } else {
                        next = reduceSegmentStarTreeDocuments(next, doc, isMerge);
                    }
                }
                _hasNext = false;
                IOUtils.closeWhileHandlingException(segmentDocsFileInput);
                IOUtils.deleteFilesIgnoringExceptions(tmpDirectory, List.of(segmentDocsFileOutput.getName()));
                return next;
            }
        };
    }

    public StarTreeDocument getSegmentStarTreeDocument(int docID, long documentBytes, boolean isMerge) throws IOException {
        return readSegmentStarTreeDocument(segmentRandomInput, docID * documentBytes, isMerge);
    }

    @Override
    public StarTreeDocument getStarTreeDocument(int docId) throws IOException {
        ensureBufferReadable(docId);
        return readStarTreeDocument(starTreeDocsFileRandomInput, starTreeDocumentOffsets.get(docId));
    }

    @Override
    public List<StarTreeDocument> getStarTreeDocuments() {
        // TODO : this is only used for testing
        return null;
    }

    // TODO: should this be just long?
    @Override
    public Long getDimensionValue(int docId, int dimensionId) throws IOException {
        ensureBufferReadable(docId);
        return starTreeDocsFileRandomInput.readLong((starTreeDocumentOffsets.get(docId) + ((long) dimensionId * Long.BYTES)));
    }

    /**
     * Sorts and aggregates all the documents of the segment based on dimension and metrics configuration
     *
     * @param numDocs          number of documents in the given segment
     * @param dimensionReaders List of docValues readers to read dimensions from the segment
     * @param metricReaders    List of docValues readers to read metrics from the segment
     * @return Iterator of star-tree documents
     */
    @Override
    public Iterator<StarTreeDocument> sortAndAggregateSegmentDocuments(
        int numDocs,
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders
    ) throws IOException {
        // Write all dimensions for segment documents into the buffer, and sort all documents using an int
        // array
        int documentBytesLength = 0;
        int[] sortedDocIds = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            sortedDocIds[i] = i;
        }

        try {
            for (int i = 0; i < numDocs; i++) {
                StarTreeDocument document = getSegmentStarTreeDocument(i, dimensionReaders, metricReaders);
                documentBytesLength = writeSegmentStarTreeDocument(document, segmentDocsFileOutput);
            }
        } finally {
            segmentDocsFileOutput.close();
        }

        // Create an iterator for aggregated documents
        return sortDocuments(sortedDocIds, numDocs, documentBytesLength);
    }

    /**
     * Generates a star-tree for a given star-node
     *
     * @param startDocId  Start document id in the star-tree
     * @param endDocId    End document id (exclusive) in the star-tree
     * @param dimensionId Dimension id of the star-node
     * @return iterator for star-tree documents of star-node
     * @throws IOException throws when unable to generate star-tree for star-node
     */
    @Override
    public Iterator<StarTreeDocument> generateStarTreeDocumentsForStarNode(int startDocId, int endDocId, int dimensionId)
        throws IOException {
        // End doc id is not inclusive but start doc is inclusive
        // Hence we need to check if buffer is readable till endDocId - 1
        ensureBufferReadable(endDocId - 1);

        // Sort all documents using an int array
        int numDocs = endDocId - startDocId;
        int[] sortedDocIds = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            sortedDocIds[i] = startDocId + i;
        }
        new IntroSorter() {
            private long[] dimensions;

            @Override
            protected void swap(int i, int j) {
                int temp = sortedDocIds[i];
                sortedDocIds[i] = sortedDocIds[j];
                sortedDocIds[j] = temp;
            }

            @Override
            protected void setPivot(int i) {
                long offset = starTreeDocumentOffsets.get(sortedDocIds[i]);
                dimensions = new long[starTreeField.getDimensionsOrder().size()];
                try {
                    for (int j = dimensionId + 1; j < dimensions.length; j++) {
                        dimensions[j] = starTreeDocsFileRandomInput.readLong(offset + (long) j * Long.BYTES);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Sort documents failed : " + e); // TODO: handle this better
                }
            }

            @Override
            protected int comparePivot(int j) {
                long offset = starTreeDocumentOffsets.get(sortedDocIds[j]);
                try {
                    for (int i = dimensionId + 1; i < dimensions.length; i++) {
                        long dimension = starTreeDocsFileRandomInput.readLong(offset + (long) i * Long.BYTES);
                        if (dimensions[i] != dimension) {
                            return Long.compare(dimensions[i], dimension);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Sort documents failed : " + e); // TODO: handle this better
                }
                return 0;
            }
        }.sort(0, numDocs);

        // Create an iterator for aggregated documents
        return new Iterator<StarTreeDocument>() {
            boolean _hasNext = true;
            StarTreeDocument _currentdocument = getStarTreeDocument(sortedDocIds[0]);
            int _docId = 1;

            private boolean hasSameDimensions(StarTreeDocument document1, StarTreeDocument document2) {
                for (int i = dimensionId + 1; i < starTreeField.getDimensionsOrder().size(); i++) {
                    if (!Objects.equals(document1.dimensions[i], document2.dimensions[i])) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean hasNext() {
                return _hasNext;
            }

            @Override
            public StarTreeDocument next() {
                StarTreeDocument next = reduceStarTreeDocuments(null, _currentdocument);
                next.dimensions[dimensionId] = STAR_IN_DOC_VALUES_INDEX;
                while (_docId < numDocs) {
                    StarTreeDocument document;
                    try {
                        document = getStarTreeDocument(sortedDocIds[_docId++]);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    if (!hasSameDimensions(document, _currentdocument)) {
                        _currentdocument = document;
                        return next;
                    } else {
                        next = reduceStarTreeDocuments(next, document);
                    }
                }
                _hasNext = false;
                return next;
            }
        };
    }

    private int writeSegmentStarTreeDocument(StarTreeDocument starTreeDocument, IndexOutput output) throws IOException {
        int numBytes = 0;
        for (Long dimension : starTreeDocument.dimensions) {
            if (dimension == null) {
                dimension = Long.MAX_VALUE;
            }
            output.writeLong(dimension);
            numBytes += Long.BYTES;
        }
        for (int i = 0; i < starTreeDocument.metrics.length; i++) {
            switch (metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType()) {
                case LONG:
                case DOUBLE:
                    if (starTreeDocument.metrics[i] != null) {
                        output.writeLong((Long) starTreeDocument.metrics[i]);
                        numBytes += Long.BYTES;
                    }
                    break;
                case INT:
                case FLOAT:
                default:
                    throw new IllegalStateException();
            }
        }
        return numBytes;
    }

    private int writeStarTreeDocument(StarTreeDocument starTreeDocument, IndexOutput output) throws IOException {
        int numBytes = 0;
        for (Long dimension : starTreeDocument.dimensions) {
            if (dimension == null) {
                dimension = Long.MAX_VALUE;
            }
            output.writeLong(dimension);
            numBytes += Long.BYTES;
        }
        for (int i = 0; i < starTreeDocument.metrics.length; i++) {
            switch (metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType()) {
                case LONG:
                    if (starTreeDocument.metrics[i] != null) {
                        output.writeLong((Long) starTreeDocument.metrics[i]);
                        numBytes += Long.BYTES;
                    }
                    break;
                case DOUBLE:
                    if (starTreeDocument.metrics[i] != null) {
                        if (starTreeDocument.metrics[i] instanceof Double) {
                            long val = NumericUtils.doubleToSortableLong((Double) starTreeDocument.metrics[i]);
                            output.writeLong(val);
                            numBytes += Long.BYTES;
                        }
                        // TODO : do i need this
                        else {
                            output.writeLong((Long) starTreeDocument.metrics[i]);
                            numBytes += Long.BYTES;
                        }
                    }
                    break;
                case INT:
                case FLOAT:
                default:
                    throw new IllegalStateException();
            }
        }
        return numBytes;
    }

    private StarTreeDocument readSegmentStarTreeDocument(RandomAccessInput input, long offset, boolean isMerge) throws IOException {
        int dimSize = starTreeField.getDimensionsOrder().size();
        Long[] dimensions = new Long[dimSize];
        for (int i = 0; i < dimSize; i++) {
            try {
                Long val = input.readLong(offset);
                if (val == Long.MAX_VALUE) {
                    val = null;
                }
                dimensions[i] = val;
            } catch (Exception e) {
                logger.info(
                    "Error reading dimension value at offset "
                        + offset
                        + " for dimension"
                        + " "
                        + i
                        + " : _numReadableStarTreedocuments = "
                        + numReadableStarTreeDocuments
                );
                throw e;
            }
            offset += Long.BYTES;
        }
        int numMetrics = 0;
        for (Metric metric : starTreeField.getMetrics()) {
            numMetrics += metric.getMetrics().size();
        }
        Object[] metrics = new Object[numMetrics];
        for (int i = 0; i < numMetrics; i++) {
            switch (metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType()) {
                case LONG:
                    metrics[i] = input.readLong(offset);
                    offset += Long.BYTES;
                    break;
                case DOUBLE:
                    long val = input.readLong(offset);
                    if (isMerge) {
                        metrics[i] = StarTreeNumericTypeConverters.sortableLongtoDouble((long) metrics[i]);
                    } else {
                        metrics[i] = val;
                    }
                    offset += Long.BYTES;
                    break;
                case FLOAT:
                case INT:
                default:
                    throw new IllegalStateException();
            }
        }
        return new StarTreeDocument(dimensions, metrics);
    }

    private StarTreeDocument readStarTreeDocument(RandomAccessInput input, long offset) throws IOException {
        int dimSize = starTreeField.getDimensionsOrder().size();
        Long[] dimensions = new Long[dimSize];
        for (int i = 0; i < dimSize; i++) {
            try {
                Long val = input.readLong(offset);
                if (val == Long.MAX_VALUE) {
                    val = null;
                }
                dimensions[i] = val;
            } catch (Exception e) {
                logger.error(
                    "Error reading dimension value at offset "
                        + offset
                        + " for dimension"
                        + " "
                        + i
                        + " : _numReadableStarTreedocuments = "
                        + numReadableStarTreeDocuments
                );
                throw e;
            }
            offset += Long.BYTES;
        }
        int numMetrics = 0;
        for (Metric metric : starTreeField.getMetrics()) {
            numMetrics += metric.getMetrics().size();
        }
        Object[] metrics = new Object[numMetrics];
        for (int i = 0; i < numMetrics; i++) {
            switch (metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType()) {
                case LONG:
                    metrics[i] = input.readLong(offset);
                    offset += Long.BYTES;
                    break;
                case DOUBLE:
                    // TODO : handle double
                    long val = input.readLong(offset);
                    offset += Long.BYTES;
                    metrics[i] = StarTreeNumericTypeConverters.sortableLongtoDouble(val);
                    break;

                case FLOAT:
                case INT:
                default:
                    throw new IllegalStateException();
            }
        }
        return new StarTreeDocument(dimensions, metrics);
    }

    private void ensureBufferReadable(int docId) throws IOException {
        ensureBufferReadable(docId, true);
    }

    private void ensureBufferReadable(int docId, boolean shouldCreateFileOutput) throws IOException {
        if (docId >= prevStartDocId && docId < numReadableStarTreeDocuments) {
            return;
        }
        IOUtils.closeWhileHandlingException(starTreeDocsFileInput);
        starTreeDocsFileInput = null;
        /**
         * If docId is less then the _numDocs , then we need to find a previous file associated with doc id
         * The fileToByteSizeMap is in the following format
         * file1 -> 521
         * file2 -> 780
         *
         * which represents that file1 contains all docs till "520".
         * "prevStartDocId" essentially tracks the "start doc id" of the range in the present file
         * "_numReadableStarTreedocuments" tracks the "end doc id + 1" of the range in the present file
         *
         * IMPORTANT : This is case where the requested file is not the file which is being currently written to\
         */
        if (docId < numStarTreeDocs) {
            int prevStartDocId = 0;
            for (Map.Entry<String, Integer> entry : fileToByteSizeMap.entrySet()) {
                if (docId < entry.getValue()) {
                    starTreeDocsFileInput = tmpDirectory.openInput(entry.getKey(), state.context);
                    starTreeDocsFileRandomInput = starTreeDocsFileInput.randomAccessSlice(
                        starTreeDocsFileInput.getFilePointer(),
                        starTreeDocsFileInput.length() - starTreeDocsFileInput.getFilePointer()
                    );
                    numReadableStarTreeDocuments = entry.getValue();
                    break;
                }
                prevStartDocId = entry.getValue();
            }
            this.prevStartDocId = prevStartDocId;
        }

        if (starTreeDocsFileInput != null) {
            return;
        }
        starTreeDocsFileOutput.close();
        currBytes = 0;
        fileToByteSizeMap.put(starTreeDocsFileOutput.getName(), numStarTreeDocs);

        if (shouldCreateFileOutput) {
            starTreeDocsFileOutput = createStarTreeDocumentsFileOutput();
        }

        // Check if we need to merge files
        if (fileToByteSizeMap.size() >= FILE_COUNT_THRESHOLD) {
            mergeFiles();
        }

        if (starTreeDocsFileRandomInput != null) {
            starTreeDocsFileRandomInput = null;
        }

        int prevStartDocId = 0;
        for (Map.Entry<String, Integer> entry : fileToByteSizeMap.entrySet()) {
            if (docId <= entry.getValue() - 1) {
                starTreeDocsFileInput = tmpDirectory.openInput(entry.getKey(), state.context);
                starTreeDocsFileRandomInput = starTreeDocsFileInput.randomAccessSlice(
                    starTreeDocsFileInput.getFilePointer(),
                    starTreeDocsFileInput.length() - starTreeDocsFileInput.getFilePointer()
                );
                numReadableStarTreeDocuments = entry.getValue();
                break;
            }
            prevStartDocId = entry.getValue();
        }
        this.prevStartDocId = prevStartDocId;

    }

    private void mergeFiles() throws IOException {
        IndexOutput mergedOutput = createStarTreeDocumentsFileOutput();
        long st = System.currentTimeMillis();

        long mergeBytes = 0L;
        for (Map.Entry<String, Integer> entry : fileToByteSizeMap.entrySet()) {
            IndexInput input = tmpDirectory.openInput(entry.getKey(), state.context);
            mergedOutput.copyBytes(input, input.length());
            mergeBytes += input.length();
            input.close();
        }
        logger.info(
            "Created file MERGE : "
                + starTreeDocsFileOutput.getName()
                + " in : "
                + (System.currentTimeMillis() - st)
                + " ms"
                + " == Size , "
                + (mergeBytes / 1024)
                + " KB"
        );
        mergedOutput.close();
        // Delete the old files
        for (String fileName : fileToByteSizeMap.keySet()) {
            tmpDirectory.deleteFile(fileName);
        }
        // Clear the fileToByteSizeMap and add the merged file
        fileToByteSizeMap.clear();
        fileToByteSizeMap.put(mergedOutput.getName(), numStarTreeDocs);

        int curr = 0;
        for (int i = 0; i < starTreeDocumentOffsets.size(); i++) {
            starTreeDocumentOffsets.set(i, curr);
            curr += docSizeInBytes;
        }

    }

    @Override
    public void close() throws IOException {
        boolean success = false;
        try {
            if (starTreeDocsFileOutput != null) {
                starTreeDocsFileOutput.close();
                IOUtils.deleteFilesIgnoringExceptions(state.directory, starTreeDocsFileOutput.getName());
            }
            success = true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.close(starTreeDocsFileInput, starTreeDocsFileOutput, segmentDocsFileOutput);
        }
        // Delete all temporary segment document files
        IOUtils.deleteFilesIgnoringExceptions(tmpDirectory, List.of(segmentDocsFileOutput.getName()));
        // Delete all temporary star tree document files
        IOUtils.deleteFilesIgnoringExceptions(tmpDirectory, fileToByteSizeMap.keySet());
        super.close();
    }

    // @Override
    // public long ramBytesUsed() {
    // long size = BASE_RAM_BYTES_USED + shallowSizeOf(rootNode);
    // if(numStarTreeNodes > 0) {
    // size += (numStarTreeNodes * shallowSizeOf(rootNode));
    // }
    // size += starTreeDocumentOffsets.size() * Integer.BYTES;
    // return size;
    // }
    //
    // @Override
    // public synchronized Collection<Accountable> getChildResources() {
    // final List<Accountable> resources = new ArrayList<>();
    // resources.add(
    // Accountables.namedAccountable("star tree offsets", shallowSizeOf(starTreeDocumentOffsets)));
    // resources.add(
    // Accountables.namedAccountable("", shallowSizeOf(rootNode))
    // );
    // return Collections.unmodifiableList(resources);
    // }
}
