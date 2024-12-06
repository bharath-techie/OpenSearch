/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LongValues;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.ValueAggregator;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Arrow-based single tree builder
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MmapArrowStarTreeBuilder extends BaseStarTreeBuilder {

    private final File mmapFile;
    private final BufferAllocator allocator;
    private VectorSchemaRoot vectorSchemaRoot;
    private ArrowFileWriter writer;
    private FileChannel channel;
    private int currentIndex = 0;
    private long totalDocuments = 0;
    private long flushedDocuments = 0;
    private List<ArrowBlock> blocks = new ArrayList<>();
    private int batchSize;

    public MmapArrowStarTreeBuilder(
        IndexOutput metaOut,
        IndexOutput dataOut,
        StarTreeField starTreeField,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) throws IOException {
        super(metaOut, dataOut, starTreeField, segmentWriteState, mapperService);

        this.allocator = new RootAllocator();
        this.mmapFile = File.createTempFile("star-tree-", ".arrow");
        this.mmapFile.deleteOnExit();

        List<Field> fields = new ArrayList<>();

        // Add dimension fields
        for (int i = 0; i < numDimensions; i++) {
            fields.add(Field.nullable("dimension_" + i, new ArrowType.Int(64, true)));
        }

        // Add metric fields
        for (int i = 0; i < numMetrics; i++) {
            ValueAggregator<?> aggregator = metricAggregatorInfos.get(i).getValueAggregators();
            ArrowType arrowType = (aggregator.getAggregatedValueType() == NumberFieldMapper.NumberType.LONG)
                ? new ArrowType.Int(64, true)
                : new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            fields.add(Field.nullable("metric_" + i, arrowType));
        }

        Schema schema = new Schema(fields);
        this.batchSize = 1024; // You can adjust this value
        this.vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        this.vectorSchemaRoot.allocateNew();

        this.channel = FileChannel.open(mmapFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.writer = new ArrowFileWriter(vectorSchemaRoot, null, channel);
        this.writer.start();
    }

    @Override
    public void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException {
        ensureCapacity();

        // Set dimension values
        for (int i = 0; i < numDimensions; i++) {
            FieldVector vector = vectorSchemaRoot.getVector("dimension_" + i);
            Long value = starTreeDocument.dimensions[i];
            if (value == null) {
                vector.setNull(currentIndex);
            } else {
                ((BigIntVector) vector).set(currentIndex, value);
            }
        }

        // Set metric values
        for (int i = 0; i < numMetrics; i++) {
            FieldVector vector = vectorSchemaRoot.getVector("metric_" + i);
            Object value = starTreeDocument.metrics[i];
            if (value == null) {
                vector.setNull(currentIndex);
            } else if (vector instanceof BigIntVector) {
                ((BigIntVector) vector).set(currentIndex, ((Number) value).longValue());
            } else if (vector instanceof Float8Vector) {
                ((Float8Vector) vector).set(currentIndex, ((Number) value).doubleValue());
            }
        }

        currentIndex++;
        totalDocuments++;

        if (currentIndex >= batchSize) {
            flushToFile();
        }
    }

    private void ensureCapacity() throws IOException {
        if (currentIndex >= batchSize) {
            flushToFile();
            vectorSchemaRoot.allocateNew();
            currentIndex = 0;
        }
    }

    private void flushToFile() throws IOException {
        vectorSchemaRoot.setRowCount(currentIndex);
        writer.writeBatch();

        // Record the block information
        long offset = channel.position() - writer.bytesWritten();
        int metadataLength = 0; // You may need to adjust this based on your Arrow file structure
        long bodyLength = writer.bytesWritten();
        blocks.add(new ArrowBlock(offset, metadataLength, bodyLength));

        flushedDocuments += currentIndex;
    }

    private void ensureDocumentFlushed(int docId) throws IOException {
        if (docId >= flushedDocuments) {
            flushToFile();
        }
    }

    @Override
    public StarTreeDocument getStarTreeDocument(int docId) throws IOException {
        if (docId >= totalDocuments) {
            throw new IllegalArgumentException("Document ID out of range");
        }

        ensureDocumentFlushed(docId);

        int batchIndex = docId / batchSize;
        int batchOffset = docId % batchSize;

        ArrowBlock block = blocks.get(batchIndex);

        Long[] dimensions = new Long[numDimensions];
        Object[] metrics = new Object[numMetrics];

        // Read dimension values
        for (int i = 0; i < numDimensions; i++) {
            dimensions[i] = readLongValue(block, "dimension_" + i, batchOffset);
        }

        // Read metric values
        for (int i = 0; i < numMetrics; i++) {
            ValueAggregator<?> aggregator = metricAggregatorInfos.get(i).getValueAggregators();
            if (aggregator.getAggregatedValueType() == NumberFieldMapper.NumberType.LONG) {
                metrics[i] = readLongValue(block, "metric_" + i, batchOffset);
            } else {
                metrics[i] = readDoubleValue(block, "metric_" + i, batchOffset);
            }
        }

        return new StarTreeDocument(dimensions, metrics);
    }

    @Override
    public Long getDimensionValue(int docId, int dimensionId) throws IOException {
        if (docId >= totalDocuments) {
            throw new IllegalArgumentException("Document ID out of range");
        }

        ensureDocumentFlushed(docId);

        int batchIndex = docId / batchSize;
        int batchOffset = docId % batchSize;

        ArrowBlock block = blocks.get(batchIndex);
        return readLongValue(block, "dimension_" + dimensionId, batchOffset);
    }

    private Long readLongValue(ArrowBlock block, String fieldName, int offset) throws IOException {
        channel.position(block.getOffset());
        ByteBuffer buffer = ByteBuffer.allocate(8);
        channel.read(buffer);
        buffer.flip();
        long value = buffer.getLong();
        return (value == Long.MIN_VALUE) ? null : value;
    }

    private Double readDoubleValue(ArrowBlock block, String fieldName, int offset) throws IOException {
        channel.position(block.getOffset());
        ByteBuffer buffer = ByteBuffer.allocate(8);
        channel.read(buffer);
        buffer.flip();
        double value = buffer.getDouble();
        return Double.isNaN(value) ? null : value;
    }


    @Override
    public List<StarTreeDocument> getStarTreeDocuments() throws IOException {
        List<StarTreeDocument> documents = new ArrayList<>(currentIndex);
        for (int i = 0; i < currentIndex; i++) {
            documents.add(getStarTreeDocument(i));
        }
        return documents;
    }

    @Override
    public Iterator<StarTreeDocument> sortAndAggregateSegmentDocuments(
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders
    ) throws IOException {
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[totalSegmentDocs];
        for (int currentDocId = 0; currentDocId < totalSegmentDocs; currentDocId++) {
            starTreeDocuments[currentDocId] = getSegmentStarTreeDocument(currentDocId, dimensionReaders, metricReaders);
        }
        return sortAndAggregateStarTreeDocuments(starTreeDocuments, false);
    }

    @Override
    public void build(
        List<StarTreeValues> starTreeValuesSubs,
        AtomicInteger fieldNumberAcrossStarTrees,
        DocValuesConsumer starTreeDocValuesConsumer
    ) throws IOException {
        build(mergeStarTrees(starTreeValuesSubs), fieldNumberAcrossStarTrees, starTreeDocValuesConsumer);
    }

    @Override
    Iterator<StarTreeDocument> mergeStarTrees(List<StarTreeValues> starTreeValuesSubs) throws IOException {
        this.isMerge = true;
        return sortAndAggregateStarTreeDocuments(getSegmentsStarTreeDocuments(starTreeValuesSubs), true);
    }

    StarTreeDocument[] getSegmentsStarTreeDocuments(List<StarTreeValues> starTreeValuesSubs) throws IOException {
        List<StarTreeDocument> starTreeDocuments = new ArrayList<>();
        Map<String, OrdinalMap> ordinalMaps = getOrdinalMaps(starTreeValuesSubs);
        int seg = 0;
        for (StarTreeValues starTreeValues : starTreeValuesSubs) {
            SequentialDocValuesIterator[] dimensionReaders = new SequentialDocValuesIterator[numDimensions];
            List<SequentialDocValuesIterator> metricReaders = new ArrayList<>();
            AtomicInteger numSegmentDocs = new AtomicInteger();
            setReadersAndNumSegmentDocsDuringMerge(dimensionReaders, metricReaders, numSegmentDocs, starTreeValues);
            int currentDocId = 0;
            Map<String, LongValues> longValuesMap = new LinkedHashMap<>();
            for (Map.Entry<String, OrdinalMap> entry : ordinalMaps.entrySet()) {
                longValuesMap.put(entry.getKey(), entry.getValue().getGlobalOrds(seg));
            }
            while (currentDocId < numSegmentDocs.get()) {
                starTreeDocuments.add(getStarTreeDocument(currentDocId, dimensionReaders, metricReaders, longValuesMap));
                currentDocId++;
            }
            seg++;
        }
        return starTreeDocuments.toArray(new StarTreeDocument[0]);
    }

    @Override
    public Iterator<StarTreeDocument> generateStarTreeDocumentsForStarNode(int startDocId, int endDocId, int dimensionId) throws IOException {
        int numDocs = endDocId - startDocId;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[numDocs];
        for (int i = 0; i < numDocs; i++) {
            starTreeDocuments[i] = getStarTreeDocument(startDocId + i);
        }

        sortStarTreeDocumentsFromDimensionId(starTreeDocuments, dimensionId + 1);

        return new Iterator<StarTreeDocument>() {
            boolean hasNext = true;
            StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
            int docId = 1;

            private boolean hasSameDimensions(StarTreeDocument doc1, StarTreeDocument doc2) {
                for (int i = dimensionId + 1; i < numDimensions; i++) {
                    if (!Objects.equals(doc1.dimensions[i], doc2.dimensions[i])) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public StarTreeDocument next() {
                StarTreeDocument next = reduceStarTreeDocuments(null, currentStarTreeDocument);
                next.dimensions[dimensionId] = STAR_IN_DOC_VALUES_INDEX;
                while (docId < numDocs) {
                    StarTreeDocument starTreeDocument = starTreeDocuments[docId];
                    docId++;
                    if (!hasSameDimensions(starTreeDocument, currentStarTreeDocument)) {
                        currentStarTreeDocument = starTreeDocument;
                        return next;
                    } else {
                        next = reduceStarTreeDocuments(next, starTreeDocument);
                    }
                }
                hasNext = false;
                return next;
            }
        };
    }

    private Iterator<StarTreeDocument> sortAndAggregateStarTreeDocuments(StarTreeDocument[] starTreeDocuments, boolean isMerge) {
        sortStarTreeDocumentsFromDimensionId(starTreeDocuments, 0);
        return mergeStarTreeDocuments(starTreeDocuments, isMerge);
    }

    private Iterator<StarTreeDocument> mergeStarTreeDocuments(StarTreeDocument[] starTreeDocuments, boolean isMerge) {
        return new Iterator<>() {
            boolean hasNext = true;
            StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
            int docId = 1;

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public StarTreeDocument next() {
                StarTreeDocument next = reduceSegmentStarTreeDocuments(null, currentStarTreeDocument, isMerge);
                while (docId < starTreeDocuments.length) {
                    StarTreeDocument starTreeDocument = starTreeDocuments[docId];
                    docId++;
                    if (!Arrays.equals(starTreeDocument.dimensions, next.dimensions)) {
                        currentStarTreeDocument = starTreeDocument;
                        return next;
                    } else {
                        next = reduceSegmentStarTreeDocuments(next, starTreeDocument, isMerge);
                    }
                }
                hasNext = false;
                return next;
            }
        };
    }

    private void sortStarTreeDocumentsFromDimensionId(StarTreeDocument[] starTreeDocuments, int dimensionId) {
        Arrays.sort(starTreeDocuments, (o1, o2) -> {
            for (int i = dimensionId; i < numDimensions; i++) {
                if (!Objects.equals(o1.dimensions[i], o2.dimensions[i])) {
                    if (o1.dimensions[i] == null && o2.dimensions[i] == null) {
                        return 0;
                    }
                    if (o1.dimensions[i] == null) {
                        return 1;
                    }
                    if (o2.dimensions[i] == null) {
                        return -1;
                    }
                    return Long.compare(o1.dimensions[i], o2.dimensions[i]);
                }
            }
            return 0;
        });
    }

    @Override
    public void close() throws IOException {
        flushToFile();
        writer.end();
        writer.close();
        channel.close();
        vectorSchemaRoot.close();
        allocator.close();
        mmapFile.delete();
        super.close();
    }
}
