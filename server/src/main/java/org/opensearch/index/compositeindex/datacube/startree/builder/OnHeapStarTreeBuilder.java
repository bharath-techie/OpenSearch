/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.BaseStarTreeBuilder;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * On heap based single tree builder
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class OnHeapStarTreeBuilder extends BaseStarTreeBuilder {

    private final List<StarTreeDocument> starTreeDocuments = new ArrayList<>();

    /**
     * Constructor for OnHeapStarTreeBuilder
     *
     * @param starTreeField     star-tree field
     * @param segmentWriteState segment write state
     * @param mapperService     helps with the numeric type of field
     * @throws IOException throws an exception when we are unable to construct a star-tree using on-heap approach
     */
    public OnHeapStarTreeBuilder(
        IndexOutput metaOut,
        IndexOutput dataOut,
        StarTreeField starTreeField,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) throws IOException {
        super(metaOut, dataOut, starTreeField, segmentWriteState, mapperService);
    }

    @Override
    public void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException {
        starTreeDocuments.add(starTreeDocument);
    }

    @Override
    public void build(
        List<StarTreeValues> starTreeValuesSubs,
        AtomicInteger fieldNumberAcrossStarTrees,
        DocValuesConsumer starTreeDocValuesConsumer
    ) throws IOException {
        build(mergeStarTrees(starTreeValuesSubs), fieldNumberAcrossStarTrees, starTreeDocValuesConsumer);
    }

    /**
     * Sorts and aggregates the star-tree documents from multiple segments and builds star tree based on the newly
     * aggregated star-tree documents
     *
     * @param starTreeValuesSubs StarTreeValues from multiple segments
     * @return iterator of star tree documents
     */
    Iterator<StarTreeDocument> mergeStarTrees(List<StarTreeValues> starTreeValuesSubs) throws IOException {
        return sortAndAggregateStarTreeDocuments(mergeStarTreeValues(starTreeValuesSubs));
    }

    /**
     * Returns an array of all the starTreeDocuments from all the segments
     *
     * @param starTreeValuesSubs StarTreeValues from multiple segments
     * @return array of star tree documents
     */
    StarTreeDocument[] mergeStarTreeValues(List<StarTreeValues> starTreeValuesSubs) throws IOException {
        List<StarTreeDocument> starTreeDocuments = new ArrayList<>();
        for (StarTreeValues starTreeValues : starTreeValuesSubs) {
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

            boolean endOfDoc = false;
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
                    metrics[i] = metricDocValuesIterator.value(currentDocId);
                    i++;
                }
                StarTreeDocument starTreeDocument = new StarTreeDocument(dims, metrics);
                starTreeDocuments.add(starTreeDocument);
                currentDocId++;
            }
        }
        StarTreeDocument[] starTreeDocumentsArr = new StarTreeDocument[starTreeDocuments.size()];
        return starTreeDocuments.toArray(starTreeDocumentsArr);
    }

    @Override
    public StarTreeDocument getStarTreeDocument(int docId) throws IOException {
        return starTreeDocuments.get(docId);
    }

    @Override
    public List<StarTreeDocument> getStarTreeDocuments() {
        return starTreeDocuments;
    }

    @Override
    public Long getDimensionValue(int docId, int dimensionId) throws IOException {
        return starTreeDocuments.get(docId).dimensions[dimensionId];
    }

    /**
     * Sorts and aggregates all the documents of the segment based on dimension and metrics configuration
     *
     * @param numDocs number of documents in the given segment
     * @param dimensionReaders List of docValues readers to read dimensions from the segment
     * @param metricReaders List of docValues readers to read metrics from the segment
     * @return Iterator of star-tree documents
     *
     */
    @Override
    public Iterator<StarTreeDocument> sortAndAggregateSegmentDocuments(
        int numDocs,
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders
    ) throws IOException {
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[numDocs];
        for (int currentDocId = 0; currentDocId < numDocs; currentDocId++) {
            starTreeDocuments[currentDocId] = getSegmentStarTreeDocument(currentDocId, dimensionReaders, metricReaders);
        }
        return sortAndAggregateStarTreeDocuments(starTreeDocuments);
    }

    /**
     * Sorts and aggregates the star-tree documents
     *
     * @param starTreeDocuments star-tree documents
     * @return iterator for star-tree documents
     * @throws IOException throws when unable to sort, merge and aggregate star-tree documents
     */
    public Iterator<StarTreeDocument> sortAndAggregateStarTreeDocuments(StarTreeDocument[] starTreeDocuments) throws IOException {
        // sort the documents
        Arrays.sort(starTreeDocuments, (o1, o2) -> {
            for (int i = 0; i < numDimensions; i++) {
                if (o1.dimensions[i] == null && o2.dimensions[i] == null) {
                    return 0;
                }
                if (o1.dimensions[i] == null) {
                    return 1;
                }
                if (o2.dimensions[i] == null) {
                    return -1;
                }
                if (!Objects.equals(o1.dimensions[i], o2.dimensions[i])) {
                    return Long.compare(o1.dimensions[i], o2.dimensions[i]);
                }
            }
            return 0;
        });

        // merge the documents
        return mergeStarTreeDocuments(starTreeDocuments);
    }

    /**
     * Merges the star-tree documents based on dimensions
     *
     * @param starTreeDocuments star-tree documents
     * @return iterator to aggregate star-tree documents
     */
    private Iterator<StarTreeDocument> mergeStarTreeDocuments(StarTreeDocument[] starTreeDocuments) {
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
                // aggregate as we move on to the next doc
                StarTreeDocument next = reduceSegmentStarTreeDocuments(null, currentStarTreeDocument);
                while (docId < starTreeDocuments.length) {
                    StarTreeDocument starTreeDocument = starTreeDocuments[docId++];
                    if (!Arrays.equals(starTreeDocument.dimensions, next.dimensions)) {
                        currentStarTreeDocument = starTreeDocument;
                        return next;
                    } else {
                        next = reduceSegmentStarTreeDocuments(next, starTreeDocument);
                    }
                }
                hasNext = false;
                return next;
            }
        };
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
        int numDocs = endDocId - startDocId;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[numDocs];
        for (int i = 0; i < numDocs; i++) {
            starTreeDocuments[i] = getStarTreeDocument(startDocId + i);
        }
        Arrays.sort(starTreeDocuments, (o1, o2) -> {
            for (int i = dimensionId + 1; i < numDimensions; i++) {
                if (!Objects.equals(o1.dimensions[i], o2.dimensions[i])) {
                    return Long.compare(o1.dimensions[i], o2.dimensions[i]);
                }
            }
            return 0;
        });
        return new Iterator<StarTreeDocument>() {
            boolean hasNext = true;
            StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
            int docId = 1;

            private boolean hasSameDimensions(StarTreeDocument starTreeDocument1, StarTreeDocument starTreeDocument2) {
                for (int i = dimensionId + 1; i < numDimensions; i++) {
                    if (!Objects.equals(starTreeDocument1.dimensions[i], starTreeDocument2.dimensions[i])) {
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
                    StarTreeDocument starTreeDocument = starTreeDocuments[docId++];
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
}
