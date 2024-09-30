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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeDocumentBitSetUtil;
import org.opensearch.index.mapper.FieldValueConverter;

import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.DOUBLE;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.LONG;


/**
 * Class for managing segment documents file.
 * Segment documents are stored in a single file named 'segment.documents' for sorting and aggregation. A document ID array is created,
 * and the document IDs in the array are swapped during sorting based on the actual segment document values in the file.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class OnHeapSegmentDocsFileManager extends AbstractDocumentsFileManager implements Closeable {

    private static final Logger logger = LogManager.getLogger(OnHeapSegmentDocsFileManager.class);
    List<StarTreeDocument> starTreeDocArr;

    public OnHeapSegmentDocsFileManager(
        SegmentWriteState state,
        StarTreeField starTreeField,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        int numDimensions
    ) throws IOException {
        super(state, starTreeField, metricAggregatorInfos, numDimensions);
        starTreeDocArr = new ArrayList<>();
    }

    @Override
    public void writeStarTreeDocument(StarTreeDocument starTreeDocument, boolean isAggregatedDoc) throws IOException {
        starTreeDocArr.add(starTreeDocument);
    }

    @Override
    public StarTreeDocument readStarTreeDocument(int docId, boolean isAggregatedDoc) throws IOException {
        Object[] metrics = new Object[numMetrics];
        if (isAggregatedDoc == false) {
           readFlushMetrics(metrics, docId);
        } else {
            readMetrics(numMetrics, metrics, isAggregatedDoc, docId);
        }
        return new StarTreeDocument(starTreeDocArr.get(docId).dimensions, metrics);
    }

    protected void readFlushMetrics(Object[] metrics, int docId) throws IOException {
        int numMetrics = 0;
        int fieldIndex = 0;
        for (Metric metric : starTreeField.getMetrics()) {
            for (MetricStat stat : metric.getBaseMetrics()) {
                metrics[numMetrics++] = starTreeDocArr.get(docId).metrics[fieldIndex];
            }
            fieldIndex++;
        }
    }

    /**
     * Read star tree metrics from file
     */
    protected void readMetrics(int numMetrics, Object[] metrics, boolean isAggregatedDoc, int docId)
        throws IOException {
        metrics = starTreeDocArr.get(docId).metrics;
    }

    @Override
    public Long[] readDimensions(int docId) throws IOException {
        return starTreeDocArr.get(docId).dimensions;
    }

    @Override
    public Long getDimensionValue(int docId, int dimensionId) throws IOException {
        Long[] dims = readDimensions(docId);
        return dims[dimensionId];
    }

    @Override
    public void close() throws IOException {
        starTreeDocArr = null;
    }
}
