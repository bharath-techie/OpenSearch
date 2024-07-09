/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricEntry;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeHelper.fullFieldNameForStarTreeDimensionsDocValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeHelper.fullFieldNameForStarTreeMetricsDocValues;

/**
 * This class is a custom abstraction of the {@link DocValuesProducer} for the Star Tree index structure.
 * It is responsible for providing access to various types of document values (numeric, binary, sorted, sorted numeric,
 * and sorted set) for fields in the Star Tree index.
 *
 * @opensearch.experimental
 */
public class StarTree99DocValuesProducer extends DocValuesProducer {

    Lucene90DocValuesProducer lucene90DocValuesProducer;
    private final List<FieldInfo> dimensions;
    private final List<MetricEntry> metrics;
    private final FieldInfos fieldInfos;

    public StarTree99DocValuesProducer(
        SegmentReadState state,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension,
        List<FieldInfo> dimensions,
        List<MetricEntry> metricEntries,
        String compositeFieldName
    ) throws IOException {
        this.dimensions = dimensions;
        this.metrics = metricEntries;

        // populates the dummy list of field infos to fetch doc id set iterators for respective fields.
        this.fieldInfos = new FieldInfos(getFieldInfoList(compositeFieldName));
        SegmentReadState segmentReadState = new SegmentReadState(state.directory, state.segmentInfo, fieldInfos, state.context);
        lucene90DocValuesProducer = new Lucene90DocValuesProducer(segmentReadState, dataCodec, dataExtension, metaCodec, metaExtension);
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return this.lucene90DocValuesProducer.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return this.lucene90DocValuesProducer.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return this.lucene90DocValuesProducer.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return this.lucene90DocValuesProducer.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return this.lucene90DocValuesProducer.getSortedSet(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        this.lucene90DocValuesProducer.checkIntegrity();
    }

    // returns the doc id set iterator based on field name
    public SortedNumericDocValues getSortedNumeric(String fieldName) throws IOException {
        return this.lucene90DocValuesProducer.getSortedNumeric(fieldInfos.fieldInfo(fieldName));
    }

    @Override
    public void close() throws IOException {
        this.lucene90DocValuesProducer.close();
    }

    private FieldInfo[] getFieldInfoList(String compositeFieldName) {
        FieldInfo[] fieldInfoList = new FieldInfo[this.dimensions.size() + metrics.size()];
        // field number is not really used. We depend on unique field names to get the desired iterator
        int fieldNumber = 0;

        for (FieldInfo dimension : this.dimensions) {
            fieldInfoList[fieldNumber] = new FieldInfo(
                fullFieldNameForStarTreeDimensionsDocValues(compositeFieldName, dimension.getName()),
                fieldNumber,
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
            fieldNumber++;
        }
        for (MetricEntry metric : metrics) {
            fieldInfoList[fieldNumber] = new FieldInfo(
                fullFieldNameForStarTreeMetricsDocValues(compositeFieldName, metric.getMetricName(), metric.getMetricStat().getTypeName()),
                fieldNumber,
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
            fieldNumber++;
        }
        return fieldInfoList;
    }

}
