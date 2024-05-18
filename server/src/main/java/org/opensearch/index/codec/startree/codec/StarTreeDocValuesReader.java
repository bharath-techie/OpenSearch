/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.startree.codec;

import java.util.LinkedHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.codec.StarTreeReader;
import org.opensearch.index.codec.startree.node.OffHeapStarTree;
import org.opensearch.index.codec.startree.node.StarTree;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/** Custom star tree doc values reader */
public class StarTreeDocValuesReader extends DocValuesProducer
    implements StarTreeReader {
    private DocValuesProducer delegate;

    private IndexInput data;

    private Lucene90DocValuesProducerCopy valuesProducer;

    StarTree starTree;

    Map<String, SortedNumericDocValues> dimensionValues;
    Map<String, SortedSetDocValues> keywordDimValues;

    Map<String, SortedNumericDocValues> metricValues;
    public static final String DATA_CODEC = "Lucene90DocValuesData";
    public static final String META_CODEC = "Lucene90DocValuesMetadata";
    private static final Logger logger = LogManager.getLogger(StarTreeDocValuesReader.class);


    public StarTreeDocValuesReader(DocValuesProducer producer, SegmentReadState state) throws IOException {
        this.delegate = producer;
        try {
            String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "stttree");
            this.data = state.directory.openInput(dataName, state.context);
            CodecUtil.checkIndexHeader(data, "STARTreeCodec", 0, 0, state.segmentInfo.getId(), state.segmentSuffix);
            starTree = new OffHeapStarTree(data);
        } catch (Exception ex){
            logger.error("====== Error reading star tree data / NO star tree =========", ex);
        }
        valuesProducer = new Lucene90DocValuesProducerCopy(state, DATA_CODEC, "sttd", META_CODEC, "sttm", starTree.getDimensionNames());
        dimensionValues = new LinkedHashMap<>();
        keywordDimValues = new LinkedHashMap<>();
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return delegate.getNumeric(field);
    }
    public StarTreeAggregatedValues getAggregatedDocValues() throws IOException {
        List<String> dimensionsSplitOrder = starTree.getDimensionNames();
        for (int i = 0; i < dimensionsSplitOrder.size(); i++) {
            try {
                dimensionValues.put(dimensionsSplitOrder.get(i), valuesProducer.getSortedNumeric(dimensionsSplitOrder.get(i) + "_dim"));
            } catch (NullPointerException e) {
                keywordDimValues.put(dimensionsSplitOrder.get(i), valuesProducer.getSortedSet(dimensionsSplitOrder.get(i) + "_dim"));
            }
        }
        metricValues = new HashMap<>();
        metricValues.put("status_sum", valuesProducer.getSortedNumeric("status_sum_metric"));
        //metricValues.put("status_count", valuesProducer.getNumeric("status_count_metric"));
        return new StarTreeAggregatedValues(starTree, dimensionValues, keywordDimValues, metricValues);
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
    public void checkIntegrity() throws IOException {}

    @Override
    public void close() throws IOException {}

    @Override
    public StarTreeAggregatedValues getStarTreeValues()
        throws IOException {
        return getAggregatedDocValues();
    }
}
