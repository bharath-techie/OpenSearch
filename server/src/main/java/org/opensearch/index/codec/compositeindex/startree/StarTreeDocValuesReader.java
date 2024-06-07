/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.compositeindex.startree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.opensearch.index.compositeindex.startree.StarTreeReader;
import org.opensearch.index.compositeindex.startree.StarTreeValues;


public class StarTreeDocValuesReader extends DocValuesProducer
    implements StarTreeReader {
    private DocValuesProducer delegate;

    private IndexInput data;
    private static final Logger logger = LogManager.getLogger(StarTreeDocValuesReader.class);


    public StarTreeDocValuesReader(DocValuesProducer producer, SegmentReadState state) throws IOException {
        this.delegate = producer;
        try {
            String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "sti");

            this.data = state.directory.openInput(dataName, state.context);
            CodecUtil.checkIndexHeader(data, StarTreeCodec.STAR_TREE_CODEC_NAME, 0, 0, state.segmentInfo.getId(), state.segmentSuffix);
        } catch (Exception ex){
            logger.error("====== Error reading star tree data / No star tree =========", ex);
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
    public void checkIntegrity() throws IOException {}

    @Override
    public void close() throws IOException {}

    @Override
    public List<String> getStarTreeFields() {
        // TODO
        return new ArrayList<>();
    }

    @Override
    public StarTreeValues getStarTreeValues(String field)
        throws IOException {
        // TODO
        return null;
    }
}
