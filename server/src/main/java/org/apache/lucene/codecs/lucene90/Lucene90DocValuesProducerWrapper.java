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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;

/**
 * This class is a custom abstraction of the {@link DocValuesProducer} for the Star Tree index structure.
 * It is responsible for providing access to various types of document values (numeric, binary, sorted, sorted numeric,
 * and sorted set) for fields in the Star Tree index.
 *
 * @opensearch.experimental
 */
public class Lucene90DocValuesProducerWrapper extends DocValuesProducer {

    Lucene90DocValuesProducer lucene90DocValuesProducer;
    SegmentReadState state;

    public Lucene90DocValuesProducerWrapper(
        SegmentReadState state,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension
    ) throws IOException {
        lucene90DocValuesProducer = new Lucene90DocValuesProducer(state, dataCodec, dataExtension, metaCodec, metaExtension);
        this.state = state;
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
        return this.lucene90DocValuesProducer.getSortedNumeric(state.fieldInfos.fieldInfo(fieldName));
    }

    @Override
    public void close() throws IOException {
        this.lucene90DocValuesProducer.close();
    }

}
