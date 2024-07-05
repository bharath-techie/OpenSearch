/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;

/**
 * DocValues format to handle composite indices
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Composite90DocValuesFormat extends DocValuesFormat {
    /**
     * Creates a new docvalues format.
     *
     * <p>The provided name will be written into the index segment in some configurations (such as
     * when using {@code PerFieldDocValuesFormat}): in such configurations, for the segment to be read
     * this class should be registered with Java's SPI mechanism (registered in META-INF/ of your jar
     * file, etc).
     */
    private final DocValuesFormat delegate;
    private final MapperService mapperService;

    // needed for SPI
    public Composite90DocValuesFormat() {
        this(new Lucene90DocValuesFormat(), null);
    }

    public Composite90DocValuesFormat(MapperService mapperService) {
        this(new Lucene90DocValuesFormat(), mapperService);
    }

    public Composite90DocValuesFormat(DocValuesFormat delegate, MapperService mapperService) {
        super(delegate.getName());
        this.delegate = delegate;
        this.mapperService = mapperService;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new Composite90DocValuesWriter(delegate.fieldsConsumer(state), state, mapperService);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new Composite90DocValuesReader(delegate.fieldsProducer(state), state, mapperService);
    }
}
