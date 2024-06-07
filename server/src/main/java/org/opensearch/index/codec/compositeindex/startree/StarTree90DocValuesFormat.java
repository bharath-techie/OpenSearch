/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.compositeindex.startree;

import java.io.IOException;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import static org.opensearch.index.codec.compositeindex.startree.StarTree99Codec.STAR_TREE_CODEC_NAME;


public class StarTree90DocValuesFormat extends DocValuesFormat {
    /**
     * Creates a new docvalues format.
     *
     * <p>The provided name will be written into the index segment in some configurations (such as
     * when using {@code PerFieldDocValuesFormat}): in such configurations, for the segment to be read
     * this class should be registered with Java's SPI mechanism (registered in META-INF/ of your jar
     * file, etc).
     */
    private final DocValuesFormat delegate;

    public StarTree90DocValuesFormat() {
        this(new Lucene90DocValuesFormat());
    }

    public StarTree90DocValuesFormat(DocValuesFormat delegate) {
        super(STAR_TREE_CODEC_NAME);
        this.delegate = delegate;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new StarTree90DocValuesWriter(delegate.fieldsConsumer(state), state);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new StarTree90DocValuesReader(delegate.fieldsProducer(state), state);
    }
}
