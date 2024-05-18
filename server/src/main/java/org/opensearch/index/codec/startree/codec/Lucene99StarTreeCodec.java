/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.startree.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;


public class Lucene99StarTreeCodec extends FilterCodec {
    public static final String LUCENE_99 = "Lucene99";
    private Codec lucene99Codec;
    public Codec getDelegate() {
        if (lucene99Codec == null) {
            lucene99Codec = Codec.forName(LUCENE_99);
        }
        return lucene99Codec;
    }
    /**
     * Sole constructor. When subclassing this codec, create a no-arg ctor and pass the delegate codec and a unique name to
     * this ctor.
     *
     */
    public Lucene99StarTreeCodec() {
        super("startree", new Lucene99Codec());
    }

    /**
     * TODO : change this - this is a hack
     */
    @Override
    public DocValuesFormat docValuesFormat() {
        return new StarTreeDocValuesFormat();
    }
}
