/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.compositeindex.startree;

import org.apache.lucene.backward_codecs.lucene70.Lucene70Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.opensearch.index.codec.PerFieldMappingPostingFormatCodec;


/**
 * Codec to create startree indices to accelerate aggregations during indexing
 * Doc values format is extended to accommodate star tree index creation
 * */
public class StarTreeCodec extends FilterCodec {
    public static final String LUCENE_99 = "Lucene99";
    public static final String STAR_TREE_CODEC_NAME = "StarTreeCodec";

    public StarTreeCodec() {
        this(STAR_TREE_CODEC_NAME, new Lucene99Codec());
    }

    /**
     * Sole constructor. When subclassing this codec, create a no-arg ctor and pass the delegate codec and a unique name to
     * this ctor.
     *
     * @param name
     * @param delegate
     */
    protected StarTreeCodec(String name, Codec delegate) {
        super(name, delegate);
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return new StarTreeDocValuesFormat();
    }
}
