/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.compositeindex.startree;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.opensearch.index.codec.PerFieldMappingPostingFormatCodec;
import org.opensearch.index.compositeindex.CompositeIndexConfig;
import org.opensearch.index.mapper.MapperService;


/**
 * Codec to create startree indices to accelerate aggregations during indexing
 * Doc values format is extended to accommodate star tree index creation
 * */
public class StarTree99Codec extends FilterCodec {
    public static final String LUCENE_99 = "Lucene99";
    public static final String STAR_TREE_CODEC_NAME = "StarTreeCodec99";

    //private final
    public StarTree99Codec() {
        this(STAR_TREE_CODEC_NAME, new Lucene99Codec());
    }

    public StarTree99Codec(CompositeIndexConfig compositeIndexConfig) {
        this(STAR_TREE_CODEC_NAME, new Lucene99Codec());
    }

    public StarTree99Codec(Lucene99Codec.Mode compressionMode, MapperService mapperService, Logger logger) {
        this(STAR_TREE_CODEC_NAME, new PerFieldMappingPostingFormatCodec(compressionMode, mapperService, logger));
    }

    /**
     * Sole constructor. When subclassing this codec, create a no-arg ctor and pass the delegate codec and a unique name to
     * this ctor.
     *
     * @param name
     * @param delegate
     */
    protected StarTree99Codec(String name, Codec delegate) {
        super(name, delegate);
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return new StarTree90DocValuesFormat();
    }
}
