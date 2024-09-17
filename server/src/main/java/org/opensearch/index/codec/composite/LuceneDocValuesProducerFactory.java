/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.opensearch.index.codec.composite.composite99.Composite99Codec;
import org.opensearch.index.compositeindex.datacube.startree.values.StarTree99ValuesProducer;
import org.opensearch.index.compositeindex.datacube.startree.values.StarTreeValuesProducer;

import java.io.IOException;

/**
 * A factory class that provides a factory method for creating {@link DocValuesProducer} instances
 * based on the specified composite codec.
 * <p>
 * In producers, we want to ensure compatibility with older codec versions during the segment reads.
 * This approach allows for writing with only the latest codec while maintaining
 * the ability to read data encoded with any codec version present in the segment.
 * <p>
 * This design ensures backward compatibility for reads across different codec versions.
 *
 * @opensearch.experimental
 */
public class LuceneDocValuesProducerFactory {

    public static StarTreeValuesProducer getDocValuesProducerForCompositeCodec(
        String compositeCodec,
        SegmentReadState state,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension
    ) throws IOException {

        switch (compositeCodec) {
            case Composite99Codec.COMPOSITE_INDEX_CODEC_NAME:
                return new StarTree99ValuesProducer(state, dataCodec, dataExtension, metaCodec, metaExtension);
            default:
                throw new IllegalStateException("Invalid composite codec " + "[" + compositeCodec + "]");
        }

    }

}
