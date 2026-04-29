/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

/**
 * Codec registered as "ParquetCodec" so we can open Lucene indexes
 * written by the POC tooling. Delegates everything to Lucene101Codec;
 * the only addition is a {@code parquet_file} attribute on segment info
 * at write time.
 *
 * <p>Test-only — lives in test sources and is discovered via Lucene's
 * SPI (META-INF/services) at test runtime.
 */
public class ParquetCodec extends FilterCodec {

    private final String parquetPath;

    /** No-arg constructor required by Lucene SPI. */
    public ParquetCodec() {
        super("ParquetCodec", new Lucene104Codec());
        parquetPath = null;
    }

    public ParquetCodec(Codec delegate, String parquetPath) {
        super("ParquetCodec", delegate);
        this.parquetPath = parquetPath;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return delegate.segmentInfoFormat().read(directory, segmentName, segmentID, context);
            }

            @Override
            public void write(Directory directory, SegmentInfo info, IOContext ioContext) throws IOException {
                if (parquetPath != null) {
                    info.putAttribute("parquet_file", parquetPath);
                }
                delegate.segmentInfoFormat().write(directory, info, ioContext);
            }
        };
    }
}
