/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * One parquet file in a shard, paired with the writer generation of the segment that
 * produced it.
 *
 * <p>Writer generation is the stable per-segment identifier the Rust/native side uses to
 * identify segments across the FFM boundary (see
 * {@link org.opensearch.be.datafusion.indexfilter.FilterTreeCallbacks#createCollector}).
 *
 * <p><b>Source of truth.</b> The value originates in the
 * {@link org.opensearch.index.engine.exec.WriterFileSet#writerGeneration()} stored on the
 * catalog snapshot and is supplied unchanged at reader-creation time. The Rust side never
 * parses filenames or reads parquet footer metadata to recover this value; the catalog is
 * authoritative. Footer-kv and SegmentInfo attribute writes still happen on the writer side
 * but are consulted by the read path only for assertion/regression checks.
 *
 * @param writerGeneration generation of the segment this file belongs to
 * @param fileName shard-relative file name (same as the strings stored in
 *                 {@code WriterFileSet.files()})
 * @opensearch.experimental
 */
@ExperimentalApi
public record SegmentFile(long writerGeneration, String fileName) {
    public SegmentFile {
        Objects.requireNonNull(fileName, "fileName must not be null");
    }
}
