/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.values;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;

/**
 * Copy of DocValuesProducer specific to star tree
 */
@ExperimentalApi
public abstract class StarTreeValuesProducer implements Closeable {
    /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
    protected StarTreeValuesProducer() {}

    /**
     * Returns {@link StarTreeNumericValues} for this field. The returned instance need not be thread-safe:
     * it will only be used by a single thread. The behavior is undefined if the doc values type of
     * the given field is not {@link DocValuesType#NUMERIC}. The return value is never {@code null}.
     */
    public abstract StarTreeNumericValues getStarTreeNumericValues(FieldInfo field) throws IOException;

    /**
     * Returns {@link StarTreeSortedNumericValues} for this field. The returned instance need not be
     * thread-safe: it will only be used by a single thread. The behavior is undefined if the doc
     * values type of the given field is not {@link DocValuesType#SORTED_NUMERIC}. The return value is
     * never {@code null}.
     */
    public abstract StarTreeSortedNumericValues getStarTreeSortedNumericValues(FieldInfo field) throws IOException;

    /**
     * Checks consistency of this producer
     *
     * <p>Note that this may be costly in terms of I/O, e.g. may involve computing a checksum value
     * against large data files.
     *
     * @lucene.internal
     */
    public abstract void checkIntegrity() throws IOException;

    /**
     * Returns an instance optimized for merging. This instance may only be consumed in the thread
     * that called {@link #getMergeInstance()}.
     *
     * <p>The default implementation returns {@code this}
     */
    public StarTreeValuesProducer getMergeInstance() {
        return this;
    }
}
