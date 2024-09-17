/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.values;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Star tree equivalent of SortedNumericDocValues
 */
@ExperimentalApi
public abstract class StarTreeSortedNumericValues extends StarTreeValuesIterator {
    /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
    protected StarTreeSortedNumericValues() {}

    /**
     * Iterates to the next value in the current document. Do not call this more than {@link
     * #docValueCount} times for the document.
     */
    public abstract long nextValue() throws IOException;

    /**
     * Retrieves the number of values for the current document. This must always be greater than zero.
     * It is illegal to call this method after {@link #advanceExact(int)} returned {@code false}.
     */
    public abstract int docValueCount();
}
