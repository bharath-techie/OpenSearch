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
 * Star tree equivalent of NumericDocValues
 */
@ExperimentalApi
public abstract class StarTreeNumericValues extends StarTreeValuesIterator {
    /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
    protected StarTreeNumericValues() {}

    /**
     * Returns the numeric value for the current document ID. It is illegal to call this method after
     * {@link #advanceExact(int)} returned {@code false}.
     *
     * @return numeric value
     */
    public abstract long longValue() throws IOException;
}
