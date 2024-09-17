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

@ExperimentalApi
public class SingletonSortedNumericDocValues extends StarTreeSortedNumericValues {
    private final StarTreeNumericValues in;

    public SingletonSortedNumericDocValues(StarTreeNumericValues in) {
        if (in.entryId() != -1) {
            throw new IllegalStateException("iterator has already been used: docID=" + in.entryId());
        }
        this.in = in;
    }

    /** Return the wrapped {@link StarTreeNumericValues} */
    public StarTreeNumericValues getNumericDocValues() {
        if (in.entryId() != -1) {
            throw new IllegalStateException("iterator has already been used: docID=" + in.entryId());
        }
        return in;
    }

    @Override
    public int entryId() {
        return in.entryId();
    }

    @Override
    public int nextEntry() throws IOException {
        return in.nextEntry();
    }

    @Override
    public int advance(int target) throws IOException {
        return in.advance(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return in.advanceExact(target);
    }

    @Override
    public long cost() {
        return in.cost();
    }

    @Override
    public long nextValue() throws IOException {
        return in.longValue();
    }

    @Override
    public int docValueCount() {
        return 1;
    }
}
