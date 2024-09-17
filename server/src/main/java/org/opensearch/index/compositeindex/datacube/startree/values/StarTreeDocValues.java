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
 * Copy of DocValues in Lucene.
 * This class changes the types associated with iterators */
@ExperimentalApi
public final class StarTreeDocValues {

    /* no instantiation */
    private StarTreeDocValues() {}

    /** An empty NumericDocValues which returns no documents */
    public static final StarTreeNumericValues emptyNumeric() {
        return new StarTreeNumericValues() {
            private int doc = -1;

            @Override
            public int advance(int target) {
                return doc = NO_MORE_ENTRIES;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                doc = target;
                return false;
            }

            @Override
            public int entryId() {
                return doc;
            }

            @Override
            public int nextEntry() {
                return doc = NO_MORE_ENTRIES;
            }

            @Override
            public long cost() {
                return 0;
            }

            @Override
            public long longValue() {
                assert false;
                return 0;
            }
        };
    }

    /** An empty SortedNumericDocValues which returns zero values for every document */
    public static final StarTreeSortedNumericValues emptySortedNumeric() {
        return singleton(emptyNumeric());
    }

    /**
     * Returns a single-valued view of the SortedNumericDocValues, if it was previously wrapped with
     * {@link #singleton(StarTreeNumericValues)}, or null.
     */
    public static StarTreeNumericValues unwrapSingleton(StarTreeSortedNumericValues dv) {
        if (dv instanceof SingletonSortedNumericDocValues) {
            return ((SingletonSortedNumericDocValues) dv).getNumericDocValues();
        } else {
            return null;
        }
    }

    /** Returns a multi-valued view over the provided NumericDocValues */
    public static StarTreeSortedNumericValues singleton(StarTreeNumericValues dv) {
        return new SingletonSortedNumericDocValues(dv);
    }
}
