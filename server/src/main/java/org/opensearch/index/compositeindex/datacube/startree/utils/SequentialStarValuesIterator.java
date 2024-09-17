
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.values.StarTreeSortedNumericValues;
import org.opensearch.index.compositeindex.datacube.startree.values.StarTreeValuesIterator;

import java.io.IOException;

/**
 * Coordinates the reading of documents across multiple DocIdSetIterators.
 * It encapsulates a single DocIdSetIterator and maintains the latest document ID and its associated value.
 *
 * TODO : revisit this, can be better
 * @opensearch.experimental
 */
@ExperimentalApi
public class SequentialStarValuesIterator extends SequentialDocValuesIterator {

    /**
     * The star tree iterator associated for each field.
     */
    private final StarTreeValuesIterator starValuesIterator;
    /**
     * The id of the latest document.
     */
    private int entryId = -1;

    /**
     * Constructs a new SequentialStarValuesIterator instance with the given starTreeValuesIterator.
     *
     * @param starTreeValuesIterator the StarTreeValuesIterator to be associated with this instance
     */
    public SequentialStarValuesIterator(StarTreeValuesIterator starTreeValuesIterator) {
        super();
        this.starValuesIterator = starTreeValuesIterator;
    }

    /**
     * Returns the id of the latest document.
     *
     * @return the id of the latest document
     */
    @Override
    public int getEntryId() {
        return entryId;
    }

    /**
     * Sets the id of the latest document.
     *
     * @param entryId the ID of the latest document
     */
    private void setEntryId(int entryId) {
        this.entryId = entryId;
    }

    @Override
    public int nextEntry(int currentEntryId) throws IOException {
        // if doc id stored is less than or equal to the requested doc id , return the stored doc id
        if (entryId >= currentEntryId) {
            return entryId;
        }
        setEntryId(this.starValuesIterator.nextEntry());
        return entryId;
    }

    @Override
    public Long value(int currentEntryId) throws IOException {
        if (this.starValuesIterator instanceof StarTreeValuesIterator) {
            StarTreeSortedNumericValues sortedNumericDocValues = (StarTreeSortedNumericValues) this.starValuesIterator;
            if (currentEntryId < 0) {
                throw new IllegalStateException("invalid doc id to fetch the next value");
            }
            if (currentEntryId == StarTreeValuesIterator.NO_MORE_ENTRIES) {
                throw new IllegalStateException("DocValuesIterator is already exhausted");
            }
            if (entryId == StarTreeValuesIterator.NO_MORE_ENTRIES || entryId != currentEntryId) {
                return null;
            }
            return sortedNumericDocValues.nextValue();

        } else {
            throw new IllegalStateException("Unsupported Iterator requested for SequentialStarValuesIterator");
        }
    }
}
