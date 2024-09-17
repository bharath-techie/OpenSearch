
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;

import java.io.IOException;

/**
 * Coordinates the reading of documents across multiple DocIdSetIterators.
 * It encapsulates a single DocIdSetIterator and maintains the latest document ID and its associated value.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SequentialDocValuesIterator {

    /**
     * The doc id set iterator associated for each field.
     */
    private final StarTreeValuesIterator starTreeValuesIterator;

    /**
     * The id of the latest document.
     */
    private int docId = -1;

    /**
     * Constructs a new SequentialDocValuesIterator instance with the given DocIdSetIterator.
     *
     * @param docIdSetIterator the DocIdSetIterator to be associated with this instance
     */
    public SequentialDocValuesIterator(DocIdSetIterator docIdSetIterator) {
        if (docIdSetIterator instanceof SortedNumericDocValues) {
            this.starTreeValuesIterator = new SortedNumericStarTreeValuesIterator(docIdSetIterator);
        } else {
            throw new IllegalStateException("Unsupported Iterator requested for SequentialDocValuesIterator");
        }
    }

    public SequentialDocValuesIterator(StarTreeValuesIterator starTreeValuesIterator) {
        this.starTreeValuesIterator = starTreeValuesIterator;
    }

    /**
     * Returns the id of the latest document.
     *
     * @return the id of the latest document
     */
    public int getDocId() {
        return docId;
    }

    /**
     * Sets the id of the latest document.
     *
     * @param docId the ID of the latest document
     */
    private void setDocId(int docId) {
        this.docId = docId;
    }

    public int nextDoc(int currentDocId) throws IOException {
        // if doc id stored is less than or equal to the requested doc id , return the stored doc id
        if (docId >= currentDocId) {
            return docId;
        }
        setDocId(this.starTreeValuesIterator.nextEntry());
        return docId;
    }

    public Long value(int currentDocId) throws IOException {
        if (starTreeValuesIterator instanceof SortedNumericStarTreeValuesIterator) {
            if (currentDocId < 0) {
                throw new IllegalStateException("invalid doc id to fetch the next value");
            }
            if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                throw new IllegalStateException("StarTreeValuesIterator is already exhausted");
            }
            if (docId == DocIdSetIterator.NO_MORE_DOCS || docId != currentDocId) {
                return null;
            }
            return ((SortedNumericStarTreeValuesIterator) starTreeValuesIterator).nextValue();

        } else {
            throw new IllegalStateException("Unsupported Iterator requested for SequentialDocValuesIterator");
        }
    }
}
