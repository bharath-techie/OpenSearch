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
    private final DocIdSetIterator docIdSetIterator;

    /**
     * The value associated with the latest document.
     */
    private long docValue = -1;

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
        this.docIdSetIterator = docIdSetIterator;
    }

    /**
     * Creates a SequentialDocValuesIterator with an empty DocIdSetIterator.
     *
     */
    public SequentialDocValuesIterator() {
        this.docIdSetIterator = new DocIdSetIterator() {
            @Override
            public int docID() {
                return NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() {
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    /**
     * Returns the value associated with the latest document.
     *
     * @return the value associated with the latest document
     */
    public long getDocValue() {
        return docValue;
    }

    /**
     * Sets the value associated with the latest document.
     *
     * @param docValue the value to be associated with the latest document
     */
    public void setDocValue(Long docValue) {
        // this.docValue = docValue;
    }

    public void setDocValue(long docValue) {
        this.docValue = docValue;
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
    public void setDocId(int docId) {
        this.docId = docId;
    }

    /**
     * Returns the DocIdSetIterator associated with this instance.
     *
     * @return the DocIdSetIterator associated with this instance
     */
    public DocIdSetIterator getDocIdSetIterator() {
        return docIdSetIterator;
    }

    public int nextDoc(int currentDocId) throws IOException {
        // if doc id stored is less than or equal to the requested doc id , return the stored doc id
        if (docId >= currentDocId) {
            return docId;
        }
        setDocId(this.docIdSetIterator.nextDoc());
        return docId;
    }

    public long value(int currentDocId) throws IOException {
        if (this.getDocIdSetIterator() instanceof SortedNumericDocValues) {
            SortedNumericDocValues sortedNumericDocValues = (SortedNumericDocValues) this.getDocIdSetIterator();
            if (currentDocId < 0) {
                throw new IllegalStateException("invalid doc id to fetch the next value");
            }
            if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                throw new IllegalStateException("DocValuesIterator is already exhausted");
            }

            if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                return -1;
            }

            if (docValue == -1) {
                setDocValue(sortedNumericDocValues.nextValue());
            }
            if (docId == currentDocId) {
                long nextValue = docValue;
                docValue = -1;
                return nextValue;
            } else {
                return -1;
            }
        } else {
            throw new IllegalStateException("Unsupported Iterator requested for SequentialDocValuesIterator");
        }
    }
}
