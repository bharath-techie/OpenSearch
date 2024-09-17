/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.values;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Wraps DISI and gives StarTreeValuesIterator
 */
@ExperimentalApi
public class StarTreeDisiWrapperIterator {
    DocIdSetIterator docIdSetIterator;

    public StarTreeDisiWrapperIterator(DocIdSetIterator docIdSetIterator) {
        this.docIdSetIterator = docIdSetIterator;
    }

    StarTreeValuesIterator getIterator() {
        return new StarTreeValuesIterator() {
            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int entryId() {
                return docIdSetIterator.docID();
            }

            @Override
            public int nextEntry() throws IOException {
                return docIdSetIterator.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return docIdSetIterator.advance(target);
            }

            @Override
            public long cost() {
                return docIdSetIterator.cost();
            }
        };
    }
}
