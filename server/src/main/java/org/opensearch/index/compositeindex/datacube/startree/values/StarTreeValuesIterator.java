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
 * Star tree equivalent iterator of DISI. We need this since star tree documents are different from segment documents.
 */
@ExperimentalApi
public abstract class StarTreeValuesIterator {

    public abstract boolean advanceExact(int target) throws IOException;

    public static final int NO_MORE_ENTRIES = Integer.MAX_VALUE;

    public abstract int entryId();

    public abstract int nextEntry() throws IOException;

    /**
     * Advances to the first beyond the current whose document number is greater than or equal to
     * <i>target</i>, and returns the document number itself. Exhausts the iterator and returns {@link
     * #NO_MORE_ENTRIES} if <i>target</i> is greater than the highest document number in the set.
     *
     * <p>The behavior of this method is <b>undefined</b> when called with <code> target &le; current
     * </code>, or after the iterator has exhausted. Both cases may result in unpredicted behavior.
     *
     * <p>When <code> target &gt; current</code> it behaves as if written:
     *
     * <pre class="prettyprint">
     * int advance(int target) {
     *   int doc;
     *   while ((doc = nextDoc()) &lt; target) {
     *   }
     *   return doc;
     * }
     * </pre>
     *
     * Some implementations are considerably more efficient than that.
     *
     * <p><b>NOTE:</b> this method may be called with {@link #NO_MORE_ENTRIES} for efficiency by some
     * Scorers. If your implementation cannot efficiently determine that it should exhaust, it is
     * recommended that you check for that value in each call to this method.
     *
     * @since 2.9
     */
    public abstract int advance(int target) throws IOException;

    /**
     * Slow (linear) implementation of {@link #advance} relying on {@link #nextEntry()} to advance
     * beyond the target position.
     */
    protected final int slowAdvance(int target) throws IOException {
        assert entryId() < target;
        int doc;
        do {
            doc = nextEntry();
        } while (doc < target);
        return doc;
    }

    /**
     * Returns the estimated cost of this {@link org.apache.lucene.search.DocIdSetIterator}.
     *
     * <p>This is generally an upper bound of the number of documents this iterator might match, but
     * may be a rough heuristic, hardcoded value, or otherwise completely inaccurate.
     */
    public abstract long cost();
}
