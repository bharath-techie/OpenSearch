/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.IndexFilterProvider;
import org.opensearch.index.engine.exec.SegmentCollector;

import java.io.IOException;
import java.util.BitSet;

/**
 * Lucene-backed {@link IndexFilterProvider}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneIndexFilterProvider implements IndexFilterProvider<Query, LuceneIndexFilterContext, DirectoryReader> {

    @Override
    public LuceneIndexFilterContext createContext(Query query, DirectoryReader reader) throws IOException {
        return new LuceneIndexFilterContext(query, reader);
    }

    @Override
    public SegmentCollector createCollector(LuceneIndexFilterContext context, int segmentOrd, int minDoc, int maxDoc) {
        try {
            Scorer scorer = context.getWeight().scorer(context.getLeaves().get(segmentOrd));
            if (scorer == null) {
                return EMPTY_COLLECTOR;
            }
            return new LuceneSegmentCollector(scorer.iterator(), minDoc, maxDoc);
        } catch (IOException e) {
            return EMPTY_COLLECTOR;
        }
    }

    @Override
    public void close() {}

    private static final SegmentCollector EMPTY_COLLECTOR = (min, max) -> new long[0];

    private static class LuceneSegmentCollector implements SegmentCollector {
        private final DocIdSetIterator iterator;
        private final int collectorMinDoc;
        private final int collectorMaxDoc;
        private int currentDoc = -1;

        LuceneSegmentCollector(DocIdSetIterator iterator, int minDoc, int maxDoc) {
            this.iterator = iterator;
            this.collectorMinDoc = minDoc;
            this.collectorMaxDoc = maxDoc;
        }

        @Override
        public long[] collectDocs(int minDoc, int maxDoc) {
            int effectiveMin = Math.max(minDoc, collectorMinDoc);
            int effectiveMax = Math.min(maxDoc, collectorMaxDoc);
            if (effectiveMin >= effectiveMax) {
                return new long[0];
            }

            BitSet bitset = new BitSet(effectiveMax - effectiveMin);
            try {
                int docId = currentDoc;
                if (docId == DocIdSetIterator.NO_MORE_DOCS || docId >= collectorMaxDoc) {
                    return new long[0];
                }
                if (docId < effectiveMin) {
                    docId = iterator.advance(effectiveMin);
                }
                while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < effectiveMax) {
                    bitset.set(docId - effectiveMin);
                    docId = iterator.nextDoc();
                }
                currentDoc = docId;
            } catch (IOException e) {
                return new long[0];
            }
            return bitset.toLongArray();
        }
    }
}
