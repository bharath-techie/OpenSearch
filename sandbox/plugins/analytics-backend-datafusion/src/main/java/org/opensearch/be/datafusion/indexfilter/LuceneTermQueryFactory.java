/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.indexfilter;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.analytics.spi.IndexFilterProvider;
import org.opensearch.analytics.spi.IndexFilterProviderFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory: deserializes {@code "field\0value"} → Lucene TermQuery.
 * Creates one Weight per query, one Scorer per createCollector call.
 */
public class LuceneTermQueryFactory implements IndexFilterProviderFactory {

    private final DirectoryReader reader;

    public LuceneTermQueryFactory(Path luceneIndexDir) throws IOException {
        this.reader = DirectoryReader.open(FSDirectory.open(luceneIndexDir));
    }

    @Override
    public IndexFilterProvider create(byte[] queryBytes) throws Exception {
        String s = new String(queryBytes, StandardCharsets.UTF_8);
        int sep = s.indexOf('\0');
        if (sep < 0) throw new IllegalArgumentException("expected 'field\\0value', got: " + s);
        TermQuery query = new TermQuery(new Term(s.substring(0, sep), s.substring(sep + 1)));

        IndexSearcher searcher = new IndexSearcher(reader);
        Query rewritten = searcher.rewrite(query);
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        List<LeafReaderContext> leaves = reader.leaves();

        return new TermQueryProvider(weight, leaves);
    }

    public void closeReader() throws IOException { reader.close(); }

    private static class TermQueryProvider implements IndexFilterProvider {
        private final Weight weight;
        private final List<LeafReaderContext> leaves;
        private final ConcurrentHashMap<Integer, CollectorState> collectors = new ConcurrentHashMap<>();
        private final AtomicInteger nextKey = new AtomicInteger(1);

        TermQueryProvider(Weight weight, List<LeafReaderContext> leaves) {
            this.weight = weight;
            this.leaves = leaves;
        }

        @Override
        public int createCollector(int segmentOrd, int minDoc, int maxDoc) {
            try {
                // Fresh scorer per collector — forward-only, one per chunk.
                Scorer scorer = weight.scorer(leaves.get(segmentOrd));
                int key = nextKey.getAndIncrement();
                collectors.put(key, new CollectorState(
                    scorer != null ? scorer.iterator() : null, minDoc, maxDoc
                ));
                return key;
            } catch (Exception e) {
                System.err.println("[TermQueryProvider] createCollector failed: " + e);
                return -1;
            }
        }

        @Override
        public int collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
            if (maxDoc <= minDoc) return 0;
            CollectorState state = collectors.get(collectorKey);
            if (state == null) return 0;

            int span = maxDoc - minDoc;
            FixedBitSet bits = new FixedBitSet(span);

            DocIdSetIterator it = state.iterator;
            if (it != null) {
                int scanFrom = Math.max(minDoc, state.partitionMin);
                int scanTo = Math.min(maxDoc, state.partitionMax);
                if (scanFrom < scanTo) {
                    try {
                        int docId = it.docID();
                        if (docId != DocIdSetIterator.NO_MORE_DOCS && docId < scanFrom) {
                            docId = it.advance(scanFrom);
                        }
                        while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < scanTo) {
                            bits.set(docId - minDoc);
                            docId = it.nextDoc();
                        }
                    } catch (IOException e) {
                        // partial results
                    }
                }
            }

            long[] words = bits.getBits();
            MemorySegment.copy(words, 0, out, ValueLayout.JAVA_LONG, 0, words.length);
            return words.length;
        }

        @Override
        public void releaseCollector(int collectorKey) {
            collectors.remove(collectorKey);
        }

        @Override
        public void close() { collectors.clear(); }
    }

    private record CollectorState(DocIdSetIterator iterator, int partitionMin, int partitionMax) {}
}
