/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the lifecycle of Lucene {@link Weight} and per-segment scorer
 * instances for a single query.
 * <p>
 * Provides a JNI-friendly primitives-only API: callers receive {@code int}
 * keys from {@link #registerWeight} and {@link #createScorer}, then use
 * those keys to invoke {@link #collectDocs}, {@link #releaseScorer}, and
 * {@link #releaseWeight}. Java owns all scorer/weight state; the native
 * (Rust) side only holds lightweight int keys.
 * <p>
 * One manager is created per query and closed when the query finishes.
 * {@link #close()} acts as a safety net, releasing any weights and scorers
 * that were not explicitly released by the caller.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ScoreWeightQueryLifecycleManager implements Closeable {

    private final AtomicInteger nextKey = new AtomicInteger(1);
    private final Map<Integer, WeightEntry> weights = new ConcurrentHashMap<>();
    private final Map<Integer, ScorerEntry> scorers = new ConcurrentHashMap<>();

    /**
     * Registers a Lucene {@link Weight} with its associated leaf reader
     * contexts and returns an int key.
     *
     * @param weight the Lucene weight to manage
     * @param leaves the leaf reader contexts for the index
     * @return a unique key that identifies this weight
     */
    public int registerWeight(Weight weight, List<LeafReaderContext> leaves) {
        int key = nextKey.getAndIncrement();
        weights.put(key, new WeightEntry(weight, leaves));
        return key;
    }

    /**
     * Creates a per-segment scorer from the weight identified by
     * {@code weightKey} and returns a scorer key.
     *
     * @param weightKey  the weight key returned by {@link #registerWeight}
     * @param segmentOrd the segment ordinal within the weight's leaf list
     * @param minDoc     inclusive lower bound for doc collection
     * @param maxDoc     exclusive upper bound for doc collection
     * @return a unique scorer key, or {@code -1} if the weight key is
     *         invalid, the segment ordinal is out of range, or no docs match
     */
    public int createScorer(int weightKey, int segmentOrd, int minDoc, int maxDoc) {
        WeightEntry entry = weights.get(weightKey);
        if (entry == null || segmentOrd < 0 || segmentOrd >= entry.leaves.size()) {
            return -1;
        }
        try {
            Scorer scorer = entry.weight.scorer(entry.leaves.get(segmentOrd));
            if (scorer == null) {
                return -1;
            }
            int key = nextKey.getAndIncrement();
            scorers.put(key, new ScorerEntry(scorer.iterator(), minDoc, maxDoc));
            return key;
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * Collects matching document IDs for the scorer identified by
     * {@code scorerKey} within [{@code rowGroupMin}, {@code rowGroupMax}).
     *
     * @param scorerKey   the scorer key returned by {@link #createScorer}
     * @param rowGroupMin inclusive lower bound
     * @param rowGroupMax exclusive upper bound
     * @return packed {@code long[]} bitset of matching doc IDs relative to
     *         {@code rowGroupMin}, or empty array if the key is invalid or
     *         no docs match
     */
    public long[] collectDocs(int scorerKey, int rowGroupMin, int rowGroupMax) {
        ScorerEntry entry = scorers.get(scorerKey);
        if (entry == null) {
            return new long[0];
        }

        int effectiveMin = Math.max(rowGroupMin, entry.minDoc);
        int effectiveMax = Math.min(rowGroupMax, entry.maxDoc);
        if (effectiveMin >= effectiveMax) {
            return new long[0];
        }

        BitSet bitset = new BitSet(effectiveMax - effectiveMin);
        try {
            DocIdSetIterator iter = entry.iterator;
            int docId = entry.currentDoc;
            if (docId == DocIdSetIterator.NO_MORE_DOCS || docId >= entry.maxDoc) {
                return new long[0];
            }
            if (docId < effectiveMin) {
                docId = iter.advance(effectiveMin);
            }
            while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < effectiveMax) {
                bitset.set(docId - effectiveMin);
                docId = iter.nextDoc();
            }
            entry.currentDoc = docId;
        } catch (IOException e) {
            return new long[0];
        }
        return bitset.toLongArray();
    }

    /**
     * Returns the number of segments for the weight identified by
     * {@code weightKey}.
     *
     * @param weightKey the weight key returned by {@link #registerWeight}
     * @return the segment count, or {@code -1} if the key is invalid
     */
    public int getSegmentCount(int weightKey) {
        WeightEntry entry = weights.get(weightKey);
        return entry != null ? entry.leaves.size() : -1;
    }

    /**
     * Returns the max doc count for a segment within the weight identified
     * by {@code weightKey}.
     *
     * @param weightKey  the weight key returned by {@link #registerWeight}
     * @param segmentOrd the segment ordinal
     * @return the max doc count, or {@code -1} if the key or ordinal is invalid
     */
    public int getSegmentMaxDoc(int weightKey, int segmentOrd) {
        WeightEntry entry = weights.get(weightKey);
        if (entry == null || segmentOrd < 0 || segmentOrd >= entry.leaves.size()) {
            return -1;
        }
        return entry.leaves.get(segmentOrd).reader().maxDoc();
    }

    /**
     * Releases the scorer identified by {@code scorerKey}, removing it
     * from the registry.
     *
     * @param scorerKey the scorer key returned by {@link #createScorer}
     */
    public void releaseScorer(int scorerKey) {
        scorers.remove(scorerKey);
    }

    /**
     * Releases the weight identified by {@code weightKey}, removing it
     * from the registry.
     *
     * @param weightKey the weight key returned by {@link #registerWeight}
     */
    public void releaseWeight(int weightKey) {
        weights.remove(weightKey);
    }

    /**
     * Closes all remaining weights and scorers. Acts as a safety net for
     * any entries that were not explicitly released.
     */
    @Override
    public void close() {
        scorers.clear();
        weights.clear();
    }

    /**
     * Holds a Lucene {@link Weight} and its associated leaf reader contexts.
     */
    static class WeightEntry {
        final Weight weight;
        final List<LeafReaderContext> leaves;

        WeightEntry(Weight weight, List<LeafReaderContext> leaves) {
            this.weight = weight;
            this.leaves = leaves;
        }
    }

    /**
     * Holds a per-segment {@link DocIdSetIterator} with its doc range and
     * current position.
     */
    static class ScorerEntry {
        final DocIdSetIterator iterator;
        final int minDoc;
        final int maxDoc;
        int currentDoc = -1;

        ScorerEntry(DocIdSetIterator iterator, int minDoc, int maxDoc) {
            this.iterator = iterator;
            this.minDoc = minDoc;
            this.maxDoc = maxDoc;
        }
    }
}
