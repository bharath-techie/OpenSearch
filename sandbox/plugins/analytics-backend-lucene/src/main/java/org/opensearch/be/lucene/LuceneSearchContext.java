/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.ScoreWeightQueryLifecycleManager;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;

/**
 * Lucene-specific search execution context.
 * <p>
 * Input: a Lucene {@link Query}.
 * Output: a registered weight key + a {@link ScoreWeightQueryLifecycleManager}
 * that Rust uses for JNI callbacks to create scorers and stream bitsets
 * per partition range.
 * <p>
 * The lifecycle manager is created per-query and closed when this context
 * is closed, acting as a safety net for any weights/scorers not explicitly
 * released.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchContext implements SearchExecutionContext {

    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;

    private final DirectoryReader reader;
    private final LuceneEngineSearcher searcher;
    private final ScoreWeightQueryLifecycleManager scoreWeightManager;
    private Query query;

    /** The int key for the registered weight in the lifecycle manager. */
    private int weightKey;

    public LuceneSearchContext(
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        DirectoryReader reader
    ) throws IOException {
        this.reader = reader;
        IndexSearcher indexSearcher = new IndexSearcher(reader);
        this.searcher = new LuceneEngineSearcher(indexSearcher, reader);
        this.scoreWeightManager = new ScoreWeightQueryLifecycleManager();
        this.request = request;
        this.shardTarget = shardTarget;
    }

    public Query getQuery() {
        return query;
    }

    public DirectoryReader getReader() {
        return reader;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    /**
     * Returns the per-query lifecycle manager for weights and scorers.
     * The Rust/JNI side uses this to create scorers and collect docs
     * via int keys.
     */
    public ScoreWeightQueryLifecycleManager getScoreWeightManager() {
        return scoreWeightManager;
    }

    /**
     * Returns the int key for the registered weight.
     */
    public int getWeightKey() {
        return weightKey;
    }

    /**
     * Sets the int key for the registered weight. Called by
     * {@link LuceneEngineSearcher#search} after registering the weight.
     */
    public void setWeightKey(int weightKey) {
        this.weightKey = weightKey;
    }

    /**
     * Returns the number of segments for the registered weight.
     */
    public int getSegmentCount() {
        return scoreWeightManager.getSegmentCount(weightKey);
    }

    /**
     * Returns the max doc array for all segments of the registered weight.
     */
    public int[] getSegmentMaxDocs() {
        int count = getSegmentCount();
        if (count <= 0) {
            return new int[0];
        }
        int[] maxDocs = new int[count];
        for (int i = 0; i < count; i++) {
            maxDocs[i] = scoreWeightManager.getSegmentMaxDoc(weightKey, i);
        }
        return maxDocs;
    }

    @Override
    public ShardSearchRequest request() {
        return request;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public void close() throws IOException {
        scoreWeightManager.close();
        searcher.close();
    }
}
