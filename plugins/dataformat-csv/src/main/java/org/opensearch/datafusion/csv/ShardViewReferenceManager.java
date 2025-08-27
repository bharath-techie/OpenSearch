/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class ShardViewReferenceManager {
    // catalog vs cacheptr

    private final ConcurrentHashMap<String, ShardView> currentShardViews = new ConcurrentHashMap<>();

    public ShardView acquireShardView(String path) {
        ShardView shardView = currentShardViews.get(path);
        if (shardView == null) {
            throw new RuntimeException("ShardView not found for path: " + path);
        }
        shardView.incRef();
        return shardView;
    }

    public void createShardView(String path, String[] files) throws IOException {
        ShardView shardView = new ShardView(path, files);
        currentShardViews.put(path, shardView);
    }

    public void swapShardViewReference(String path, String[] files) throws IOException {
        ShardView currentShardView = currentShardViews.get(path);
        this.release(currentShardView);
        createShardView(path, files);
    }

    public void release(ShardView reference) throws IOException {
        assert reference != null : "Shard view can't be null";
        reference.decRef();
    }
}
