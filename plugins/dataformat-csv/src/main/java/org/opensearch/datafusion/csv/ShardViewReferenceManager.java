/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import java.io.IOException;

public class ShardViewReferenceManager {
    // catalog vs cacheptr
    private final String directoryPath;
    private ShardView currentShardView;

    public ShardView acquireShardView(String path) {
        if (currentShardView == null) {
            throw new RuntimeException("Invalid state of ShardView: " + path);
        }
        currentShardView.incRef();
        return currentShardView;
    }

    public ShardViewReferenceManager(String path, String[] files) throws IOException {
        this.directoryPath = path;
        System.out.println("Starting Creation of View");
        this.currentShardView = new ShardView(path, files);

        System.out.println("Endning Creation of View");
    }

    public void swapShardViewReference(String path, String[] files) throws IOException {
        this.release(currentShardView);
        currentShardView = new ShardView(path, files);
    }

    public void release(ShardView reference) throws IOException {
        assert reference != null : "Shard view can't be null";
        reference.decRef();
    }
}
