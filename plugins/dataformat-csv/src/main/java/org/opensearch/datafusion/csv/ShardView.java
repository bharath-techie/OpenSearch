/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

// JNI from java to rust
// substrait
// Harcode --> file --> register as the table with the same name
public class ShardView implements Closeable {
    public String directoryPath;
    public String[] files;
    private AtomicInteger refCount = new AtomicInteger(0);
    public long cachePtr;

    public ShardView(String directoryPath, String[] files) {
        this.directoryPath = directoryPath;
        this.files = files;
        this.cachePtr = createShardViewCache(directoryPath, files);
        incRef();
    }

    public long getCachePtr() {
        return cachePtr;
    }

    public void incRef() {
        refCount.getAndIncrement();
    }

    public void decRef() throws IOException {
        if(refCount.get() == 0) {
            throw new IllegalStateException("Listing table has been already closed");
        }

        int currRefCount = refCount.decrementAndGet();
        if(currRefCount == 0) {
            this.close();
        }

    }

    private static native long createShardViewCache(String path, String[] files);
    private static native void destroyShardViewCache(long ptr);

    @Override
    public void close() throws IOException {
        if(cachePtr == -1L) {
            throw new IllegalStateException("Listing table has been already closed");
        }

        destroyShardViewCache(this.cachePtr);
        this.cachePtr = -1;
    }
}
