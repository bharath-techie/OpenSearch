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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class ListingTable implements Closeable {
    private Long ptr;
    private AtomicInteger refCount = new AtomicInteger(0);

    // How to make sure that Apart from ListingTableManager no one else can create or close it?
    ListingTable(String path, String[] files) {
        ptr = createListingTable(path, files);
        incRef();
    }

    public Long getPtr() {
        return ptr;
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

    private static native long createListingTable(String path, String[] files);
    private static native void destroyListingTable(long ptr);

    @Override
    public void close() throws IOException {
        destroyListingTable(this.ptr);
        this.ptr = null;
    }
}
