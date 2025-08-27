/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import org.apache.lucene.store.AlreadyClosedException;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;


class Searcher implements Closeable {
    private Long ptr;
    private Long contextId;
    private long globalRunTimeId;
    private boolean isClose = false;

    Closeable onClose;


    Searcher(Long contextId, ShardView shardView, Long globalRunTimeId, Closeable onClose) {
        this.contextId = contextId;
        this.globalRunTimeId = globalRunTimeId;
        this.ptr = nativeCreateSessionContext(constants.configKeys, constants.configValues, shardView.getCachePtr());
        this.onClose = onClose;
    }

    public boolean isClosed() {
        return isClose;
    }


    public long executeSubstraitQuery(byte[] substraitPlanBytes) {
        return nativeExecuteSubstraitQuery(this.ptr, substraitPlanBytes);
    }

    @Override
    public void close() {
        try {
            destroySessionContext(this.ptr);
            onClose.close();
        } catch(IOException e) {
            throw new UncheckedIOException("failed to close", e);
        } catch (AlreadyClosedException e) {
            throw new AssertionError(e);
        } finally {
            isClose = true;
        }
    }

    private static native long nativeCreateSessionContext(String[] configKeys, String[] configValues, long shardViewPtr);
    private static native void destroySessionContext(long ptr);
    private static native long nativeExecuteSubstraitQuery(long sessionContextPtr, byte[] substraitPlan);

}
