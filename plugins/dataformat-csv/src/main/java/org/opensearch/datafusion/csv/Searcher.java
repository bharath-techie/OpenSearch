/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.vectorized.execution.spi.RecordBatchStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;


class Searcher implements Closeable {
    private Long ptr;
    private Long contextId;
    private long globalRunTimeId;
    private boolean isClose = false;

    Closeable onClose;


    Searcher(Long contextId, ShardView shardView, Long globalRunTimeId, Closeable onClose) {
        System.out.println("Searcher starting");
        this.contextId = contextId;
        this.globalRunTimeId = globalRunTimeId;
        this.ptr = nativeCreateSessionContext(0L, shardView.getCachePtr(), 0L);
        this.onClose = onClose;
        System.out.println("Created SessionContext");
    }

    public boolean isClosed() {
        return isClose;
    }


    public long executeSubstraitQuery(byte[] substraitPlanBytes) {
        CompletableFuture<RecordBatchStream> result = new CompletableFuture<>();
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

    private static native long nativeCreateSessionContext(long runtimePtr, long shardViewPtr, long globalRunTimePtr);
    private static native void destroySessionContext(long ptr);
    private static native long nativeExecuteSubstraitQuery(
        long sessionContextPtr,
        byte[] substraitPlan
    );

}
