/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import org.opensearch.common.lease.Releasable;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SessionContextSupplier implements Releasable {
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public final Searcher acquireSessionContext(){
        final Searcher searcher = acquireSessionContextInternal();
        return searcher;
    }

    @Override
    public final void close() {
        if(closed.compareAndSet(false, true)) {
            doClose();
        } else {
            assert false: "SearchSupplier is released twice";
        }
    }

    protected abstract void doClose();
    protected abstract Searcher acquireSessionContextInternal();
}
