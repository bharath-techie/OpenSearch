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

class SessionContext implements Closeable {
    private Long ptr;
    private Long contextId;
    private boolean isClose = false;
    private ListingTableManager listingTableManager = ListingTableManager.getInstance();
    private ListingTable attachedListingTable;

    SessionContext(Long contextId, Long ptr) {
        this.contextId = contextId;
        this.ptr = ptr;
    }

    SessionContext(Long contextId) {
        this.contextId = contextId;
        this.ptr = nativeCreateSessionContext(constants.configKeys, constants.configValues);
    }

    public boolean isClosed() {
        return isClose;
    }

    public void attachListingTable() throws IOException {
        attachedListingTable = listingTableManager.attachSessionContextWithListingTable(contextId);
    }

    public long executeSubstraitQuery(byte[] substraitPlanBytes) {
        return nativeExecuteSubstraitQuery(this.ptr, attachedListingTable.getPtr(), substraitPlanBytes);
    }

    @Override
    public void close() throws IOException {
        destroySessionContext(contextId);
        listingTableManager.removeSessionContextFromListingTable(contextId);
        isClose = true;
    }

    private static native long nativeCreateSessionContext(String[] configKeys, String[] configValues);
    private static native void destroySessionContext(long ptr);
    private static native long nativeExecuteSubstraitQuery(long sessionContextPtr, long currentListingTablePtr, byte[] substraitPlan);

}
