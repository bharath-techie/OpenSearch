/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ListingTableManager {

    private ListingTable currentListingTable;

    // How to create it the very first time?
    public ListingTableManager(ListingTable listingTable){
        this.currentListingTable = listingTable;
    }

    public ListingTable acquireListingTable() {
        currentListingTable.incRef();
        return currentListingTable;
    }


    public void createListingTable(String path, List<String> files) throws IOException {
        ListingTable listingTable = new ListingTable(path, files.toArray(new String[0]));
        swapReference(listingTable);
    }

    public synchronized void swapReference(ListingTable newListingTable) throws IOException {
        ListingTable oldReference = this.currentListingTable;
        this.currentListingTable = newListingTable;
        this.release(oldReference);
    }

    public void release(ListingTable reference) throws IOException {
        assert reference != null;
        reference.decRef();
    }
}
