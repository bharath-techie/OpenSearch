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

    private static ConcurrentHashMap<Long, ListingTable> listingTables = new ConcurrentHashMap<>();
    private ListingTable currentListingTable;
    private static volatile ListingTableManager instance;

    // How to create it the very first time?
    private ListingTableManager(ListingTable listingTable){
        this.currentListingTable = listingTable;
    }

    // Added to avoid failure
    private ListingTableManager() {
    }

    // How to handle the initialisation?
    public static ListingTableManager getInstance() {
        if (instance == null) {
            synchronized (ListingTableManager.class) {
                if (instance == null) {
                    instance = new ListingTableManager();
                }
            }
        }
        return instance;
    }

    public ListingTable getCurrentListingTable() {
        return currentListingTable;
    }

    public void attachSessionContextWithListingTable(long session_id, ListingTable listingTable) {
        listingTable.incRef();
        listingTables.put(session_id, listingTable);
    }

    public synchronized ListingTable attachSessionContextWithListingTable(long session_id) {
        currentListingTable.incRef();
        listingTables.put(session_id, currentListingTable);
        return currentListingTable;
    }

    public void removeSessionContextFromListingTable(long session_id) throws IOException {
        ListingTable listingTable = listingTables.get(session_id);
        listingTable.decRef();
        listingTables.remove(session_id);
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
