/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.engine.read;

public class ParquetListingTableOptions {

    private long listingTableOptionsPtr;

    ParquetListingTableOptions() {
        listingTableOptionsPtr = createListingTableOptionsPtr();
    }

    public long getListingTableOptionsPtr() {
        return listingTableOptionsPtr;
    }

    native static long createListingTableOptionsPtr();

}
