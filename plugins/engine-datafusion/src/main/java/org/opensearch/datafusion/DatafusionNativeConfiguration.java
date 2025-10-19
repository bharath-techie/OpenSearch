/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.vectorized.execution.search.spi.NativeConfiguration;

public class DatafusionNativeConfiguration implements NativeConfiguration {

    private long listingTableOptions;

    @Override
    public void setListingTableOptions(long listingTableOptions) {
        this.listingTableOptions = listingTableOptions;
    }

    @Override
    public long getListingTableOptions() {
        if(listingTableOptions == 0) {
            throw new RuntimeException("Listing table options not set");
        }
        return listingTableOptions;
    }

    @Override
    public void mergeFrom(NativeConfiguration other) {
        if(other.getListingTableOptions() != 0) {
            setListingTableOptions(other.getListingTableOptions());
        }
    }
}
