/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.engine.read;

import org.opensearch.vectorized.execution.search.spi.NativeConfiguration;
import org.opensearch.vectorized.execution.search.spi.SessionConfig;

public class ParquetNativeConfiguration implements NativeConfiguration {

    private ParquetListingTableOptions parquetListingTableOptions;

    ParquetNativeConfiguration() {
        parquetListingTableOptions = new ParquetListingTableOptions();
    }

    @Override
    public void setListingTableOptions(long listingTableOptions) {
        throw new UnsupportedOperationException("Listing table options can't be modified for Parquet");
    }

    @Override
    public long getListingTableOptions() {
        return parquetListingTableOptions.getListingTableOptionsPtr();
    }

    @Override
    public void mergeFrom(NativeConfiguration other) {
        throw new UnsupportedOperationException("Not Supported for parquet Dataformat");
    }


}
