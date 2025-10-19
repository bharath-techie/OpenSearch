/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.search.spi;

public interface NativeConfiguration {

    void setListingTableOptions(long listingTableOptions);
    long getListingTableOptions();

    /**
     * Merges values from another session config, overriding non-default values
     * @param other The config to merge from
     */
    void mergeFrom(NativeConfiguration other);

}
