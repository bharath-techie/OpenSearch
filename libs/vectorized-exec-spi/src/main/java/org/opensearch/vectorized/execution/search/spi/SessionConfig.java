/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.search.spi;

/**
 * Session configuration for query execution
 */
public interface SessionConfig {

    /**
     * Gets the batch size
     * @return The batch size
     */
    default Integer getBatchSize() {
        return null;
    }

    /**
     * Sets the batch size
     * @param batchSize The batch size
     */
    void setBatchSize(int batchSize);

    /**
     * Merges values from another session config, overriding non-default values
     * @param other The config to merge from
     */
    void mergeFrom(SessionConfig other);

}
