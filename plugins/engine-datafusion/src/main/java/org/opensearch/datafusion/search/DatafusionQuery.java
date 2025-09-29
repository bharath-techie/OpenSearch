/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import java.util.Iterator;
import java.util.List;

/**
 * Query representation for DataFusion execution.
 */
public class DatafusionQuery {
    private final byte[] substraitBytes;

    // List of Search executors which returns a result iterator which contains row id which can be joined in datafusion
    private final List<SearchExecutor> searchExecutors;

    /**
     * Creates a new DatafusionQuery.
     * 
     * @param substraitBytes the Substrait query bytes
     * @param searchExecutors the search executors
     */
    public DatafusionQuery(byte[] substraitBytes, List<SearchExecutor> searchExecutors) {
        this.substraitBytes = substraitBytes;
        this.searchExecutors = searchExecutors;
    }

    /**
     * Gets the Substrait query bytes.
     * 
     * @return the Substrait bytes
     */
    public byte[] getSubstraitBytes() {
        return substraitBytes;
    }

    /**
     * Gets the search executors.
     * 
     * @return the search executors
     */
    public List<SearchExecutor> getSearchExecutors() {
        return searchExecutors;
    }
}
