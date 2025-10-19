/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.vectorized.execution.search.spi.SessionConfig;

public class DatafusionSessionConfig implements SessionConfig {

    private static native void updateNativeConfig(String key, Boolean value);


    public void updateConfig(String key, Boolean value) {
        updateNativeConfig(key, value);
    }

    @Override
    public Integer getBatchSize() {
        return 0;
    }

    @Override
    public void setBatchSize(int batchSize) {

    }

    @Override
    public void mergeFrom(SessionConfig other) {
        // If not null, means it needs to be overridden
        if(other.getBatchSize() != null) {
            setBatchSize(other.getBatchSize());
        }
    }

    native static long createDefaultNativeSessionConfigPtr();
    native static void updateParquetSessionConfig(long ptr, String key, String value);
    native static int getParquetSessionConfigValue(long ptr, String key);
}
