/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.search.spi;

import org.opensearch.vectorized.execution.search.DataFormat;

/**
 * Listener for configuration updates.
 * DataFusionPlugin implements this to receive updates from format-specific plugins (e.g., Parquet).
 */
public interface ConfigUpdateListener {
    void onSessionConfigUpdate(SessionConfig sessionConfig);
}
