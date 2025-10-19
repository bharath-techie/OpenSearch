/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.search.spi;

/**
 * Configuration interface for data formats.
 * Format-specific configs (e.g., Parquet) publish updates when settings change.
 * DataFusionPlugin subscribes to receive updates and holds the final config.
 */
public interface EngineConfig {

    /**
     * Gets the session config
     * @return The session config
     */
    SessionConfig getSessionConfig();

    NativeConfiguration getNativeConfiguration();

    /**
     * Updates the session config by merging values
     *
     * @param sessionConfig The session config to merge from
     * @return
     */
    EngineConfig updateSessionConfig(SessionConfig sessionConfig);

    /**
     * Updates the listing table options
     * @param nativeConfiguration The new listing table options
     */
    EngineConfig updateNativeConfiguration(NativeConfiguration nativeConfiguration);

}
