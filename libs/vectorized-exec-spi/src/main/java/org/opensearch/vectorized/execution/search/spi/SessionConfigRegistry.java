/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.search.spi;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry for configuration updates with pub-sub support.
 * Format-specific plugins (e.g., Parquet) publish config updates through this registry.
 * DataFusionPlugin subscribes to receive updates and holds the final config.
 */
public class SessionConfigRegistry {
    private final List<ConfigUpdateListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * Register a listener for config updates
     * @param listener The listener to register
     */
    public void registerListener(ConfigUpdateListener listener) {
        listeners.add(listener);
    }

    /**
     * Unregister a listener
     * @param listener The listener to unregister
     */
    public void unregisterListener(ConfigUpdateListener listener) {
        listeners.remove(listener);
    }

    public void publishSessionConfigUpdate(SessionConfig config) {
        for (ConfigUpdateListener listener : listeners) {
            listener.onSessionConfigUpdate(config);
        }
    }

}
