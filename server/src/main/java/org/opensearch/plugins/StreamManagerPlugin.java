/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.arrow.StreamManager;

/**
 * An interface for OpenSearch plugins to implement to provide a StreamManager.
 * This interface is used by the Arrow Flight plugin to get the StreamManager instance.
 * Other plugins can also implement this interface to provide their own StreamManager implementation.
 * @see org.opensearch.arrow.StreamManager
 */
public interface StreamManagerPlugin {
    /**
     * Returns the StreamManager instance for this plugin.
     *
     * @return The StreamManager instance
     */
    StreamManager getStreamManager();
}
