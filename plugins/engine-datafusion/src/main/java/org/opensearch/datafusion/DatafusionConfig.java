/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.vectorized.execution.search.spi.EngineConfig;
import org.opensearch.vectorized.execution.search.spi.NativeConfiguration;
import org.opensearch.vectorized.execution.search.spi.SessionConfig;

public class DatafusionConfig implements EngineConfig {

    private SessionConfig sessionConfig;
    private NativeConfiguration nativeConfiguration;

    public DatafusionConfig() {
        this.sessionConfig = new DatafusionSessionConfig();
        this.nativeConfiguration = new DatafusionNativeConfiguration();
    }

    @Override
    public SessionConfig getSessionConfig() {
        return this.sessionConfig;
    }

    @Override
    public NativeConfiguration getNativeConfiguration() {
        return this.nativeConfiguration;
    }

    @Override
    public EngineConfig updateSessionConfig(SessionConfig sessionConfig) {
        // TODO:: It should update, not mutate the object here.
        this.sessionConfig.mergeFrom(sessionConfig);
        return this;
    }

    @Override
    public EngineConfig updateNativeConfiguration(NativeConfiguration nativeConfiguration) {
        this.nativeConfiguration.mergeFrom(nativeConfiguration);
        return this;
    }

}
