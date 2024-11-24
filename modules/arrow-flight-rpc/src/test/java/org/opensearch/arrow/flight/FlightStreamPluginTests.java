/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.opensearch.common.util.FeatureFlags.ARROW_STREAMS_SETTING;
import static org.mockito.Mockito.mock;

public class FlightStreamPluginTests extends OpenSearchTestCase {
    private Settings settings;
    private FlightStreamPlugin plugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put("node.attr.transport.stream.port", "8815").put(ARROW_STREAMS_SETTING.getKey(), true).build();
        plugin = new FlightStreamPlugin(settings);
    }

    public void testPluginEnableAndDisable() throws IOException {
        FeatureFlags.initializeFeatureFlags(settings);
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        Collection<Object> components = plugin.createComponents(
            null,
            clusterService,
            threadPool,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertNotNull(components);
        assertFalse(components.isEmpty());

        List<ExecutorBuilder<?>> executorBuilders = plugin.getExecutorBuilders(settings);
        assertNotNull(executorBuilders);
        assertFalse(executorBuilders.isEmpty());

        StreamManager streamManager = plugin.getStreamManager();
        assertNotNull(streamManager);

        List<Setting<?>> settings = plugin.getSettings();
        assertNotNull(settings);
        assertFalse(settings.isEmpty());

        plugin.close();

        Settings disabledSettings = Settings.builder()
            .put("node.attr.transport.stream.port", "8815")
            .put(ARROW_STREAMS_SETTING.getKey(), false)
            .build();
        FeatureFlags.initializeFeatureFlags(disabledSettings);
        FlightStreamPlugin disabledPlugin = new FlightStreamPlugin(disabledSettings);

        Collection<Object> disabledPluginComponents = disabledPlugin.createComponents(
            null,
            clusterService,
            threadPool,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertTrue(disabledPluginComponents.isEmpty());
        assertNull(disabledPlugin.getStreamManager());
        assertTrue(disabledPlugin.getExecutorBuilders(disabledSettings).isEmpty());
        disabledPlugin.close();
    }
}
