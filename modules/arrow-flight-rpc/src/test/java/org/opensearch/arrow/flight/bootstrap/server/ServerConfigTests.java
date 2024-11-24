/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.bootstrap.server;

import org.apache.arrow.flight.Location;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;

public class ServerConfigTests extends OpenSearchTestCase {

    private Settings settings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder()
            .put("node.attr.transport.stream.port", 8815)
            .put("arrow.allocation.manager.type", "Netty")
            .put("arrow.enable_null_check_for_get", false)
            .put("arrow.enable_unsafe_memory_access", true)
            .put("arrow.memory.debug.allocator", false)
            .put("arrow.ssl.enable", true)
            .put("thread_pool.flight-server.min", 1)
            .put("thread_pool.flight-server.max", 4)
            .put("thread_pool.flight-server.keep_alive", TimeValue.timeValueMinutes(5))
            .build();
    }

    public void testInit() {
        ServerConfig.init(settings);

        // Verify system properties are set correctly
        assertEquals("Netty", System.getProperty("arrow.allocation.manager.type"));
        assertEquals("false", System.getProperty("arrow.enable_null_check_for_get"));
        assertEquals("true", System.getProperty("arrow.enable_unsafe_memory_access"));
        assertEquals("false", System.getProperty("arrow.memory.debug.allocator"));

        // Verify SSL settings
        assertTrue(ServerConfig.isSslEnabled());

        ScalingExecutorBuilder executorBuilder = ServerConfig.getExecutorBuilder();
        assertNotNull(executorBuilder);
        assertEquals(3, executorBuilder.getRegisteredSettings().size());
        assertEquals(1, executorBuilder.getRegisteredSettings().get(0).get(settings)); // min
        assertEquals(4, executorBuilder.getRegisteredSettings().get(1).get(settings)); // max
        assertEquals(TimeValue.timeValueMinutes(5), executorBuilder.getRegisteredSettings().get(2).get(settings)); // keep alive
    }

    public void testGetServerLocation() {
        ServerConfig.init(settings);
        Location location = ServerConfig.getServerLocation();
        assertNotNull(location);
        assertEquals("localhost", location.getUri().getHost());
        assertEquals(8815, location.getUri().getPort());
        assertTrue(location.getUri().getScheme().contains("grpc+tls"));
    }

    public void testGetServerLocationWithoutSsl() {
        Settings noSslSettings = Settings.builder().put(settings).put("arrow.ssl.enable", false).build();

        ServerConfig.init(noSslSettings);
        Location location = ServerConfig.getServerLocation();
        assertNotNull(location);
        assertEquals("localhost", location.getUri().getHost());
        assertEquals(8815, location.getUri().getPort());
        assertTrue(location.getUri().getScheme().contains("grpc"));
    }

    public void testGetSettings() {
        var settings = ServerConfig.getSettings();
        assertNotNull(settings);
        assertFalse(settings.isEmpty());

        assertTrue(settings.contains(ServerConfig.ARROW_ALLOCATION_MANAGER_TYPE));
        assertTrue(settings.contains(ServerConfig.ARROW_ENABLE_NULL_CHECK_FOR_GET));
        assertTrue(settings.contains(ServerConfig.ARROW_ENABLE_UNSAFE_MEMORY_ACCESS));
        assertTrue(settings.contains(ServerConfig.ARROW_ENABLE_DEBUG_ALLOCATOR));
        assertTrue(settings.contains(ServerConfig.ARROW_SSL_ENABLE));
    }

    public void testDefaultSettings() {
        Settings defaultSettings = Settings.EMPTY;
        ServerConfig.init(defaultSettings);

        // Verify default values
        assertEquals(8815, ServerConfig.STREAM_PORT.get(defaultSettings).intValue());
        assertEquals("Netty", ServerConfig.ARROW_ALLOCATION_MANAGER_TYPE.get(defaultSettings));
        assertFalse(ServerConfig.ARROW_ENABLE_NULL_CHECK_FOR_GET.get(defaultSettings));
        assertTrue(ServerConfig.ARROW_ENABLE_UNSAFE_MEMORY_ACCESS.get(defaultSettings));
        assertFalse(ServerConfig.ARROW_ENABLE_DEBUG_ALLOCATOR.get(defaultSettings));
        assertFalse(ServerConfig.ARROW_SSL_ENABLE.get(defaultSettings));
    }

    public void testInvalidPortSetting() {
        Settings invalidSettings = Settings.builder()
            .put("node.attr.transport.stream.port", 100) // Below minimum port number
            .build();

        expectThrows(IllegalArgumentException.class, () -> { ServerConfig.init(invalidSettings); });
    }
}
