/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.bootstrap;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.OpenSearchFlightClient;
import org.opensearch.arrow.flight.bootstrap.client.FlightClientBuilder;
import org.opensearch.arrow.flight.bootstrap.tls.DefaultSslContextProvider;
import org.opensearch.arrow.flight.bootstrap.tls.DisabledSslContextProvider;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlightServiceTests extends OpenSearchTestCase {

    private FlightService flightService;
    private Settings settings;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private SecureTransportSettingsProvider secureTransportSettingsProvider;
    private AtomicInteger port = new AtomicInteger(0);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        int availablePort = getBasePort() + port.addAndGet(1);
        settings = Settings.builder().put("node.attr.transport.stream.port", String.valueOf(availablePort)).build();

        clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.nodes()).thenReturn(nodes);
        when(nodes.getLocalNodeId()).thenReturn("test-node");

        threadPool = mock(ThreadPool.class);
        secureTransportSettingsProvider = mock(SecureTransportSettingsProvider.class);

        flightService = new FlightService(settings);
    }

    public void testInitializeWithSslEnabled() throws Exception {
        int testPort = getBasePort() + port.addAndGet(1);

        // Configure SSL enabled
        Settings sslSettings = Settings.builder()
            .put("node.attr.transport.stream.port", String.valueOf(testPort))
            .put("arrow.ssl.enable", true)
            .build();

        try (FlightService sslService = new FlightService(sslSettings)) {
            sslService.setSecureTransportSettingsProvider(secureTransportSettingsProvider);
            sslService.initialize(clusterService, threadPool);

            SslContextProvider sslContextProvider = sslService.getSslContextProvider();
            assertNotNull("SSL context provider should not be null", sslContextProvider);
            assertTrue("SSL context provider should be DefaultSslContextProvider", sslContextProvider instanceof DefaultSslContextProvider);
            assertTrue("SSL should be enabled", sslContextProvider.isSslEnabled());
        }
    }

    public void testInitializeWithSslDisabled() throws Exception {
        int testPort = getBasePort() + port.addAndGet(1);

        Settings noSslSettings = Settings.builder()
            .put("node.attr.transport.stream.port", String.valueOf(testPort))
            .put("arrow.ssl.enable", false)
            .build();

        try (FlightService noSslService = new FlightService(noSslSettings)) {
            noSslService.initialize(clusterService, threadPool);

            // Verify SSL is properly disabled
            SslContextProvider sslContextProvider = noSslService.getSslContextProvider();
            assertNotNull("SSL context provider should not be null", sslContextProvider);
            assertTrue(
                "SSL context provider should be DisabledSslContextProvider",
                sslContextProvider instanceof DisabledSslContextProvider
            );
            assertFalse("SSL should be disabled", sslContextProvider.isSslEnabled());
        }
    }

    public void testStartAndStop() throws Exception {
        int testPort = getBasePort() + port.addAndGet(1);

        Settings testSettings = Settings.builder().put("node.attr.transport.stream.port", String.valueOf(testPort)).build();

        try (FlightService testService = new FlightService(testSettings)) {
            testService.initialize(clusterService, threadPool);

            testService.start();

            verifyServerRunning(testService, testPort);

            testService.stop();

            testService.start();
            assertNotNull(testService.getStreamManager());
        }
    }

    public void testClose() throws Exception {
        flightService.initialize(clusterService, threadPool);
        flightService.start();
        flightService.close();

        expectThrows(IllegalStateException.class, () -> { flightService.start(); });
    }

    public void testInitializeWithoutSecureTransportSettingsProvider() {
        Settings sslSettings = Settings.builder().put(settings).put("arrow.ssl.enable", true).build();

        FlightService sslService = new FlightService(sslSettings);

        // Should throw exception when initializing without provider
        expectThrows(NullPointerException.class, () -> {
            sslService.initialize(clusterService, threadPool);
            sslService.start();
        });
    }

    public void testStopWithoutStart() {
        flightService.initialize(clusterService, threadPool);

        flightService.stop();
    }

    public void testCloseWithoutStart() {
        flightService.initialize(clusterService, threadPool);
        flightService.close();
    }

    public void testServerStartupFailure() {
        Settings invalidSettings = Settings.builder()
            .put("node.attr.transport.stream.port", "-1") // Invalid port
            .build();
        expectThrows(RuntimeException.class, () -> { FlightService invalidService = new FlightService(invalidSettings); });
    }

    public void testLifecycleStateTransitions() throws Exception {
        // Find new port for this test
        int testPort = getBasePort() + port.addAndGet(1);

        Settings testSettings = Settings.builder().put("node.attr.transport.stream.port", String.valueOf(testPort)).build();

        FlightService testService = new FlightService(testSettings);
        testService.initialize(clusterService, threadPool);

        // Test all state transitions
        testService.start();
        assertEquals("STARTED", testService.lifecycleState().toString());
        verifyServerRunning(testService, testPort);

        testService.stop();
        assertEquals("STOPPED", testService.lifecycleState().toString());

        testService.close();
        assertEquals("CLOSED", testService.lifecycleState().toString());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void verifyServerRunning(FlightService flightService, int clientPort) throws InterruptedException {
        FlightClientBuilder builder = new FlightClientBuilder(
            "localhost",
            clientPort,
            flightService.getAllocator(),
            flightService.getSslContextProvider()
        );
        try (OpenSearchFlightClient client = builder.build()) {
            // If we can connect, server is running
            assertNotNull("Should be able to connect to server", client.doAction(new Action("ping")));
        }
    }
}
