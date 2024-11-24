/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flight.bootstrap.client;

import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.OpenSearchFlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.flight.bootstrap.server.ServerConfig;
import org.opensearch.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlightClientManagerTests extends OpenSearchTestCase {

    private static final int FLIGHT_PORT = 8815;
    private FlightClientManager clientManager;
    private ClusterService clusterService;
    private BufferAllocator allocator;
    private SslContextProvider sslContextProvider;
    private DiscoveryNode localNode;
    private DiscoveryNode remoteNode;
    private SslContext clientSslContext;
    private ClusterState clusterState;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ServerConfig.init(Settings.EMPTY);
        allocator = new RootAllocator();
        clusterService = mock(ClusterService.class);
        sslContextProvider = mock(SslContextProvider.class);

        // Create a proper gRPC client SSL context with ALPN and HTTP/2 support
        clientSslContext = GrpcSslContexts.configure(SslContextBuilder.forClient()).build();

        when(sslContextProvider.isSslEnabled()).thenReturn(true);
        when(sslContextProvider.getClientSslContext()).thenReturn(clientSslContext);

        localNode = createNode("local_node", "127.0.0.1", FLIGHT_PORT);
        remoteNode = createNode("remote_node", "127.0.0.2", FLIGHT_PORT);

        // Setup initial cluster state
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(remoteNode);
        nodesBuilder.localNodeId(localNode.getId());
        DiscoveryNodes nodes = nodesBuilder.build();

        clusterState = ClusterState.builder(new ClusterName("test")).nodes(nodes).build();

        when(clusterService.state()).thenReturn(clusterState);

        clientManager = new FlightClientManager(() -> allocator, clusterService, sslContextProvider);
    }

    private DiscoveryNode createNode(String nodeId, String host, int port) throws Exception {
        TransportAddress address = new TransportAddress(InetAddress.getByName(host), port);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("transport.stream.port", String.valueOf(port));
        Set<DiscoveryNodeRole> roles = Collections.singleton(DiscoveryNodeRole.DATA_ROLE);
        return new DiscoveryNode(nodeId, address, attributes, roles, Version.CURRENT);
    }

    @Override
    public void tearDown() throws Exception {
        clientManager.close();
        allocator.close();
        super.tearDown();
    }

    public void testGetFlightClientForExistingNode() throws Exception {
        OpenSearchFlightClient client = clientManager.getFlightClient(remoteNode.getId());
        assertNotNull("Flight client should be created for existing node", client);
    }

    public void testGetFlightClientLocation() throws Exception {
        Location location = clientManager.getFlightClientLocation(remoteNode.getId());
        assertNotNull("Flight client location should be returned", location);
        assertEquals("Location host should match", remoteNode.getHostAddress(), location.getUri().getHost());
        assertEquals("Location port should match", FLIGHT_PORT, location.getUri().getPort());
    }

    public void testGetFlightClientForNonExistentNode() {
        expectThrows(IllegalArgumentException.class, () -> { clientManager.getFlightClient("non_existent_node"); });
    }

    public void testUpdateFlightClientsWithNodesChanged() throws Exception {
        OpenSearchFlightClient initialClient = clientManager.getFlightClient(remoteNode.getId());
        assertNotNull(initialClient);

        DiscoveryNode newNode = createNode("new_node", "127.0.0.3", FLIGHT_PORT);

        // Update cluster state with new node
        DiscoveryNodes.Builder newNodesBuilder = DiscoveryNodes.builder();
        newNodesBuilder.add(remoteNode);
        newNodesBuilder.add(newNode);
        newNodesBuilder.localNodeId(localNode.getId());
        DiscoveryNodes newNodes = newNodesBuilder.build();

        ClusterState newState = ClusterState.builder(new ClusterName("test")).nodes(newNodes).build();

        when(clusterService.state()).thenReturn(newState);

        clientManager.updateFlightClients();

        // Verify both clients exist
        assertNotNull(clientManager.getFlightClient(remoteNode.getId()));
        assertNotNull(clientManager.getFlightClient(newNode.getId()));
    }

    public void testClusterChangedWithNodesChanged() throws Exception {
        DiscoveryNode newNode = createNode("new_node", "127.0.0.3", FLIGHT_PORT);

        // Update cluster state with new node
        DiscoveryNodes.Builder newNodesBuilder = DiscoveryNodes.builder();
        newNodesBuilder.add(remoteNode);
        newNodesBuilder.add(newNode);
        newNodesBuilder.localNodeId(localNode.getId());
        DiscoveryNodes newNodes = newNodesBuilder.build();

        ClusterState newState = ClusterState.builder(new ClusterName("test")).nodes(newNodes).build();

        when(clusterService.state()).thenReturn(newState);

        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, clusterState);
        clientManager.clusterChanged(event);

        // Verify both clients exist
        assertNotNull(clientManager.getFlightClient(remoteNode.getId()));
        assertNotNull(clientManager.getFlightClient(newNode.getId()));
    }

    public void testClusterChangedWithNoNodesChanged() throws Exception {
        ClusterChangedEvent event = new ClusterChangedEvent("test", clusterState, clusterState);
        clientManager.clusterChanged(event);

        // Verify original client still exists
        assertNotNull(clientManager.getFlightClient(remoteNode.getId()));
    }

    public void testGetLocalNodeId() {
        assertEquals("Local node ID should match", localNode.getId(), clientManager.getLocalNodeId());
    }

    public void testNodeWithoutStreamPort() throws Exception {
        // Create node without stream port
        DiscoveryNode invalidNode = new DiscoveryNode(
            "invalid_node",
            new TransportAddress(InetAddress.getByName("127.0.0.4"), FLIGHT_PORT),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(invalidNode);
        nodesBuilder.localNodeId(localNode.getId());
        DiscoveryNodes nodes = nodesBuilder.build();
        ClusterState invalidState = ClusterState.builder(new ClusterName("test")).nodes(nodes).build();

        when(clusterService.state()).thenReturn(invalidState);

        expectThrows(NumberFormatException.class, () -> { clientManager.getFlightClient(invalidNode.getId()); });
    }

    public void testCloseWithActiveClients() throws Exception {
        OpenSearchFlightClient client = clientManager.getFlightClient(remoteNode.getId());
        assertNotNull(client);

        clientManager.close();
        assertEquals(0, clientManager.getFlightClients().size());
    }

    public void testSslDisabled() throws Exception {
        when(sslContextProvider.isSslEnabled()).thenReturn(false);

        OpenSearchFlightClient client = clientManager.getFlightClient(remoteNode.getId());
        assertNotNull("Flight client should be created with SSL disabled", client);

        verify(sslContextProvider, never()).getClientSslContext();
    }
}
