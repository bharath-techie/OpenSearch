/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.client;

import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.OpenSearchFlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Manages Flight client connections to OpenSearch nodes in a cluster.
 * This class maintains a pool of Flight clients for internode communication,
 * handles client lifecycle, and responds to cluster state changes.
 *
 * <p>The manager implements ClusterStateListener to automatically update
 * client connections when nodes join or leave the cluster. </p>
 */
public class FlightClientManager implements ClusterStateListener, AutoCloseable {
    private final Map<String, FlightClientHolder> flightClients;
    private final ClusterService clusterService;
    private final Supplier<BufferAllocator> allocator;
    private final SslContextProvider sslContextProvider;

    /**
     * Creates a new FlightClientManager instance.
     *
     * @param allocator Supplier for buffer allocation
     * @param clusterService Service for cluster state management
     * @param sslContextProvider Provider for SSL/TLS context configuration
     */
    public FlightClientManager(Supplier<BufferAllocator> allocator, ClusterService clusterService, SslContextProvider sslContextProvider) {
        this.allocator = allocator;
        this.clusterService = clusterService;
        this.flightClients = new ConcurrentHashMap<>();
        this.sslContextProvider = sslContextProvider;
        clusterService.addListener(this);
    }

    /**
     * Initializes the Flight clients for the current cluster state.
     * @param event The ClusterChangedEvent containing the current cluster state
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesChanged()) {
            updateFlightClients();
        }
    }

    /**
     * Returns a Flight client for a given node ID.
     * @param nodeId The ID of the node for which to retrieve the Flight client
     * @return An OpenSearchFlightClient instance for the specified node
     */
    public OpenSearchFlightClient getFlightClient(String nodeId) {
        return flightClients.computeIfAbsent(nodeId, this::buildFlightClient).flightClient;
    }

    /**
     * Returns the location of a Flight client for a given node ID.
     * @param nodeId The ID of the node for which to retrieve the location
     * @return The Location of the Flight client for the specified node
     */
    public Location getFlightClientLocation(String nodeId) {
        return flightClients.computeIfAbsent(nodeId, this::buildFlightClient).location;
    }

    /**
     * Returns the ID of the local node in the cluster.
     *
     * @return String representing the local node ID
     */
    public String getLocalNodeId() {
        return Objects.requireNonNull(clusterService).state().nodes().getLocalNodeId();
    }

    /**
     * Closes the FlightClientManager and all associated Flight clients.
     */
    @Override
    public void close() throws Exception {
        for (FlightClientHolder clientHolder : flightClients.values()) {
            clientHolder.flightClient.close();
        }
        flightClients.clear();
    }

    private FlightClientHolder buildFlightClient(String nodeId) {
        DiscoveryNode node = Objects.requireNonNull(clusterService).state().nodes().get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Node with id " + nodeId + " not found in cluster");
        }
        // TODO: handle cases where flight server isn't running like mixed cluster with nodes of previous version
        // ideally streaming shouldn't be supported on mixed cluster.
        String clientPort = node.getAttributes().get("transport.stream.port");
        FlightClientBuilder builder = new FlightClientBuilder(
            node.getHostAddress(),
            Integer.parseInt(clientPort),
            allocator.get(),
            sslContextProvider
        );
        return new FlightClientHolder(builder.build(), builder.getLocation());
    }

    @VisibleForTesting
    void updateFlightClients() {
        Set<String> currentNodes = Objects.requireNonNull(clusterService).state().nodes().getNodes().keySet();
        flightClients.keySet().removeIf(nodeId -> !currentNodes.contains(nodeId));
        initializeFlightClients();
    }

    private void initializeFlightClients() {
        for (DiscoveryNode node : Objects.requireNonNull(clusterService).state().nodes()) {
            String nodeId = node.getId();
            if (!flightClients.containsKey(nodeId)) {
                getFlightClient(nodeId);
            }
        }
    }

    @VisibleForTesting
    Map<String, FlightClientHolder> getFlightClients() {
        return flightClients;
    }

    private static class FlightClientHolder {
        final OpenSearchFlightClient flightClient;
        final Location location;

        FlightClientHolder(OpenSearchFlightClient flightClient, Location location) {
            this.flightClient = flightClient;
            this.location = location;
        }
    }
}
