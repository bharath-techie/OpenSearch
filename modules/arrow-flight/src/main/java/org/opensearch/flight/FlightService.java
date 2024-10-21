/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flight;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.StreamManager;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FlightService manages the Arrow Flight server and client for OpenSearch.
 * It handles the initialization, startup, and shutdown of the Flight server and client,
 * as well as managing the stream operations through a FlightStreamManager.
 */
@ExperimentalApi
public class FlightService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static FlightServer server;
    private static FlightClient client;
    private static BufferAllocator allocator;
    private static StreamManager streamManager;
    private static final Logger logger = LogManager.getLogger(FlightService.class);
    private static final String host = "localhost";
    private static int port;

    public static final Setting<Integer> STREAM_PORT = Setting.intSetting(
        "node.attr.transport.stream.port",
        8815,
        1024,
        65535,
        Property.NodeScope
    );
    public static final Setting<String> ARROW_ALLOCATION_MANAGER_TYPE = Setting.simpleString(
        "arrow.allocation.manager.type",
        "Netty",
        Property.NodeScope
    );

    public static final Setting<Boolean> ARROW_ENABLE_NULL_CHECK_FOR_GET = Setting.boolSetting(
        "arrow.enable_null_check_for_get",
        false,
        Property.NodeScope
    );

    public static final Setting<Boolean> NETTY_TRY_REFLECTION_SET_ACCESSIBLE = Setting.boolSetting(
        "io.netty.tryReflectionSetAccessible",
        true,
        Property.NodeScope
    );

    public static final Setting<Boolean> ARROW_ENABLE_UNSAFE_MEMORY_ACCESS = Setting.boolSetting(
        "arrow.enable_unsafe_memory_access",
        true,
        Property.NodeScope
    );

    public static final Setting<Integer> NETTY_ALLOCATOR_NUM_DIRECT_ARENAS = Setting.intSetting(
        "io.netty.allocator.numDirectArenas",
        1, // TODO - 2 * the number of available processors
        1,
        Property.NodeScope
    );

    public static final Setting<Boolean> NETTY_NO_UNSAFE = Setting.boolSetting("io.netty.noUnsafe", false, Setting.Property.NodeScope);

    public static final Setting<Boolean> NETTY_TRY_UNSAFE = Setting.boolSetting("io.netty.tryUnsafe", true, Property.NodeScope);

    private final Map<String, FlightClientHolder> flightClients;

    private final SetOnce<ClusterService> clusterService = new SetOnce<>();

    FlightService(Settings settings) {
        System.setProperty("arrow.allocation.manager.type", ARROW_ALLOCATION_MANAGER_TYPE.get(settings));
        System.setProperty("arrow.enable_null_check_for_get", Boolean.toString(ARROW_ENABLE_NULL_CHECK_FOR_GET.get(settings)));
        System.setProperty("io.netty.tryReflectionSetAccessible", Boolean.toString(NETTY_TRY_REFLECTION_SET_ACCESSIBLE.get(settings)));
        System.setProperty("arrow.enable_unsafe_memory_access", Boolean.toString(ARROW_ENABLE_UNSAFE_MEMORY_ACCESS.get(settings)));
        System.setProperty("io.netty.allocator.numDirectArenas", Integer.toString(NETTY_ALLOCATOR_NUM_DIRECT_ARENAS.get(settings)));
        System.setProperty("io.netty.noUnsafe", Boolean.toString(NETTY_NO_UNSAFE.get(settings)));
        System.setProperty("io.netty.tryUnsafe", Boolean.toString(NETTY_TRY_UNSAFE.get(settings)));
        this.flightClients = new ConcurrentHashMap<>();
        port = STREAM_PORT.get(settings);
    }

    public void initialize(ClusterService clusterService) {
        this.clusterService.trySet(clusterService);
        clusterService.addListener(this);
        streamManager = new FlightStreamManager(this::getAllocator, this);
    }

    private BufferAllocator getAllocator() {
        return allocator;
    }

    @Override
    protected void doStart() {
        try {
            allocator = new RootAllocator(Integer.MAX_VALUE);
            BaseFlightProducer producer = new BaseFlightProducer(this, streamManager, allocator);
            final Location location = Location.forGrpcInsecure(host, port);
            server = FlightServer.builder(allocator, location, producer).build();
            client = FlightClient.builder(allocator, location).build();
            server.start();
            logger.info("Arrow Flight server started successfully:{}", location.getUri().toString());
        } catch (IOException e) {
            logger.error("Failed to start Arrow Flight server", e);
            throw new RuntimeException("Failed to start Arrow Flight server", e);
        }
    }

    @Override
    protected void doStop() {
        try {
            server.shutdown();
            streamManager.close();
            client.close();
            for (FlightClientHolder clientHolder : flightClients.values()) {
                clientHolder.flightClient.close();
            }
            server.close();
            logger.info("Arrow Flight service closed successfully");
        } catch (Exception e) {
            logger.error("Error while closing Arrow Flight service", e);
        }
    }

    @Override
    protected void doClose() {
        doStop();
        allocator.close();
    }

    public StreamManager getStreamManager() {
        return streamManager;
    }

    public FlightClient getFlightClient(String nodeId) {
        return flightClients.computeIfAbsent(nodeId, this::createFlightClient).flightClient;
    }

    public Location getFlightClientLocation(String nodeId) {
        return flightClients.computeIfAbsent(nodeId, this::createFlightClient).location;
    }

    private FlightClientHolder createFlightClient(String nodeId) {
        DiscoveryNode node = Objects.requireNonNull(clusterService.get()).state().nodes().get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Node with id " + nodeId + " not found in cluster");
        }
        String clientPort = node.getAttributes().get("transport.stream.port");
        Location location = Location.forGrpcInsecure(node.getHostAddress(), Integer.parseInt(clientPort));
        return new FlightClientHolder(FlightClient.builder(allocator, location).build(), location);
    }

    private void initializeFlightClients() {
        for (DiscoveryNode node : Objects.requireNonNull(clusterService.get()).state().nodes()) {
            String nodeId = node.getId();
            if (!flightClients.containsKey(nodeId)) {
                getFlightClient(nodeId);
            }
        }
    }

    public void updateFlightClients() {
        Set<String> currentNodes = Objects.requireNonNull(clusterService.get()).state().nodes().getNodes().keySet();
        flightClients.keySet().removeIf(nodeId -> !currentNodes.contains(nodeId));
        initializeFlightClients();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesChanged()) {
            updateFlightClients();
        }
    }

    public String getLocalNodeId() {
        return Objects.requireNonNull(clusterService.get()).state().nodes().getLocalNodeId();
    }

    private static class FlightClientHolder {
        final FlightClient flightClient;
        final Location location;

        FlightClientHolder(FlightClient flightClient, Location location) {
            this.flightClient = flightClient;
            this.location = location;
        }
    }
}
