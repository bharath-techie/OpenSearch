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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.arrow.StreamManager;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;

import java.io.IOException;

@ExperimentalApi
public class FlightService extends AbstractLifecycleComponent {

    private static FlightServer server;
    private static FlightClient client;
    private static BufferAllocator allocator;
    private static StreamManager streamManager;
    private static final Logger logger = LogManager.getLogger(FlightService.class);
    private static String host;
    private static int port;

    public static final Setting<String> FLIGHT_HOST = Setting.simpleString(
            "opensearch.flight.host",
            "localhost",
            Property.NodeScope
    );

    public static final Setting<Integer> FLIGHT_PORT = Setting.intSetting(
            "opensearch.flight.port",
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
            1,
            1,
            Property.NodeScope
    );

    public static final Setting<Boolean> NETTY_NO_UNSAFE = Setting.boolSetting(
            "io.netty.noUnsafe",
            false,
            Setting.Property.NodeScope
    );

    public static final Setting<Boolean> NETTY_TRY_UNSAFE = Setting.boolSetting(
            "io.netty.tryUnsafe",
            true,
            Property.NodeScope
    );

    FlightService(Settings settings) {
        System.setProperty("arrow.allocation.manager.type", ARROW_ALLOCATION_MANAGER_TYPE.get(settings));
        System.setProperty("arrow.enable_null_check_for_get", Boolean.toString(ARROW_ENABLE_NULL_CHECK_FOR_GET.get(settings)));
        System.setProperty("io.netty.tryReflectionSetAccessible", Boolean.toString(NETTY_TRY_REFLECTION_SET_ACCESSIBLE.get(settings)));
        System.setProperty("arrow.enable_unsafe_memory_access", Boolean.toString(ARROW_ENABLE_UNSAFE_MEMORY_ACCESS.get(settings)));
        System.setProperty("io.netty.allocator.numDirectArenas", Integer.toString(NETTY_ALLOCATOR_NUM_DIRECT_ARENAS.get(settings)));
        System.setProperty("io.netty.noUnsafe", Boolean.toString(NETTY_NO_UNSAFE.get(settings)));
        System.setProperty("io.netty.tryUnsafe", Boolean.toString(NETTY_TRY_UNSAFE.get(settings)));
        host = FLIGHT_HOST.get(settings);
        port = FLIGHT_PORT.get(settings);
        streamManager = new FlightStreamManager(client);
    }

    @Override
    protected void doStart() {
        try {
            allocator = new RootAllocator(Integer.MAX_VALUE);
            BaseFlightProducer producer = new BaseFlightProducer(streamManager, allocator);
            final Location location = Location.forGrpcInsecure(host, port);
            server = FlightServer.builder(allocator, location, producer).build();
            client = FlightClient.builder(allocator, location).build();
            server.start();
            logger.info("Arrow Flight server started successfully");
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
            server.close();
            allocator.close();
            logger.info("Arrow Flight service closed successfully");
        } catch (Exception e) {
            logger.error("Error while closing Arrow Flight service", e);
        }
    }

    @Override
    protected void doClose() {
        doStop();
    }

    public StreamManager getStreamManager() {
        return streamManager;
    }
}
