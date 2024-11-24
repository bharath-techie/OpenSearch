/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.server;

import org.apache.arrow.flight.Location;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Configuration class for OpenSearch Flight server settings.
 * This class manages server-side configurations including port settings, Arrow memory settings,
 * thread pool configurations, and SSL/TLS settings.
 */
public class ServerConfig {
    /**
     * Creates a new instance of the server configuration with default settings.
     */
    public ServerConfig() {}

    static final Setting<Integer> STREAM_PORT = Setting.intSetting(
        "node.attr.transport.stream.port",
        8815,
        1024,
        65535,
        Setting.Property.NodeScope
    );

    static final Setting<String> ARROW_ALLOCATION_MANAGER_TYPE = Setting.simpleString(
        "arrow.allocation.manager.type",
        "Netty",
        Setting.Property.NodeScope
    );

    static final Setting<Boolean> ARROW_ENABLE_NULL_CHECK_FOR_GET = Setting.boolSetting(
        "arrow.enable_null_check_for_get",
        false,
        Setting.Property.NodeScope
    );

    static final Setting<Boolean> ARROW_ENABLE_DEBUG_ALLOCATOR = Setting.boolSetting(
        "arrow.memory.debug.allocator",
        false,
        Setting.Property.NodeScope
    );

    static final Setting<Boolean> ARROW_ENABLE_UNSAFE_MEMORY_ACCESS = Setting.boolSetting(
        "arrow.enable_unsafe_memory_access",
        true,
        Setting.Property.NodeScope
    );

    static final Setting<Integer> FLIGHT_THREAD_POOL_MIN_SIZE = Setting.intSetting(
        "thread_pool.flight-server.min",
        0,
        0,
        Setting.Property.NodeScope
    );

    static final Setting<Integer> FLIGHT_THREAD_POOL_MAX_SIZE = Setting.intSetting(
        "thread_pool.flight-server.max",
        100000,
        1,
        Setting.Property.NodeScope
    );

    static final Setting<TimeValue> FLIGHT_THREAD_POOL_KEEP_ALIVE = Setting.timeSetting(
        "thread_pool.flight-server.keep_alive",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    static final Setting<Boolean> ARROW_SSL_ENABLE = Setting.boolSetting(
        "arrow.ssl.enable",
        false, // TODO: get default from security enabled
        Setting.Property.NodeScope
    );

    static final String FLIGHT_THREAD_POOL_NAME = "flight-server";

    private static final String host = "localhost";
    private static int port;
    private static ThreadPool threadPool;
    private static boolean enableSsl;
    private static ScalingExecutorBuilder executorBuilder;

    /**
     * Initializes the server configuration with the provided settings.
     * Sets system properties for Arrow memory management and configures thread pool settings.
     *
     * @param settings The OpenSearch settings to initialize the server with
     */
    @SuppressForbidden(reason = "required for arrow allocator")
    public static void init(Settings settings) {
        System.setProperty("arrow.allocation.manager.type", ARROW_ALLOCATION_MANAGER_TYPE.get(settings));
        System.setProperty("arrow.enable_null_check_for_get", Boolean.toString(ARROW_ENABLE_NULL_CHECK_FOR_GET.get(settings)));
        System.setProperty("arrow.enable_unsafe_memory_access", Boolean.toString(ARROW_ENABLE_UNSAFE_MEMORY_ACCESS.get(settings)));
        System.setProperty("arrow.memory.debug.allocator", Boolean.toString(ARROW_ENABLE_DEBUG_ALLOCATOR.get(settings)));
        Netty4Configs.init(settings);
        port = STREAM_PORT.get(settings);
        enableSsl = ARROW_SSL_ENABLE.get(settings);
        executorBuilder = new ScalingExecutorBuilder(
            FLIGHT_THREAD_POOL_NAME,
            FLIGHT_THREAD_POOL_MIN_SIZE.get(settings),
            FLIGHT_THREAD_POOL_MAX_SIZE.get(settings),
            FLIGHT_THREAD_POOL_KEEP_ALIVE.get(settings)
        );
    }

    /**
     * Checks if SSL/TLS is enabled for the Flight server.
     *
     * @return true if SSL is enabled, false otherwise
     */
    public static boolean isSslEnabled() {
        return enableSsl;
    }

    /**
     * Gets the thread pool executor builder configured for the Flight server.
     *
     * @return The configured ScalingExecutorBuilder instance
     */
    public static ScalingExecutorBuilder getExecutorBuilder() {
        return executorBuilder;
    }

    /**
     * Gets the server location configuration.
     *
     * @return Location instance configured for the server
     */
    public static Location getServerLocation() {
        return getLocation(host, port);
    }

    /**
     * Returns a list of all settings managed by this configuration class.
     *
     * @return List of Setting instances
     */
    public static List<Setting<?>> getSettings() {
        return new ArrayList<>(
            Arrays.asList(
                ARROW_ALLOCATION_MANAGER_TYPE,
                ARROW_ENABLE_NULL_CHECK_FOR_GET,
                ARROW_ENABLE_DEBUG_ALLOCATOR,
                ARROW_ENABLE_UNSAFE_MEMORY_ACCESS,
                ARROW_SSL_ENABLE
            )
        ) {
            {
                addAll(Netty4Configs.getSettings());
            }
        };
    }

    private static Location getLocation(String address, int port) {
        if (enableSsl) {
            return Location.forGrpcTls(address, port);
        }
        return Location.forGrpcInsecure(address, port);
    }

    private static class Netty4Configs {
        public static final Setting<Integer> NETTY_ALLOCATOR_NUM_DIRECT_ARENAS = Setting.intSetting(
            "io.netty.allocator.numDirectArenas",
            1, // TODO - 2 * the number of available processors
            1,
            Setting.Property.NodeScope
        );

        public static final Setting<Boolean> NETTY_TRY_REFLECTION_SET_ACCESSIBLE = Setting.boolSetting(
            "io.netty.tryReflectionSetAccessible",
            true,
            Setting.Property.NodeScope
        );

        public static final Setting<Boolean> NETTY_NO_UNSAFE = Setting.boolSetting("io.netty.noUnsafe", false, Setting.Property.NodeScope);

        public static final Setting<Boolean> NETTY_TRY_UNSAFE = Setting.boolSetting("io.netty.tryUnsafe", true, Setting.Property.NodeScope);

        @SuppressForbidden(reason = "required for netty allocator configuration")
        public static void init(Settings settings) {
            System.setProperty("io.netty.allocator.numDirectArenas", Integer.toString(NETTY_ALLOCATOR_NUM_DIRECT_ARENAS.get(settings)));
            System.setProperty("io.netty.noUnsafe", Boolean.toString(NETTY_NO_UNSAFE.get(settings)));
            System.setProperty("io.netty.tryUnsafe", Boolean.toString(NETTY_TRY_UNSAFE.get(settings)));
            System.setProperty("io.netty.tryReflectionSetAccessible", Boolean.toString(NETTY_TRY_REFLECTION_SET_ACCESSIBLE.get(settings)));
        }

        public static List<Setting<?>> getSettings() {
            return Arrays.asList(NETTY_TRY_REFLECTION_SET_ACCESSIBLE, NETTY_ALLOCATOR_NUM_DIRECT_ARENAS, NETTY_NO_UNSAFE, NETTY_TRY_UNSAFE);
        }
    }
}
