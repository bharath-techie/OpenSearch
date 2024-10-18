/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flight;

import org.opensearch.arrow.StreamManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.StreamManagerPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.flight.FlightService.ARROW_ALLOCATION_MANAGER_TYPE;
import static org.opensearch.flight.FlightService.ARROW_ENABLE_NULL_CHECK_FOR_GET;
import static org.opensearch.flight.FlightService.ARROW_ENABLE_UNSAFE_MEMORY_ACCESS;
import static org.opensearch.flight.FlightService.NETTY_ALLOCATOR_NUM_DIRECT_ARENAS;
import static org.opensearch.flight.FlightService.NETTY_NO_UNSAFE;
import static org.opensearch.flight.FlightService.NETTY_TRY_REFLECTION_SET_ACCESSIBLE;
import static org.opensearch.flight.FlightService.NETTY_TRY_UNSAFE;

public class FlightStreamPlugin extends Plugin implements StreamManagerPlugin {

    private final FlightService flightService;

    public FlightStreamPlugin(Settings settings) {
        this.flightService = new FlightService(settings);
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        flightService.initialize(clusterService);
        return List.of(flightService);
    }

    @Override
    public StreamManager getStreamManager() {
        return flightService.getStreamManager();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            ARROW_ALLOCATION_MANAGER_TYPE,
            ARROW_ENABLE_NULL_CHECK_FOR_GET,
            NETTY_TRY_REFLECTION_SET_ACCESSIBLE,
            ARROW_ENABLE_UNSAFE_MEMORY_ACCESS,
            NETTY_ALLOCATOR_NUM_DIRECT_ARENAS,
            NETTY_NO_UNSAFE,
            NETTY_TRY_UNSAFE
        );
    }
}
