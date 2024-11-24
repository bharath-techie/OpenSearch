/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap;

import org.opensearch.arrow.flight.BaseFlightStreamPlugin;
import org.opensearch.arrow.flight.bootstrap.server.ServerConfig;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * FlightStreamPlugin class extends BaseFlightStreamPlugin and provides implementation for FlightStream plugin.
 */
public class FlightStreamPluginImpl extends BaseFlightStreamPlugin {

    private final FlightService flightService;

    /**
     * Constructor for FlightStreamPluginImpl.
     * @param settings The settings for the FlightStreamPlugin.
     */
    public FlightStreamPluginImpl(Settings settings) {
        this.flightService = new FlightService(settings);
    }

    /**
     * Creates components for the FlightStream plugin.
     * @param client The client instance.
     * @param clusterService The cluster service instance.
     * @param threadPool The thread pool instance.
     * @param resourceWatcherService The resource watcher service instance.
     * @param scriptService The script service instance.
     * @param xContentRegistry The named XContent registry.
     * @param environment The environment instance.
     * @param nodeEnvironment The node environment instance.
     * @param namedWriteableRegistry The named writeable registry.
     * @param indexNameExpressionResolver The index name expression resolver instance.
     * @param repositoriesServiceSupplier The supplier for the repositories service.
     * @return FlightService
     */
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
        flightService.initialize(clusterService, threadPool);
        return List.of(flightService);
    }

    /**
     * Gets the secure transports for the FlightStream plugin.
     * @param settings The settings for the plugin.
     * @param threadPool The thread pool instance.
     * @param pageCacheRecycler The page cache recycler instance.
     * @param circuitBreakerService The circuit breaker service instance.
     * @param namedWriteableRegistry The named writeable registry.
     * @param networkService The network service instance.
     * @param secureTransportSettingsProvider The secure transport settings provider.
     * @param tracer The tracer instance.
     * @return A map of secure transports.
     */
    @Override
    public Map<String, Supplier<Transport>> getSecureTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        SecureTransportSettingsProvider secureTransportSettingsProvider,
        Tracer tracer
    ) {
        flightService.setSecureTransportSettingsProvider(secureTransportSettingsProvider);
        return Collections.emptyMap();
    }

    /**
     * Gets the StreamManager instance for managing flight streams.
     */
    @Override
    public StreamManager getStreamManager() {
        return flightService.getStreamManager();
    }

    /**
     * Gets the list of ExecutorBuilder instances for building thread pools used for FlightServer.
     * @param settings The settings for the plugin
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.singletonList(ServerConfig.getExecutorBuilder());
    }

    /**
     * Gets the list of settings for the Flight plugin.
     */
    @Override
    public List<Setting<?>> getSettings() {
        return ServerConfig.getSettings();
    }
}
