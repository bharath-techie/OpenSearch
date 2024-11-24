/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flight.bootstrap.server;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.OpenSearchFlightServer;
import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.opensearch.flight.bootstrap.server.ServerConfig.FLIGHT_THREAD_POOL_NAME;

/**
 * Builder class for creating and configuring OpenSearch Flight server instances.
 * This class handles the setup of Arrow Flight servers with customizable thread pools,
 * buffer allocation, producer configuration, and SSL/TLS settings.
 */
public class FlightServerBuilder {
    private final ThreadPool threadPool;
    private final Supplier<BufferAllocator> allocator;
    private final FlightProducer producer;
    private final SslContextProvider sslContextProvider;

    /**
     * Creates a new FlightServerBuilder instance with the specified configurations.
     *
     * @param threadPool The thread pool used for handling Flight server operations
     * @param allocator Supplier for Arrow buffer allocation
     * @param producer The Flight producer that handles incoming requests
     * @param sslContextProvider Provider for SSL/TLS context configuration
     */
    public FlightServerBuilder(
        ThreadPool threadPool,
        Supplier<BufferAllocator> allocator,
        FlightProducer producer,
        SslContextProvider sslContextProvider
    ) {
        this.threadPool = threadPool;
        this.allocator = allocator;
        this.producer = producer;
        this.sslContextProvider = sslContextProvider;
    }

    /**
     * Builds and configures an OpenSearchFlightServer instance.
     * @return A configured OpenSearchFlightServer instance
     */
    public OpenSearchFlightServer build() throws IOException {
        final Location location = ServerConfig.getServerLocation();
        ExecutorService executorService = threadPool.executor(FLIGHT_THREAD_POOL_NAME);
        OpenSearchFlightServer.Builder builder = OpenSearchFlightServer.builder(allocator.get(), location, producer);
        builder.executor(executorService);
        if (sslContextProvider.isSslEnabled()) {
            builder.useTls(sslContextProvider.getServerSslContext());
        }
        return builder.build();
    }
}
