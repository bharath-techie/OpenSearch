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
import org.opensearch.flight.bootstrap.tls.SslContextProvider;

/**
 * Builder class for creating OpenSearchFlightClient instances.
 * This class handles the configuration of Flight clients including SSL/TLS settings
 * and connection parameters.
 */
public class FlightClientBuilder {

    private final String address;
    private final int port;
    private final BufferAllocator allocator;
    private final SslContextProvider sslContextProvider;

    /**
     * Creates a new FlightClientBuilder instance.
     *
     * @param address The network address of the Flight server
     * @param port The port number of the Flight server
     * @param allocator The buffer allocator for Arrow memory management
     * @param sslContextProvider Provider for SSL/TLS context configuration
     */
    public FlightClientBuilder(String address, int port, BufferAllocator allocator, SslContextProvider sslContextProvider) {
        this.address = address;
        this.port = port;
        this.allocator = allocator;
        this.sslContextProvider = sslContextProvider;
    }

    Location getLocation() {
        if (sslContextProvider.isSslEnabled()) {
            return Location.forGrpcTls(address, port);
        }
        return Location.forGrpcInsecure(address, port);
    }

    /**
     * Builds and returns a configured OpenSearchFlightClient instance.
     *
     * @return A new OpenSearchFlightClient configured with the builder's settings
     */
    public OpenSearchFlightClient build() {
        OpenSearchFlightClient.Builder clientBuilder = OpenSearchFlightClient.builder(allocator, getLocation());
        if (sslContextProvider.isSslEnabled()) {
            clientBuilder.useTls();
            clientBuilder.sslContext(sslContextProvider.getClientSslContext());
        }
        return clientBuilder.build();
    }
}
