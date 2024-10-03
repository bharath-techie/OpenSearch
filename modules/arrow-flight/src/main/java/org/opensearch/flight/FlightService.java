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

import java.io.IOException;

@ExperimentalApi
public class FlightService extends AbstractLifecycleComponent {

    private static FlightServer server;
    private static FlightClient client;
    private static BufferAllocator allocator;
    private static StreamManager streamManager;
    private static final Logger logger = LogManager.getLogger(FlightService.class);

    FlightService() {
        streamManager = new FlightStreamManager(client);
    }

    @Override
    protected void doStart() {
        try {
            allocator = new RootAllocator(Integer.MAX_VALUE);
            BaseFlightProducer producer = new BaseFlightProducer(streamManager, allocator);
            // TODO: Load these settings from OpenSearch configuration
            String host = "localhost";
            int port = 8815;
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
