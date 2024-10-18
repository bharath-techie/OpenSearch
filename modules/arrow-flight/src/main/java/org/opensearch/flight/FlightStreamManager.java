/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flight;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.StreamManager;
import org.opensearch.arrow.StreamTicket;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * FlightStreamManager is a concrete implementation of StreamManager that provides
 * an abstraction layer for managing Arrow Flight streams in OpenSearch.
 * It encapsulates the details of Flight client operations, allowing consumers to
 * work with streams without direct exposure to Flight internals.
 */
public class FlightStreamManager extends StreamManager {

    private final FlightService flightService;

    /**
     * Constructs a new FlightStreamManager.
     * @param flightService The FlightService instance to use for Flight client operations.
     */
    public FlightStreamManager(Supplier<BufferAllocator> allocatorSupplier, FlightService flightService) {
        super(allocatorSupplier);
        this.flightService = flightService;
    }

    /**
     * Retrieves a VectorSchemaRoot for a given stream ticket.
     * @param ticket The StreamTicket identifying the desired stream.
     * @return The VectorSchemaRoot associated with the given ticket.
     */
    @Override
    public VectorSchemaRoot getVectorSchemaRoot(StreamTicket ticket) {
        FlightStream stream = flightService.getFlightClient(ticket.getNodeID()).getStream(new Ticket(ticket.toBytes()));
        return stream.getRoot();
    }

    @Override
    public String generateUniqueTicket() {
        return UUID.randomUUID().toString();
    }

    @Override
    public String getNodeId() {
        return flightService.getLocalNodeId();
    }
}
