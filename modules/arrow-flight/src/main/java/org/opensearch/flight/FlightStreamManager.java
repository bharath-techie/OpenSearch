/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flight;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.StreamManager;
import org.opensearch.arrow.StreamTicket;

import java.util.UUID;

/**
 * FlightStreamManager is a concrete implementation of StreamManager that provides
 * an abstraction layer for managing Arrow Flight streams in OpenSearch.
 * It encapsulates the details of Flight client operations, allowing consumers to
 * work with streams without direct exposure to Flight internals.
 */
public class FlightStreamManager extends StreamManager {

    private final FlightClient flightClient;

    /**
     * Constructs a new FlightStreamManager.
     *
     * @param flightClient The FlightClient instance used for stream operations.
     */
    public FlightStreamManager(FlightClient flightClient) {
        super();
        this.flightClient = flightClient;
    }

    /**
     * Retrieves a VectorSchemaRoot for a given stream ticket.
     * @param ticket The StreamTicket identifying the desired stream.
     * @return The VectorSchemaRoot associated with the given ticket.
     */
    @Override
    public VectorSchemaRoot getVectorSchemaRoot(StreamTicket ticket) {
        // TODO: for remote streams, register streams in cluster state with node details
        // maintain flightClient for all nodes in the cluster to serve the stream
        FlightStream stream = flightClient.getStream(new Ticket(ticket.getBytes()));
        return stream.getRoot();
    }

    @Override
    public StreamTicket generateUniqueTicket() {
        return new StreamTicket(UUID.randomUUID().toString().getBytes()) {};
    }
}
