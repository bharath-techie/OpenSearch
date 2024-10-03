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

public class FlightStreamManager extends StreamManager {

    private final FlightClient flightClient;

    public FlightStreamManager(FlightClient flightClient) {
        super();
        this.flightClient = flightClient;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot(StreamTicket ticket) {
        // TODO: for remote streams
        FlightStream stream = flightClient.getStream(new Ticket(ticket.getBytes()));
        return stream.getRoot();
    }

    @Override
    public StreamTicket generateUniqueTicket() {
        return new StreamTicket(UUID.randomUUID().toString().getBytes()) {};
    }
}
