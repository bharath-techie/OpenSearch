/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a.java
 * compatible open source license.
 */

package org.opensearch.flight;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.arrow.StreamIterator;
import org.opensearch.arrow.StreamTicket;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

import static org.mockito.Mockito.*;

public class FlightStreamManagerTests extends OpenSearchTestCase {

    private FlightClient flightClient;
    private FlightStreamManager flightStreamManager;
    private static final String NODE_ID = "testNodeId";
    private static final String TICKET_ID = "testTicketId";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        flightClient = mock(FlightClient.class);
        FlightService flightService = mock(FlightService.class);
        when(flightService.getFlightClient(NODE_ID)).thenReturn(flightClient);
        BufferAllocator allocator = mock(BufferAllocator.class);
        flightStreamManager = new FlightStreamManager(()->allocator, flightService);
    }

    public void testGetStreamIterator() {
        StreamTicket ticket = new StreamTicket(TICKET_ID, NODE_ID);
        FlightStream mockFlightStream = mock(FlightStream.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        when(flightClient.getStream(new Ticket(ticket.toBytes()))).thenReturn(mockFlightStream);
        when(mockFlightStream.getRoot()).thenReturn(mockRoot);
        when(mockRoot.getSchema()).thenReturn(new Schema(Collections.emptyList()));

        StreamIterator streamIterator = flightStreamManager.getStreamIterator(ticket);

        assertNotNull(streamIterator);
        assertNotNull(streamIterator.getRoot());
        assertEquals(new Schema(Collections.emptyList()), streamIterator.getRoot().getSchema());
        verify(flightClient).getStream(new Ticket(ticket.toBytes()));
    }

    public void testGenerateUniqueTicket() {
        String ticket = flightStreamManager.generateUniqueTicket();
        assertNotNull(ticket);
    }

    public void testGetVectorSchemaRootWithException() {
        StreamTicket ticket = new StreamTicket(TICKET_ID, NODE_ID);
        when(flightClient.getStream(new Ticket(ticket.toBytes()))).thenThrow(new RuntimeException("Test exception"));

        expectThrows(RuntimeException.class, () -> flightStreamManager.getStreamIterator(ticket));
    }
}
