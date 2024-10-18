/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a.java
 * compatible open source license.
 */

package org.opensearch.flight;

import org.opensearch.test.OpenSearchTestCase;

public class FlightStreamManagerTests extends OpenSearchTestCase {
    /*
    private FlightClient flightClient;
    private FlightStreamManager flightStreamManager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        flightClient = mock(FlightClient.class);
        flightStreamManager = new FlightStreamManager(flightClient);
    }

    public void testGetVectorSchemaRoot() {
        StreamTicket ticket = new StreamTicket(new byte[]{1, 2, 3});
        FlightStream mockFlightStream = mock(FlightStream.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);

        when(flightClient.getStream(new Ticket(ticket.getBytes()))).thenReturn(mockFlightStream);
        when(mockFlightStream.getRoot()).thenReturn(mockRoot);
        when(mockRoot.getSchema()).thenReturn(new Schema(Collections.emptyList()));

        VectorSchemaRoot root = flightStreamManager.getVectorSchemaRoot(ticket);

        assertNotNull(root);
        assertEquals(new Schema(Collections.emptyList()), root.getSchema());
        verify(flightClient).getStream(new Ticket(ticket.getBytes()));
    }

    public void testGenerateUniqueTicket() {
        byte[] ticket = flightStreamManager.generateUniqueTicket();
        assertNotNull(ticket);
        assertNotNull(ticket.getBytes());
        assertTrue(ticket.getBytes().length > 0);
    }

    public void testGetVectorSchemaRootWithException() {
        StreamTicket ticket = new StreamTicket(new byte[]{1, 2, 3});
        when(flightClient.getStream(new Ticket(ticket.getBytes()))).thenThrow(new RuntimeException("Test exception"));

        expectThrows(RuntimeException.class, () -> flightStreamManager.getVectorSchemaRoot(ticket));
        verify(flightClient).getStream(new Ticket(ticket.getBytes()));
    }

    public void testGenerateUniqueTicketMultipleCalls() {
        StreamTicket ticket1 = flightStreamManager.generateUniqueTicket();
        StreamTicket ticket2 = flightStreamManager.generateUniqueTicket();

        assertNotNull(ticket1);
        assertNotNull(ticket2);
        assertNotEquals(ticket1, ticket2);
    }

    public void testGetVectorSchemaRootWithNullTicket() {
        expectThrows(NullPointerException.class, () -> flightStreamManager.getVectorSchemaRoot(null));
    }

     */
}
