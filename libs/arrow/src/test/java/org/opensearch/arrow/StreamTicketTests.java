/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Base64;

public class StreamTicketTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        String ticketID = "ticket123";
        String nodeID = "node456";
        StreamTicket ticket = new StreamTicket(ticketID, nodeID);

        assertEquals(ticketID, ticket.getTicketID());
        assertEquals(nodeID, ticket.getNodeID());
    }

    public void testToBytes() {
        StreamTicket ticket = new StreamTicket("ticket123", "node456");
        byte[] bytes = ticket.toBytes();

        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        // Decode the Base64 and check the structure
        byte[] decoded = Base64.getDecoder().decode(bytes);
        assertEquals(2 + 9 + 2 + 7, decoded.length); // 2 shorts + "ticket123" + "node456"
    }

    public void testFromBytes() {
        StreamTicket original = new StreamTicket("ticket123", "node456");
        byte[] bytes = original.toBytes();

        StreamTicket reconstructed = StreamTicket.fromBytes(bytes);

        assertEquals(original.getTicketID(), reconstructed.getTicketID());
        assertEquals(original.getNodeID(), reconstructed.getNodeID());
    }

    public void testToBytesWithLongStrings() {
        String longString = randomAlphaOfLength(Short.MAX_VALUE + 1);
        StreamTicket ticket = new StreamTicket(longString, "node456");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, ticket::toBytes);
        assertEquals("Field lengths exceed the maximum allowed size.", exception.getMessage());
    }

    public void testFromBytesWithInvalidInput() {
        // TODO
    }

    public void testEquals() {
        StreamTicket ticket1 = new StreamTicket("ticket123", "node456");
        StreamTicket ticket2 = new StreamTicket("ticket123", "node456");
        StreamTicket ticket3 = new StreamTicket("ticket789", "node456");

        assertEquals(ticket1, ticket2);
        assertNotEquals(ticket1, ticket3);
        assertNotEquals(null, ticket1);
        assertNotEquals("Not a StreamTicket", ticket1);
    }

    public void testHashCode() {
        StreamTicket ticket1 = new StreamTicket("ticket123", "node456");
        StreamTicket ticket2 = new StreamTicket("ticket123", "node456");

        assertEquals(ticket1.hashCode(), ticket2.hashCode());
    }

    public void testToString() {
        StreamTicket ticket = new StreamTicket("ticket123", "node456");
        String expected = "StreamTicket{ticketID='ticket123', nodeID='node456'}";
        assertEquals(expected, ticket.toString());
    }
}
