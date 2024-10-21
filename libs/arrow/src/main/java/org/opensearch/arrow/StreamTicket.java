/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * Represents a ticket for identifying and managing Arrow streams.
 * This class encapsulates a byte array that serves as a unique identifier for a stream.
 * The byte array is constructed by concatenating the ticket ID and node ID strings.
 */
public class StreamTicket {
    private final String ticketID;
    private final String nodeID;

    public StreamTicket(String ticketID, String nodeID) {
        this.ticketID = ticketID;
        this.nodeID = nodeID;
    }

    public String getTicketID() {
        return ticketID;
    }

    public String getNodeID() {
        return nodeID;
    }

    public byte[] toBytes() {
        byte[] ticketIDBytes = ticketID.getBytes(StandardCharsets.UTF_8);
        byte[] nodeIDBytes = nodeID.getBytes(StandardCharsets.UTF_8);

        if (ticketIDBytes.length > Short.MAX_VALUE || nodeIDBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Field lengths exceed the maximum allowed size.");
        }

        ByteBuffer buffer = ByteBuffer.allocate(2 + ticketIDBytes.length + 2 + nodeIDBytes.length); // 2 bytes for length
        buffer.putShort((short) ticketIDBytes.length);
        buffer.put(ticketIDBytes);
        buffer.putShort((short) nodeIDBytes.length);
        buffer.put(nodeIDBytes);
        return Base64.getEncoder().encode(buffer.array());
    }

    public static StreamTicket fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length < 4) {
            throw new IllegalArgumentException("Invalid byte array input.");
        }
        ByteBuffer buffer = ByteBuffer.wrap(Base64.getDecoder().decode(bytes));

        short ticketIDLength = buffer.getShort();
        if (ticketIDLength < 0) {
            throw new IllegalArgumentException("Invalid ticketID length.");
        }

        byte[] ticketIDBytes = new byte[ticketIDLength];
        if (buffer.remaining() < ticketIDLength) {
            throw new IllegalArgumentException("Malformed byte array. Not enough data for ticketID.");
        }
        buffer.get(ticketIDBytes);

        short nodeIDLength = buffer.getShort();
        if (nodeIDLength < 0) {
            throw new IllegalArgumentException("Invalid nodeID length.");
        }

        byte[] nodeIDBytes = new byte[nodeIDLength];
        if (buffer.remaining() < nodeIDLength) {
            throw new IllegalArgumentException("Malformed byte array.");
        }
        buffer.get(nodeIDBytes);

        String ticketID = new String(ticketIDBytes, StandardCharsets.UTF_8);
        String nodeID = new String(nodeIDBytes, StandardCharsets.UTF_8);

        return new StreamTicket(ticketID, nodeID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ticketID, nodeID);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        StreamTicket that = (StreamTicket) obj;
        return Objects.equals(ticketID, that.ticketID) && Objects.equals(nodeID, that.nodeID);
    }

    @Override
    public String toString() {
        return "StreamTicket{ticketID='" + ticketID + "', nodeID='" + nodeID + "'}";
    }
}
