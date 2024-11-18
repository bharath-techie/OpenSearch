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
 * The class provides methods to serialize and deserialize the ticket information to and from bytes.
 */
public class StreamTicket {
    private final String ticketID;
    private final String nodeID;

    /**
     * Constructs a new StreamTicket with the specified ticket ID and node ID.
     *
     * @param ticketID the unique identifier for the ticket
     * @param nodeID the identifier of the node associated with this ticket
     */
    public StreamTicket(String ticketID, String nodeID) {
        this.ticketID = ticketID;
        this.nodeID = nodeID;
    }

    /**
     * Returns the ticket ID associated with this stream ticket.
     *
     * @return the ticket ID string
     */
    public String getTicketID() {
        return ticketID;
    }

    /**
     * Returns the node ID associated with this stream ticket.
     *
     * @return the node ID string
     */
    public String getNodeID() {
        return nodeID;
    }

    /**
     * Converts this StreamTicket into a byte array representation.
     * The byte array contains the lengths and contents of both ticketID and nodeID,
     * encoded in Base64 format. The format is:
     * [ticketID length (2 bytes)][ticketID bytes][nodeID length (2 bytes)][nodeID bytes]
     *
     * @return a Base64 encoded byte array containing the ticket information
     * @throws IllegalArgumentException if either ticketID or nodeID length exceeds Short.MAX_VALUE
     */
    public byte[] toBytes() {
        byte[] ticketIDBytes = ticketID.getBytes(StandardCharsets.UTF_8);
        byte[] nodeIDBytes = nodeID.getBytes(StandardCharsets.UTF_8);

        if (ticketIDBytes.length > Short.MAX_VALUE || nodeIDBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Field lengths exceed the maximum allowed size.");
        }
        ByteBuffer buffer = ByteBuffer.allocate(2 + ticketIDBytes.length + 2 + nodeIDBytes.length);
        buffer.putShort((short) ticketIDBytes.length);
        buffer.put(ticketIDBytes);
        buffer.putShort((short) nodeIDBytes.length);
        buffer.put(nodeIDBytes);
        return Base64.getEncoder().encode(buffer.array());
    }

    /**
     * Creates a StreamTicket instance from its byte array representation.
     * The byte array should be in the format created by {@link #toBytes()}.
     *
     * @param bytes the Base64 encoded byte array containing the ticket information
     * @return a new StreamTicket instance
     * @throws IllegalArgumentException if the input byte array is null, too short,
     *         or doesn't contain valid ticket information
     */
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

    /**
     * Returns a hash code value for this StreamTicket.
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {
        return Objects.hash(ticketID, nodeID);
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @param obj the reference object with which to compare
     * @return true if this object is the same as the obj argument; false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        StreamTicket that = (StreamTicket) obj;
        return Objects.equals(ticketID, that.ticketID) && Objects.equals(nodeID, that.nodeID);
    }

    /**
     * Returns a string representation of this StreamTicket.
     *
     * @return a string representation of this object
     */
    @Override
    public String toString() {
        return "StreamTicket{ticketID='" + ticketID + "', nodeID='" + nodeID + "'}";
    }
}
