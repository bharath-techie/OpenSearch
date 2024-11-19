/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow;

import org.opensearch.common.annotation.ExperimentalApi;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * A ticket that uniquely identifies a stream. This ticket is created when a producer registers
 * a stream with {@link StreamManager} and can be used by consumers to retrieve the stream using
 * {@link StreamManager#getStreamIterator(StreamTicket)}.
 */
@ExperimentalApi
public class StreamTicket {
    private static final int MAX_TOTAL_SIZE = 4096;
    private static final int MAX_ID_LENGTH = 256;

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
     * Serializes this ticket into a Base64 encoded byte array that can be deserialized using
     * {@link #fromBytes(byte[])}.
     *
     * @return Base64 encoded byte array containing the ticket information
     */
    public byte[] toBytes() {
        byte[] ticketIDBytes = ticketID.getBytes(StandardCharsets.UTF_8);
        byte[] nodeIDBytes = nodeID.getBytes(StandardCharsets.UTF_8);

        if (ticketIDBytes.length > Short.MAX_VALUE || nodeIDBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Field lengths exceed the maximum allowed size.");
        }
        ByteBuffer buffer = ByteBuffer.allocate(2 + ticketIDBytes.length + 2 + nodeIDBytes.length);
        buffer.putShort((short) ticketIDBytes.length);
        buffer.putShort((short) nodeIDBytes.length);
        buffer.put(ticketIDBytes);
        buffer.put(nodeIDBytes);
        return Base64.getEncoder().encode(buffer.array());
    }

    /**
     * Creates a StreamTicket from its serialized byte representation. The byte array should be
     * a Base64 encoded string containing the ticketID and nodeID.
     *
     * @param bytes Base64 encoded byte array containing ticket information
     * @return a new StreamTicket instance
     */
    public static StreamTicket fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length < 4) {
            throw new IllegalArgumentException("Invalid byte array input.");
        }

        if (bytes.length > MAX_TOTAL_SIZE) {
            throw new IllegalArgumentException("Input exceeds maximum allowed size");
        }

        ByteBuffer buffer = ByteBuffer.wrap(Base64.getDecoder().decode(bytes));

        short ticketIDLength = buffer.getShort();
        if (ticketIDLength < 0 || ticketIDLength > MAX_ID_LENGTH) {
            throw new IllegalArgumentException("Invalid ticketID length: " + ticketIDLength);
        }

        short nodeIDLength = buffer.getShort();
        if (nodeIDLength < 0 || nodeIDLength > MAX_ID_LENGTH) {
            throw new IllegalArgumentException("Invalid nodeID length: " + nodeIDLength);
        }
        byte[] ticketIDBytes = new byte[ticketIDLength];
        if (buffer.remaining() < ticketIDLength) {
            throw new IllegalArgumentException("Malformed byte array. Not enough data for TicketId.");
        }
        buffer.get(ticketIDBytes);
        byte[] nodeIDBytes = new byte[nodeIDLength];
        if (buffer.remaining() < nodeIDLength) {
            throw new IllegalArgumentException("Malformed byte array. Not enough data for NodeId.");
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
