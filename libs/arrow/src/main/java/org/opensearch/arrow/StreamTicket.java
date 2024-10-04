/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow;

import java.util.Arrays;

/**
 * Represents a ticket for identifying and managing Arrow streams.
 * This class encapsulates a byte array that serves as a unique identifier for a stream.
 */
public class StreamTicket {
    private final byte[] bytes;

    /**
     * Constructs a new StreamTicket with the given byte array.
     *
     * @param bytes The byte array to use as the ticket identifier.
     */
    public StreamTicket(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * Retrieves the byte array representing this ticket.
     *
     * @return The byte array identifier of this ticket.
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * Computes the hash code for this StreamTicket.
     *
     * @return The hash code value for this object.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bytes);
        return result;
    }

    /**
     * Compares this StreamTicket to the specified object for equality.
     *
     * @param obj The object to compare this StreamTicket against.
     * @return true if the given object represents a StreamTicket equivalent to this ticket, false otherwise.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof StreamTicket)) {
            return false;
        }
        StreamTicket other = (StreamTicket) obj;
        return Arrays.equals(bytes, other.getBytes());
    }
}
