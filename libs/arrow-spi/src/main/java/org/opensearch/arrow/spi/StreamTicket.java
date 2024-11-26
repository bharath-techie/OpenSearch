/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.spi;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * A ticket that uniquely identifies a stream. This ticket is created when a producer registers
 * a stream with {@link StreamManager} and can be used by consumers to retrieve the stream using
 * {@link StreamManager#getStreamReader(StreamTicket)}.
 */
@ExperimentalApi
public interface StreamTicket {
    /**
     * Returns the ticket ID associated with this stream ticket.
     *
     * @return the ticket ID string
     */
    String getTicketID();

    /**
     * Returns the node ID associated with this stream ticket.
     *
     * @return the node ID string
     */
    String getNodeID();

    /**
     * Serializes this ticket into a Base64 encoded byte array.
     *
     * @return Base64 encoded byte array containing the ticket information
     */
    byte[] toBytes();
}
