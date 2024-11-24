/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.core;

import java.util.UUID;
import java.util.function.Supplier;

class StreamTicketFactory {
    private final Supplier<String> nodeId;

    public StreamTicketFactory(Supplier<String> nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Generates a new StreamTicket with a unique ticket ID.
     *
     * @return A new StreamTicket instance
     */
    public FlightStreamTicket createTicket() {
        return new FlightStreamTicket(generateUniqueTicket(), nodeId.get());
    }

    private String generateUniqueTicket() {
        return UUID.randomUUID().toString();
    }
}
