/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flight.core;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.StreamIterator;

/**
 * FlightStreamIterator is a wrapper class that adapts the FlightStream interface
 * to the StreamIterator interface.
 */
public class FlightStreamIterator implements StreamIterator {

    private final FlightStream flightStream;

    /**
     * Constructs a FlightStreamIterator with the given FlightStream.
     *
     * @param flightStream The FlightStream to be adapted.
     */
    public FlightStreamIterator(FlightStream flightStream) {
        this.flightStream = flightStream;
    }

    /**
     * Moves the flightStream to the next batch of data.
     * @return true if there is a next batch of data, false otherwise.
     */
    @Override
    public boolean next() {
        return flightStream.next();
    }

    /**
     * Returns the VectorSchemaRoot containing the current batch of data.
     * @return The VectorSchemaRoot containing the current batch of data.
     */
    @Override
    public VectorSchemaRoot getRoot() {
        return flightStream.getRoot();
    }

    /**
     * Closes the flightStream.
     */
    @Override
    public void close() {
        try {
            flightStream.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
