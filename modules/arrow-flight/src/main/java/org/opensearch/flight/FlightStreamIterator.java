/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flight;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.StreamIterator;

/**
 * FlightStreamIterator is a wrapper class that adapts the FlightStream interface
 * to the StreamIterator interface.
 */
public class FlightStreamIterator implements StreamIterator {

    private final FlightStream flightStream;

    public FlightStreamIterator(FlightStream flightStream) {
        this.flightStream = flightStream;
    }

    @Override
    public boolean next() {
        return flightStream.next();
    }

    @Override
    public VectorSchemaRoot getRoot() {
        return flightStream.getRoot();
    }

    @Override
    public void close() {
        try {
            flightStream.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
