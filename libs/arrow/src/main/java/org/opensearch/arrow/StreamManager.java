/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract class for managing Arrow streams.
 * This class provides functionality for registering, retrieving, and removing streams.
 * It also manages the lifecycle of streams and their associated resources.
 */
@ExperimentalApi
public abstract class StreamManager implements AutoCloseable {
    private final ConcurrentHashMap<StreamTicket, ArrowStreamProvider> streams;

    /**
     * Constructs a new StreamManager with an empty stream map.
     */
    public StreamManager() {
        this.streams = new ConcurrentHashMap<>();
    }

    /**
     * Registers a new stream with the given ArrowStreamProvider.
     *
     * @param factory The ArrowStreamProvider to register.
     * @return A new StreamTicket for the registered stream.
     */
    public StreamTicket registerStream(ArrowStreamProvider factory) {
        StreamTicket ticket = generateUniqueTicket();
        streams.put(ticket, factory);
        return ticket;
    }

    /**
     * Retrieves the ArrowStreamProvider associated with the given StreamTicket.
     *
     * @param ticket The StreamTicket of the desired stream.
     * @return The ArrowStreamProvider associated with the ticket, or null if not found.
     */
    public ArrowStreamProvider getStream(StreamTicket ticket) {
        return streams.get(ticket);
    }

    /**
     * Retrieves the VectorSchemaRoot for the stream associated with the given StreamTicket.
     *
     * @param ticket The StreamTicket of the desired stream.
     * @return The VectorSchemaRoot for the associated stream.
     */
    public abstract VectorSchemaRoot getVectorSchemaRoot(StreamTicket ticket);

    /**
     * Removes the stream associated with the given StreamTicket.
     *
     * @param ticket The StreamTicket of the stream to remove.
     */
    public void removeStream(StreamTicket ticket) {
        streams.remove(ticket);
    }

    /**
     * Returns the map of all registered streams.
     *
     * @return A ConcurrentHashMap of all registered streams.
     */
    public ConcurrentHashMap<StreamTicket, ArrowStreamProvider> getStreams() {
        return streams;
    }

    /**
     * Generates a unique StreamTicket.
     *
     * @return A new, unique StreamTicket.
     */
    public abstract StreamTicket generateUniqueTicket();

    /**
     * Closes the StreamManager and cancels all associated streams.
     * This method should be called when the StreamManager is no longer needed to clean up resources.
     * It is recommended to implement this method to cancel all threads and clear the streamManager queue.
     */
    public void close() {
        // TODO: logic to cancel all threads and clear the streamManager queue
        streams.clear();
    }
}
