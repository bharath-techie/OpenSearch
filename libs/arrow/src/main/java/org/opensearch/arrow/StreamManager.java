/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Abstract class for managing Arrow streams.
 * This class provides functionality for registering, retrieving, and removing streams.
 * It also manages the lifecycle of streams and their associated resources.
 */
@ExperimentalApi
public abstract class StreamManager implements AutoCloseable {
    private final ConcurrentHashMap<String, StreamHolder> streamProviders;
    private final Supplier<BufferAllocator> allocatorSupplier;
    /**
     * Constructs a new StreamManager with an empty stream map.
     */
        public StreamManager(Supplier<BufferAllocator> allocatorSupplier) {
        this.allocatorSupplier = allocatorSupplier;
        this.streamProviders = new ConcurrentHashMap<>();
    }

    /**
     * Registers a new stream with the given ArrowStreamProvider.
     *
     * @param provider The ArrowStreamProvider to register.
     * @return A new StreamTicket for the registered stream.
     */
    public StreamTicket registerStream(ArrowStreamProvider provider) {
        String ticket = generateUniqueTicket();
        VectorSchemaRoot root = provider.create(allocatorSupplier.get()).init(allocatorSupplier.get());
        streamProviders.put(ticket, new StreamHolder(provider, root));
        return new StreamTicket(ticket, getNodeId());
    }

    /**
     * Retrieves the ArrowStreamProvider associated with the given StreamTicket.
     *
     * @param ticket The StreamTicket of the desired stream.
     * @return The ArrowStreamProvider associated with the ticket, or null if not found.
     */
    public StreamHolder getStreamProvider(StreamTicket ticket) {
        return streamProviders.get(ticket.getTicketID());
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
    public void removeStreamProvider(StreamTicket ticket) {
        streamProviders.remove(ticket.getTicketID());
    }

    /**
     * Returns the map of all registered streams.
     *
     * @return A ConcurrentHashMap of all registered streams.
     */
    public ConcurrentHashMap<String, StreamHolder> getStreamProviders() {
        return streamProviders;
    }

    /**
     * Generates a unique StreamTicket.
     *
     * @return A new, unique StreamTicket.
     */
    public abstract String generateUniqueTicket();

    public abstract String getNodeId();

    /**
     * Closes the StreamManager and cancels all associated streams.
     * This method should be called when the StreamManager is no longer needed to clean up resources.
     * It is recommended to implement this method to cancel all threads and clear the streamManager queue.
     */
    public void close() {
        // TODO: logic to cancel all threads and clear the streamManager queue
        streamProviders.clear();
    }

    public static class StreamHolder {
        private final ArrowStreamProvider provider;
        private final VectorSchemaRoot root;

        public StreamHolder(ArrowStreamProvider provider, VectorSchemaRoot root) {
            this.provider = provider;
            this.root = root;
        }

        public ArrowStreamProvider getProvider() {
            return provider;
        }

        public VectorSchemaRoot getRoot() {
            return root;
        }
    }
}
