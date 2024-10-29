/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

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
import org.opensearch.core.tasks.TaskId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Abstract class for managing Arrow streams.
 * This class provides functionality for registering, retrieving, and removing streams.
 * It also manages the lifecycle of streams and their associated resources.
 */
@ExperimentalApi
public abstract class StreamManager implements AutoCloseable {
    private final ConcurrentHashMap<String, StreamProducerHolder> streamProducers;
    private final Supplier<BufferAllocator> allocatorSupplier;

    /**
     * Constructs a new StreamManager with an empty stream map.
     */
    public StreamManager(Supplier<BufferAllocator> allocatorSupplier) {
        this.allocatorSupplier = allocatorSupplier;
        this.streamProducers = new ConcurrentHashMap<>();
    }

    public Supplier<BufferAllocator> allocatorSupplier() {
        return allocatorSupplier;
    }

    /**
     * Registers a new stream with the given ArrowStreamProvider.
     *
     * @param provider The ArrowStreamProvider to register.
     * @return A new StreamTicket for the registered stream.
     */
    public StreamTicket registerStream(StreamProducer provider, TaskId parentTaskId) {
        String ticket = generateUniqueTicket();
        VectorSchemaRoot root = provider.createRoot(allocatorSupplier.get());
        streamProducers.put(ticket, new StreamProducerHolder(provider, root));
        return new StreamTicket(ticket, getLocalNodeId());
    }

    /**
     * Retrieves the ArrowStreamProvider associated with the given StreamTicket.
     *
     * @param ticket The StreamTicket of the desired stream.
     * @return The ArrowStreamProvider associated with the ticket, or null if not found.
     */
    public StreamProducerHolder getStreamProducer(StreamTicket ticket) {
        return streamProducers.get(ticket.getTicketID());
    }

    /**
     * Retrieves the StreamIterator for the given StreamTicket.
     *
     * @param ticket The StreamTicket of the desired stream.
     * @return The StreamIterator for the associated stream.
     */
    public abstract StreamIterator getStreamIterator(StreamTicket ticket);

    /**
     * Removes the stream associated with the given StreamTicket.
     *
     * @param ticket The StreamTicket of the stream to remove.
     */
    public void removeStreamProvider(StreamTicket ticket) {
        streamProducers.remove(ticket.getTicketID());
    }

    /**
     * Generates a unique StreamTicket.
     *
     * @return A new, unique StreamTicket.
     */
    public abstract String generateUniqueTicket();

    /**
     * Returns the ID of the local node.
     *
     * @return The ID of the local node.
     */
    public abstract String getLocalNodeId();

    /**
     * Closes the StreamManager and cancels all associated streams.
     * This method should be called when the StreamManager is no longer needed to clean up resources.
     * It is recommended to implement this method to cancel all threads and clear the streamManager queue.
     */
    public void close() {
        // TODO: logic to cancel all threads and clear the streamManager queue
        streamProducers.clear();
    }

    /**
     * Holds a StreamProducer and its associated VectorSchemaRoot.
     */
    public static class StreamProducerHolder {
        private final StreamProducer producer;
        private final VectorSchemaRoot root;

        public StreamProducerHolder(StreamProducer producer, VectorSchemaRoot root) {
            this.producer = producer;
            this.root = root;
        }

        public StreamProducer getProducer() {
            return producer;
        }

        public VectorSchemaRoot getRoot() {
            return root;
        }
    }
}
