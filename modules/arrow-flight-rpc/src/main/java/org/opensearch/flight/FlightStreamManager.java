/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flight;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.StreamIterator;
import org.opensearch.arrow.StreamManager;
import org.opensearch.arrow.StreamProducer;
import org.opensearch.arrow.StreamTicket;
import org.opensearch.common.SetOnce;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * FlightStreamManager is a concrete implementation of StreamManager that provides
 * an abstraction layer for managing Arrow Flight streams in OpenSearch.
 * It encapsulates the details of Flight client operations, allowing consumers to
 * work with streams without direct exposure to Flight internals.
 */
public class FlightStreamManager implements StreamManager {

    private final FlightService flightService;
    private final Supplier<BufferAllocator> allocatorSupplier;
    private final Cache<String, StreamProducerHolder> streamProducers;
    private static final TimeValue expireAfter = TimeValue.timeValueMinutes(2);
    private static final long MAX_PRODUCERS = 10000;

    /**
     * Constructs a new FlightStreamManager.
     * @param flightService The FlightService instance to use for Flight client operations.
     */
    public FlightStreamManager(Supplier<BufferAllocator> allocatorSupplier, FlightService flightService) {
        this.allocatorSupplier = allocatorSupplier;
        this.flightService = flightService;
        this.streamProducers = CacheBuilder.<String, StreamProducerHolder>builder()
            .setExpireAfterWrite(expireAfter)
            .setMaximumWeight(MAX_PRODUCERS)
            .build();
    }

    @Override
    public StreamTicket registerStream(StreamProducer provider, TaskId parentTaskId) {
        String ticket = generateUniqueTicket();
        streamProducers.put(ticket, new StreamProducerHolder(provider, allocatorSupplier.get()));
        return new StreamTicket(ticket, getLocalNodeId());
    }

    /**
     * Retrieves a StreamIterator for the given StreamTicket.
     * @param ticket The StreamTicket representing the stream to retrieve.
     * @return A StreamIterator instance for the specified stream.
     */
    @Override
    public StreamIterator getStreamIterator(StreamTicket ticket) {
        FlightStream stream = flightService.getFlightClient(ticket.getNodeID()).getStream(new Ticket(ticket.toBytes()));
        return new FlightStreamIterator(stream);
    }

    @Override
    public String generateUniqueTicket() {
        return UUID.randomUUID().toString();
    }

    @Override
    public String getLocalNodeId() {
        return flightService.getLocalNodeId();
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
     * Removes the stream associated with the given StreamTicket.
     *
     * @param ticket The StreamTicket of the stream to remove.
     */
    public void removeStreamProvider(StreamTicket ticket) {
        streamProducers.invalidate(ticket.getTicketID());
    }

    /**
     * Closes the StreamManager and cancels all associated streams.
     * This method should be called when the StreamManager is no longer needed to clean up resources.
     * It is recommended to implement this method to cancel all threads and clear the streamManager queue.
     */
    public void close() {
        // TODO: logic to cancel all threads and clear the streamManager queue
        streamProducers.invalidateAll();
    }

    /**
     * Holds a StreamProducer and its associated VectorSchemaRoot.
     */
    public static class StreamProducerHolder {
        private final StreamProducer producer;
        private final SetOnce<VectorSchemaRoot> root;
        private final BufferAllocator allocator;

        public StreamProducerHolder(StreamProducer producer, BufferAllocator allocator) {
            this.producer = producer;
            this.allocator = allocator;
            this.root = new SetOnce<>();
        }

        public StreamProducer getProducer() {
            return producer;
        }

        public VectorSchemaRoot getRoot() {
            root.trySet(producer.createRoot(allocator));
            return root.get();
        }
    }
}
