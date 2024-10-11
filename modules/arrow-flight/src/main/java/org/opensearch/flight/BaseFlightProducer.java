/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flight;

import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.ArrowStreamProvider;
import org.opensearch.arrow.StreamManager;
import org.opensearch.arrow.StreamTicket;

import java.util.function.Supplier;

/**
 * BaseFlightProducer extends NoOpFlightProducer to provide stream management functionality
 * for Arrow Flight in OpenSearch. This class handles the retrieval and streaming of data
 * based on provided tickets, managing backpressure, and coordinating between the stream
 * provider and the server stream listener.
 */
public class BaseFlightProducer extends NoOpFlightProducer {
    private final StreamManager streamManager;
    private final BufferAllocator allocator;

    /**
     * Constructs a new BaseFlightProducer.
     *
     * @param streamManager The StreamManager to handle stream operations, including
     *                      retrieving and removing streams based on tickets.
     * @param allocator The BufferAllocator for memory management in Arrow operations.
     */
    public BaseFlightProducer(StreamManager streamManager, BufferAllocator allocator) {
        this.streamManager = streamManager;
        this.allocator = allocator;
    }

    /**
     * Handles the retrieval and streaming of data based on the provided ticket.
     * This method orchestrates the entire process of setting up the stream,
     * managing backpressure, and handling data flow to the client.
     *
     * @param context The call context (unused in this implementation)
     * @param ticket The ticket containing stream information
     * @param listener The server stream listener to handle the data flow
     */
    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        StreamTicket streamTicket = new StreamTicket(ticket.getBytes()) {};
        try {
            ArrowStreamProvider provider = streamManager.getStream(streamTicket);
            if (provider == null) {
                listener.error(CallStatus.NOT_FOUND.withDescription("Stream not found").toRuntimeException());
                return;
            }
            ArrowStreamProvider.Task task = provider.create(allocator);
            if (context.isCancelled()) {
                task.onCancel();
                listener.error(CallStatus.CANCELLED.cause());
                return;
            }
            listener.setOnCancelHandler(task::onCancel);
            BackpressureStrategy backpressureStrategy = new BaseBackpressureStrategy(null, task::onCancel);
            backpressureStrategy.register(listener);
            ArrowStreamProvider.FlushSignal flushSignal = (timeout) -> {
                BackpressureStrategy.WaitResult result = backpressureStrategy.waitForListener(timeout);
                if (result.equals(BackpressureStrategy.WaitResult.READY)) {
                    listener.putNext();
                } else if (result.equals(BackpressureStrategy.WaitResult.TIMEOUT)) {
                    listener.error(CallStatus.TIMED_OUT.cause());
                    throw new RuntimeException("Stream deadline exceeded for consumption");
                } else if (result.equals(BackpressureStrategy.WaitResult.CANCELLED)) {
                    task.onCancel();
                    listener.error(CallStatus.CANCELLED.cause());
                    throw new RuntimeException("Stream cancelled by client");
                } else {
                    listener.error(CallStatus.INTERNAL.toRuntimeException());
                    throw new RuntimeException("Error while waiting for client: " + result);
                }
            };
            try(VectorSchemaRoot root = task.init(allocator)) {
                listener.start(root);
                task.run(root, flushSignal);
            }
        } catch (Exception e) {
            listener.error(CallStatus.INTERNAL.withDescription(e.getMessage()).withCause(e).cause());
            throw e;
        } finally {
            listener.completed();
            streamManager.removeStream(streamTicket);
        }
    }
}
