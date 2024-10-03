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


public class BaseFlightProducer extends NoOpFlightProducer {
    private final StreamManager streamManager;
    private final BufferAllocator allocator;

    public BaseFlightProducer(StreamManager streamManager, BufferAllocator allocator) {
        this.streamManager = streamManager;
        this.allocator = allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        StreamTicket streamTicket = new StreamTicket(ticket.getBytes()) {};
        try {
            ArrowStreamProvider provider = streamManager.getStream(streamTicket);
            if (provider == null) {
                listener.error(CallStatus.NOT_FOUND.withDescription("Stream not found").toRuntimeException());
                return;
            }
            BackpressureStrategy backpressureStrategy = new BackpressureStrategy.CallbackBackpressureStrategy();
            backpressureStrategy.register(listener);
            ArrowStreamProvider.Task task = provider.create(allocator);
            VectorSchemaRoot root = task.init(allocator);
            listener.start(root);
            ArrowStreamProvider.FlushSignal flushSignal = () -> {
                BackpressureStrategy.WaitResult result = backpressureStrategy.waitForListener(1000);
                if (result.equals(BackpressureStrategy.WaitResult.READY)) {
                    listener.putNext();
                } else if (result.equals(BackpressureStrategy.WaitResult.TIMEOUT)) {
                    listener.error(CallStatus.TIMED_OUT.cause());
                    throw new RuntimeException("Timeout waiting for listener" + result);
                } else {
                    listener.error(CallStatus.INTERNAL.toRuntimeException());
                    throw new RuntimeException("Error while waiting for client: " + result);
                }
            };
            try {
                task.run(root, flushSignal);
            } finally {
                root.close();
            }
        } catch (Exception e) {
            listener.error(CallStatus.INTERNAL.toRuntimeException().initCause(e));
            throw e;
        } finally {
            listener.completed();
            streamManager.removeStream(streamTicket);
        }
    }
}
