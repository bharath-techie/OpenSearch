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
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.StreamManager;
import org.opensearch.arrow.StreamProducer;
import org.opensearch.arrow.StreamTicket;

import java.util.Collections;

/**
 * BaseFlightProducer extends NoOpFlightProducer to provide stream management functionality
 * for Arrow Flight in OpenSearch. This class handles the retrieval and streaming of data
 * based on provided tickets, managing backpressure, and coordinating between the stream
 * provider and the server stream listener.
 */
public class BaseFlightProducer extends NoOpFlightProducer {
    private final FlightService flightService;
    private final StreamManager streamManager;
    private final BufferAllocator allocator;

    /**
     * Constructs a new BaseFlightProducer.
     *
     * @param streamManager The StreamManager to handle stream operations, including
     *                      retrieving and removing streams based on tickets.
     * @param allocator The BufferAllocator for memory management in Arrow operations.
     */
    public BaseFlightProducer(FlightService flightService, StreamManager streamManager, BufferAllocator allocator) {
        this.flightService = flightService;
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
        StreamTicket streamTicket = StreamTicket.fromBytes(ticket.getBytes());
        try {
            StreamManager.StreamHolder streamHolder;
            if (streamTicket.getNodeID().equals(flightService.getLocalNodeId())) {
                streamHolder = streamManager.getStreamProvider(streamTicket);
            } else {
                FlightClient remoteClient = flightService.getFlightClient(streamTicket.getNodeID());
                StreamProducer proxyProvider = new ProxyStreamProducer(remoteClient.getStream(ticket));
                VectorSchemaRoot remoteRoot = proxyProvider.createRoot(allocator);
                streamHolder = new StreamManager.StreamHolder(proxyProvider, remoteRoot);
            }
            if (streamHolder == null) {
                listener.error(CallStatus.NOT_FOUND.withDescription("Stream not found").toRuntimeException());
                return;
            }
            StreamProducer.BatchedJob batchedJob = streamHolder.getProvider().createJob(allocator);
            if (context.isCancelled()) {
                batchedJob.onCancel();
                listener.error(CallStatus.CANCELLED.cause());
                return;
            }
            listener.setOnCancelHandler(batchedJob::onCancel);
            BackpressureStrategy backpressureStrategy = new BaseBackpressureStrategy(null, batchedJob::onCancel);
            backpressureStrategy.register(listener);
            StreamProducer.FlushSignal flushSignal = (timeout) -> {
                BackpressureStrategy.WaitResult result = backpressureStrategy.waitForListener(timeout);
                if (result.equals(BackpressureStrategy.WaitResult.READY)) {
                    listener.putNext();
                } else if (result.equals(BackpressureStrategy.WaitResult.TIMEOUT)) {
                    listener.error(CallStatus.TIMED_OUT.cause());
                    throw new RuntimeException("Stream deadline exceeded for consumption");
                } else if (result.equals(BackpressureStrategy.WaitResult.CANCELLED)) {
                    batchedJob.onCancel();
                    listener.error(CallStatus.CANCELLED.cause());
                    throw new RuntimeException("Stream cancelled by client");
                } else {
                    listener.error(CallStatus.INTERNAL.toRuntimeException());
                    throw new RuntimeException("Error while waiting for client: " + result);
                }
            };
            try (VectorSchemaRoot root = streamHolder.getRoot()) {
                listener.start(root);
                batchedJob.run(root, flushSignal);
            }
        } catch (Exception e) {
            listener.error(CallStatus.INTERNAL.withDescription(e.getMessage()).withCause(e).cause());
            throw e;
        } finally {
            listener.completed();
            streamManager.removeStreamProvider(streamTicket);
        }
    }

    /**
     * Retrieves FlightInfo for the given FlightDescriptor, handling both local and remote cases.
     *
     * @param context The call context
     * @param descriptor The FlightDescriptor containing stream information
     * @return FlightInfo for the requested stream
     */
    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        StreamTicket streamTicket = StreamTicket.fromBytes(descriptor.getCommand());
        StreamManager.StreamHolder streamHolder;
        if (streamTicket.getNodeID().equals(flightService.getLocalNodeId())) {
            streamHolder = streamManager.getStreamProvider(streamTicket);
            if (streamHolder == null) {
                throw CallStatus.NOT_FOUND.withDescription("FlightInfo not found").toRuntimeException();
            }
            Location location = flightService.getFlightClientLocation(streamTicket.getNodeID());
            FlightEndpoint endpoint = new FlightEndpoint(new Ticket(descriptor.getCommand()), location);
            FlightInfo.Builder infoBuilder = FlightInfo.builder(
                streamHolder.getRoot().getSchema(),
                descriptor,
                Collections.singletonList(endpoint)
            ).setRecords(streamHolder.getProvider().estimatedRowCount());
            return infoBuilder.build();
        } else {
            FlightClient remoteClient = flightService.getFlightClient(streamTicket.getNodeID());
            return remoteClient.getInfo(descriptor);
        }
    }
}
