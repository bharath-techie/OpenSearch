/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flight.core;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.StreamProducer;
import org.opensearch.arrow.StreamTicket;

/**
 * ProxyStreamProvider acts as forward proxy for FlightStream.
 * It creates a BatchedJob to handle the streaming of data from the remote FlightStream.
 * This is useful when stream is not present locally and needs to be fetched from a node
 * retrieved using {@link StreamTicket#getNodeID()} where it is present.
 */
public class ProxyStreamProducer implements StreamProducer {

    private final FlightStream remoteStream;

    /**
     * Constructs a new ProxyStreamProducer instance.
     *
     * @param remoteStream The remote FlightStream to be proxied.
     */
    public ProxyStreamProducer(FlightStream remoteStream) {
        this.remoteStream = remoteStream;
    }

    /**
     * Creates a VectorSchemaRoot for the remote FlightStream.
     * @param allocator The allocator to use for creating vectors
     * @return
     */
    @Override
    public VectorSchemaRoot createRoot(BufferAllocator allocator) {
        return remoteStream.getRoot();
    }

    /**
     * Creates a BatchedJob
     * @param allocator The allocator to use for any additional memory allocations
     */
    @Override
    public BatchedJob createJob(BufferAllocator allocator) {
        return new ProxyBatchedJob(remoteStream);
    }

    /**
     * Closes the remote FlightStream.
     */
    @Override
    public void close() {
        try {
            remoteStream.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class ProxyBatchedJob implements BatchedJob {

        private final FlightStream remoteStream;

        ProxyBatchedJob(FlightStream remoteStream) {
            this.remoteStream = remoteStream;
        }

        @Override
        public void run(VectorSchemaRoot root, FlushSignal flushSignal) {
            while (remoteStream.next()) {
                flushSignal.awaitConsumption(1000);
            }
            try {
                remoteStream.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onCancel() {
            try {
                remoteStream.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isCancelled() {
            // Proxy stream don't have any business logic to set this flag,
            // they piggyback on remote stream getting cancelled.
            return false;
        }
    }
}
