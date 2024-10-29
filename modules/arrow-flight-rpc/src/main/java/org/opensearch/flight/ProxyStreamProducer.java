/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flight;

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

    ProxyStreamProducer(FlightStream remoteStream) {
        this.remoteStream = remoteStream;
    }

    @Override
    public VectorSchemaRoot createRoot(BufferAllocator allocator) {
        return remoteStream.getRoot();
    }

    @Override
    public BatchedJob createJob(BufferAllocator allocator) {
        return new ProxyBatchedJob(remoteStream);
    }

    public static class ProxyBatchedJob implements BatchedJob {

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
    }
}
