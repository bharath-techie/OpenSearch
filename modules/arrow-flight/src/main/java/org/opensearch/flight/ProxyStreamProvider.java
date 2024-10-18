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
import org.opensearch.arrow.StreamProvider;

public class ProxyStreamProvider implements StreamProvider {

    private final FlightStream remoteStream;

    ProxyStreamProvider(FlightStream remoteStream) {
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

    private static class ProxyBatchedJob implements BatchedJob {

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
