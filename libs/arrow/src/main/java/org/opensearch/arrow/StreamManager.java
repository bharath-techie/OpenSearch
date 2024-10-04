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

@ExperimentalApi
public abstract class StreamManager implements AutoCloseable {
    private final ConcurrentHashMap<StreamTicket, ArrowStreamProvider> streams;

    public StreamManager() {
        this.streams = new ConcurrentHashMap<>();
    }

    public StreamTicket registerStream(ArrowStreamProvider factory) {
        StreamTicket ticket = generateUniqueTicket();
        streams.put(ticket, factory);
        return ticket;
    }

    public ArrowStreamProvider getStream(StreamTicket ticket) {
        return streams.get(ticket);
    }

    public abstract VectorSchemaRoot getVectorSchemaRoot(StreamTicket ticket);

    public void removeStream(StreamTicket ticket) {
        streams.remove(ticket);
    }

    public ConcurrentHashMap<StreamTicket, ArrowStreamProvider> getStreams() {
        return streams;
    }

    public abstract StreamTicket generateUniqueTicket();

    public void close() {
        // TODO: logic to cancel all threads and clear the streamManager queue
        streams.clear();
    }
}
