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
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

import org.mockito.Mock;

import static org.mockito.Mockito.mock;

public class StreamManagerTests extends OpenSearchTestCase {

    /*
    private StreamManager streamManager;

    @Mock
    private ArrowStreamProvider mockProvider;

    private final VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        streamManager = new StreamManager() {
            @Override
            public VectorSchemaRoot getVectorSchemaRoot(StreamTicket ticket) {
                return mockRoot;
            }

            @Override
            public StreamTicket generateUniqueTicket() {
                return new StreamTicket(("ticket" + (getStreamProviders().size() + 1)).getBytes(StandardCharsets.UTF_8));
            }
        };
        mockProvider = allocator -> new ArrowStreamProvider.Task() {
            @Override
            public VectorSchemaRoot init(BufferAllocator allocator) {
                return mockRoot;
            }

            @Override
            public void run(VectorSchemaRoot root, ArrowStreamProvider.FlushSignal flushSignal) {

            }

            @Override
            public void onCancel() {

            }
        };
    }

    public void testRegisterStream() {
        StreamTicket ticket = streamManager.registerStream(mockProvider);
        assertNotNull(ticket);
        assertEquals(new StreamTicket("ticket1".getBytes(StandardCharsets.UTF_8)), ticket);
    }

    public void testGetStreamProvider() {
        StreamTicket ticket = streamManager.registerStream(mockProvider);
        ArrowStreamProvider retrievedProvider = streamManager.getStreamProvider(ticket);
        assertEquals(mockProvider, retrievedProvider);
    }

    public void testGetVectorSchemaRoot() {
        StreamTicket ticket = streamManager.registerStream(mockProvider);
        VectorSchemaRoot root = streamManager.getVectorSchemaRoot(ticket);
        assertEquals(mockRoot, root);
    }

    public void testRemoveStreamProvider() {
        StreamTicket ticket = streamManager.registerStream(mockProvider);
        streamManager.removeStreamProvider(ticket);
        assertNull(streamManager.getStreamProvider(ticket));
    }

    public void testClose() {
        StreamTicket ticket = streamManager.registerStream(mockProvider);
        streamManager.close();
        assertNull(streamManager.getStreamProvider(ticket));
    }

    public void testMultipleStreams() {
        ArrowStreamProvider mockProvider2 = mock(ArrowStreamProvider.class);

        StreamTicket ticket1 = streamManager.registerStream(mockProvider);
        StreamTicket ticket2 = streamManager.registerStream(mockProvider2);
        assertNotEquals(ticket1, ticket2);
        assertEquals(2, streamManager.getStreamProviders().size());
    }

    public void testInvalidTicket() {
        StreamTicket invalidTicket = new StreamTicket("invalid-ticket".getBytes(StandardCharsets.UTF_8));
        assertNull(streamManager.getStreamProvider(invalidTicket));
    }
    
     */
}
