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
import org.opensearch.core.tasks.TaskId;

import java.util.function.Supplier;

import static org.mockito.Mockito.mock;

public class StreamManagerTests extends OpenSearchTestCase {

    private TestStreamManager streamManager;
    private BufferAllocator testAllocator;
    private TestStreamProducer testStreamProducer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        testAllocator = mock(BufferAllocator.class);
        Supplier<BufferAllocator> allocatorSupplier = () -> testAllocator;
        streamManager = new TestStreamManager(allocatorSupplier);
        testStreamProducer = new TestStreamProducer();
    }

    public void testRegisterStream() {
        TaskId taskId = new TaskId("node1", 1);
        StreamTicket ticket = streamManager.registerStream(testStreamProducer, taskId);

        assertNotNull(ticket);
        assertEquals("test-ticket", ticket.getTicketID());
        assertEquals("test-node", ticket.getNodeID());
        assertTrue(testStreamProducer.rootCreated);
    }

    public void testGetStreamProducer() {
        TaskId taskId = new TaskId("node1", 1);
        StreamTicket ticket = streamManager.registerStream(testStreamProducer, taskId);
        StreamManager.StreamProducerHolder holder = streamManager.getStreamProducer(ticket);

        assertNotNull(holder);
        assertSame(testStreamProducer, holder.getProducer());
        assertNotNull(holder.getRoot());
    }

    public void testRemoveStreamProvider() {
        TaskId taskId = new TaskId("node1", 1);
        StreamTicket ticket = streamManager.registerStream(testStreamProducer, taskId);
        streamManager.removeStreamProvider(ticket);

        assertNull(streamManager.getStreamProducer(ticket));
    }

    public void testClose() throws Exception {
        TaskId taskId = new TaskId("node1", 1);
        StreamTicket ticket = streamManager.registerStream(testStreamProducer, taskId);
        streamManager.close();

        assertNull(streamManager.getStreamProducer(ticket));
    }

    public void testAllocatorSupplier() {
        assertSame(testAllocator, streamManager.allocatorSupplier().get());
    }

    private static class TestStreamManager extends StreamManager {
        public TestStreamManager(Supplier<BufferAllocator> allocatorSupplier) {
            super(allocatorSupplier);
        }

        @Override
        public StreamIterator getStreamIterator(StreamTicket ticket) {
            return null; // Not tested in this class
        }

        @Override
        public String generateUniqueTicket() {
            return "test-ticket";
        }

        @Override
        public String getLocalNodeId() {
            return "test-node";
        }
    }

    private static class TestStreamProducer implements StreamProducer {
        boolean rootCreated = false;

        @Override
        public VectorSchemaRoot createRoot(BufferAllocator allocator) {
            rootCreated = true;
            return mock(VectorSchemaRoot.class);
        }

        @Override
        public BatchedJob createJob(BufferAllocator allocator) {
            return null;
        }
    }
}

