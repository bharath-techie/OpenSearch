/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a.java
 * compatible open source license.
 */

package org.opensearch.flight;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.opensearch.arrow.StreamProducer;
import org.opensearch.arrow.StreamManager;
import org.opensearch.arrow.StreamTicket;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BaseFlightProducerTests extends OpenSearchTestCase {

    private BaseFlightProducer baseFlightProducer;
    private StreamManager streamManager;
    private StreamProducer streamProducer;
    private StreamProducer.BatchedJob batchedJob;
    private static final String LOCAL_NODE_ID = "localNodeId";
    private final FlightService flightService = mock(FlightService.class);
    private final Ticket ticket = new Ticket((new StreamTicket("test-ticket", LOCAL_NODE_ID)).toBytes());
    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        streamManager = mock(StreamManager.class);
        when(streamManager.getLocalNodeId()).thenReturn(LOCAL_NODE_ID);
        when(flightService.getLocalNodeId()).thenReturn(LOCAL_NODE_ID);
        allocator = mock(BufferAllocator.class);
        streamProducer = mock(StreamProducer.class);
        batchedJob = mock(StreamProducer.BatchedJob.class);
        baseFlightProducer = new BaseFlightProducer(flightService, streamManager, allocator);
    }

    private static class TestServerStreamListener implements FlightProducer.ServerStreamListener {
        private final CountDownLatch completionLatch = new CountDownLatch(1);
        private final AtomicInteger putNextCount = new AtomicInteger(0);
        private final AtomicBoolean isCancelled = new AtomicBoolean(false);
        private Throwable error;
        private final AtomicBoolean dataConsumed = new AtomicBoolean(false);
        private final AtomicBoolean ready = new AtomicBoolean(false);
        private Runnable onReadyHandler;
        private Runnable onCancelHandler;

        @Override
        public void putNext() {
            assertFalse(dataConsumed.get());
            putNextCount.incrementAndGet();
            dataConsumed.set(true);
        }

        @Override
        public boolean isReady() {
            return ready.get();
        }

        public void setReady(boolean val) {
            ready.set(val);
            if (this.onReadyHandler != null) {
                this.onReadyHandler.run();
            }
        }

        @Override
        public void start(VectorSchemaRoot root) {
            // No-op for this test
        }

        @Override
        public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
        }

        @Override
        public void putNext(ArrowBuf metadata) {
            putNext();
        }

        @Override
        public void putMetadata(ArrowBuf metadata) {

        }

        @Override
        public void completed() {
            completionLatch.countDown();
        }

        @Override
        public void error(Throwable t) {
            error = t;
            completionLatch.countDown();
        }

        @Override
        public boolean isCancelled() {
            return isCancelled.get();
        }

        @Override
        public void setOnReadyHandler(Runnable handler) {
            this.onReadyHandler = handler;
        }

        @Override
        public void setOnCancelHandler(Runnable handler) {
            this.onCancelHandler = handler;
        }

        public void resetConsumptionLatch() {
            dataConsumed.set(false);
        }

        public boolean getDataConsumed() {
            return dataConsumed.get();
        }

        public int getPutNextCount() {
            return putNextCount.get();
        }

        public Throwable getError() {
            return error;
        }

        public void cancel() {
            isCancelled.set(true);
            if (this.onCancelHandler != null) {
                this.onCancelHandler.run();
            }
        }
    }

    public void testGetStream_SuccessfulFlow() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);
        when(streamManager.getStreamProvider(any(StreamTicket.class))).thenReturn(new StreamManager.StreamHolder(streamProducer, root));
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 3; i++) {
                Thread clientThread = new Thread(() -> {
                    listener.setReady(false);
                    listener.setReady(true);
                });
                listener.setReady(false);
                clientThread.start();
                flushSignal.awaitConsumption(100);
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(VectorSchemaRoot.class), any(StreamProducer.FlushSignal.class));
        baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener);

        assertNull(listener.getError());
        assertEquals(3, listener.getPutNextCount());
        assertEquals(3, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithSlowClient() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getStreamProvider(any(StreamTicket.class))).thenReturn(new StreamManager.StreamHolder(streamProducer, root));
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();

        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                Thread clientThread = new Thread(() -> {
                    try {
                        listener.setReady(false);
                        Thread.sleep(100);
                        listener.setReady(true);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
                listener.setReady(false);
                clientThread.start();
                flushSignal.awaitConsumption(300); // waiting for consumption for more than client thread sleep
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(), any());

        baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener);

        assertNull(listener.getError());
        assertEquals(5, listener.getPutNextCount());
        assertEquals(5, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithSlowClientTimeout() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getStreamProvider(any(StreamTicket.class))).thenReturn(new StreamManager.StreamHolder(streamProducer, root));
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                Thread clientThread = new Thread(() -> {
                    try {
                        listener.setReady(false);
                        Thread.sleep(400);
                        listener.setReady(true);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
                listener.setReady(false);
                clientThread.start();
                flushSignal.awaitConsumption(100); // waiting for consumption for less than client thread sleep
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(), any());

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));

        assertNotNull(listener.getError());
        assertEquals("Stream deadline exceeded for consumption", listener.getError().getMessage());
        assertEquals(0, listener.getPutNextCount());
        assertEquals(0, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithClientCancel() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getStreamProvider(any(StreamTicket.class))).thenReturn(new StreamManager.StreamHolder(streamProducer, root));
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                int finalI = i;
                Thread clientThread = new Thread(() -> {
                    if (finalI == 4) {
                        listener.cancel();
                    } else {
                        listener.setReady(false);
                        listener.setReady(true);
                    }
                });
                listener.setReady(false);
                clientThread.start();
                flushSignal.awaitConsumption(100); // waiting for consumption for less than client thread sleep
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(), any());

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));
        assertNotNull(listener.getError());
        assertEquals("Stream cancelled by client", listener.getError().getMessage());
        assertEquals(4, listener.getPutNextCount());
        assertEquals(4, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithUnresponsiveClient() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getStreamProvider(any(StreamTicket.class))).thenReturn(new StreamManager.StreamHolder(streamProducer, root));
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                Thread clientThread = new Thread(() -> {
                    listener.setReady(false);
                    // not setting ready to simulate unresponsive behaviour
                });
                listener.setReady(false);
                clientThread.start();
                flushSignal.awaitConsumption(100); // waiting for consumption for less than client thread sleep
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(), any());

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));

        assertNotNull(listener.getError());
        assertEquals("Stream deadline exceeded for consumption", listener.getError().getMessage());
        assertEquals(0, listener.getPutNextCount());
        assertEquals(0, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithServerBackpressure() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getStreamProvider(any(StreamTicket.class))).thenReturn(new StreamManager.StreamHolder(streamProducer, root));
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        TestServerStreamListener listener = new TestServerStreamListener();
        AtomicInteger flushCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                Thread clientThread = new Thread(() -> {
                    listener.setReady(false);
                    listener.setReady(true);
                });
                listener.setReady(false);
                clientThread.start();
                Thread.sleep(100); // simulating writer backpressure
                flushSignal.awaitConsumption(100);
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(VectorSchemaRoot.class), any(StreamProducer.FlushSignal.class));

        baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener);

        assertNull(listener.getError());
        assertEquals(5, listener.getPutNextCount());
        assertEquals(5, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithServerError() throws Exception {
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getStreamProvider(any(StreamTicket.class))).thenReturn(new StreamManager.StreamHolder(streamProducer, root));
        when(streamProducer.createJob(any(BufferAllocator.class))).thenReturn(batchedJob);
        when(streamProducer.createRoot(any(BufferAllocator.class))).thenReturn(root);

        TestServerStreamListener listener = new TestServerStreamListener();
        AtomicInteger flushCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 5; i++) {
                Thread clientThread = new Thread(() -> {
                    listener.setReady(false);
                    listener.setReady(true);
                });
                listener.setReady(false);
                clientThread.start();
                if (i == 4) {
                    throw new RuntimeException("Server error");
                }
                flushSignal.awaitConsumption(100);
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(VectorSchemaRoot.class), any(StreamProducer.FlushSignal.class));

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));

        assertNotNull(listener.getError());
        assertEquals("Server error", listener.getError().getMessage());
        assertEquals(4, listener.getPutNextCount());
        assertEquals(4, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_StreamNotFound() throws Exception {

        when(streamManager.getStreamProvider(any(StreamTicket.class))).thenReturn(null);

        TestServerStreamListener listener = new TestServerStreamListener();

        baseFlightProducer.getStream(null, ticket, listener);

        assertNotNull(listener.getError());
        assertTrue(listener.getError().getMessage().contains("Stream not found"));
        assertEquals(0, listener.getPutNextCount());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
    }

    public void testProxyStreamProviderCreationWithDifferentNodeIDs() {
        // Mock streamTicket with a remote node ID different from LOCAL_NODE_ID
        String remoteNodeId = LOCAL_NODE_ID + "_remote";
        StreamTicket streamTicket = new StreamTicket("test-ticket", remoteNodeId);

        FlightClient mockFlightClient = mock(FlightClient.class);
        when(flightService.getFlightClient(anyString())).thenReturn(mockFlightClient);

        FlightStream mockFlightStream = mock(FlightStream.class);
        when(mockFlightClient.getStream(any())).thenReturn(mockFlightStream);

        FlightClient remoteClient = flightService.getFlightClient(streamTicket.getNodeID());
        verify(flightService).getFlightClient(remoteNodeId);

        StreamProducer proxyProvider = new ProxyStreamProducer(remoteClient.getStream(new Ticket(streamTicket.toBytes())));
        VectorSchemaRoot remoteRoot = proxyProvider.createRoot(allocator);

        StreamManager.StreamHolder streamHolder = new StreamManager.StreamHolder(proxyProvider, remoteRoot);
        assertNotNull(streamHolder);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            StreamProducer.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 3; i++) {
                Thread clientThread = new Thread(() -> {
                    listener.setReady(false);
                    listener.setReady(true);
                });
                listener.setReady(false);
                clientThread.start();
                flushSignal.awaitConsumption(100);
                assertTrue(listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(batchedJob).run(any(VectorSchemaRoot.class), any(StreamProducer.FlushSignal.class));

        baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), new Ticket(streamTicket.toBytes()), listener);

        assertNull(listener.getError());
        assertEquals(3, listener.getPutNextCount());
        assertEquals(3, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        // verify(root).close();

        verify(mockFlightClient).getStream(new Ticket(streamTicket.toBytes()));

        verify(streamManager, never()).getStreamProvider(any());
    }
}
