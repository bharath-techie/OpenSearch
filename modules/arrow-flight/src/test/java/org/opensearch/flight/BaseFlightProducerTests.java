/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a.java
 * compatible open source license.
 */

package org.opensearch.flight;

import org.opensearch.test.OpenSearchTestCase;

public class BaseFlightProducerTests extends OpenSearchTestCase {
    /*
    private BaseFlightProducer baseFlightProducer;
    private StreamManager streamManager;
    private ArrowStreamProvider arrowStreamProvider;
    private ArrowStreamProvider.Task task;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        streamManager = mock(StreamManager.class);
        BufferAllocator allocator = mock(BufferAllocator.class);
        arrowStreamProvider = mock(ArrowStreamProvider.class);
        task = mock(ArrowStreamProvider.Task.class);
        baseFlightProducer = new BaseFlightProducer(streamManager, allocator);
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
        StreamTicket streamTicket = new StreamTicket("testTicket");
        Ticket ticket = new Ticket(streamTicket.getBytes());
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getVectorSchemaRoot(any(StreamTicket.class))).thenReturn(root);
        when(streamManager.getArrowStreamProvider(any(StreamTicket.class))).thenReturn(arrowStreamProvider);
        when(arrowStreamProvider.createTask()).thenReturn(task);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            ArrowStreamProvider.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 3; i++) {
                Thread clientThread = new Thread(() -> {
                    listener.setReady(false);
                    listener.setReady(true);
                });
                listener.setReady(false);
                clientThread.start();
                assertTrue("Await consumption should return true", flushSignal.awaitConsumption(100));
                assertTrue("Data should be consumed", listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(task).run(any(VectorSchemaRoot.class), any(ArrowStreamProvider.FlushSignal.class));

        baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener);

        assertNull("No error should be set", listener.getError());
        assertEquals("PutNext should be called 3 times", 3, listener.getPutNextCount());
        assertEquals("Flush should be called 3 times", 3, flushCount.get());

        verify(streamManager).getVectorSchemaRoot(eq(streamTicket));
        verify(streamManager).getArrowStreamProvider(eq(streamTicket));
        verify(arrowStreamProvider).createTask();
        verify(task).run(eq(root), any(ArrowStreamProvider.FlushSignal.class));
        verify(streamManager).removeStreamProvider(eq(streamTicket));
        verify(root).close();

        assertFalse("Stream should not be cancelled", listener.isCancelled());
        assertNull("OnReady handler should not be set", listener.getOnReadyHandler());
        assertNull("OnCancel handler should not be set", listener.getOnCancelHandler());
    }

    public void testGetStream_WithSlowClient() throws Exception {
        StreamTicket streamTicket = new StreamTicket("testTicket");
        Ticket ticket = new Ticket(streamTicket.getBytes());
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getVectorSchemaRoot(any(StreamTicket.class))).thenReturn(root);
        when(streamManager.getArrowStreamProvider(any(StreamTicket.class))).thenReturn(arrowStreamProvider);
        when(arrowStreamProvider.createTask()).thenReturn(task);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();

        doAnswer(invocation -> {
            ArrowStreamProvider.FlushSignal flushSignal = invocation.getArgument(1);
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
                assertTrue("Await consumption should return true", flushSignal.awaitConsumption(300));
                assertTrue("Data should be consumed", listener.getDataConsumed());
                flushCount.incrementAndGet();
                listener.resetConsumptionLatch();
            }
            return null;
        }).when(task).run(any(VectorSchemaRoot.class), any(ArrowStreamProvider.FlushSignal.class));

        baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener);

        assertNull("No error should be set", listener.getError());
        assertEquals("PutNext should be called 5 times", 5, listener.getPutNextCount());
        assertEquals("Flush should be called 5 times", 5, flushCount.get());

        verify(streamManager).getVectorSchemaRoot(eq(streamTicket));
        verify(streamManager).getArrowStreamProvider(eq(streamTicket));
        verify(arrowStreamProvider).createTask();
        verify(task).run(eq(root), any(ArrowStreamProvider.FlushSignal.class));
        verify(streamManager).removeStreamProvider(eq(streamTicket));
        verify(root).close();

        assertFalse("Stream should not be cancelled", listener.isCancelled());
        assertNull("OnReady handler should not be set", listener.getOnReadyHandler());
        assertNull("OnCancel handler should not be set", listener.getOnCancelHandler());
    }

    public void testGetStream_WithSlowClientTimeout() throws Exception {
        StreamTicket streamTicket = new StreamTicket("testTicket");
        Ticket ticket = new Ticket(streamTicket.getBytes());
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getVectorSchemaRoot(any(StreamTicket.class))).thenReturn(root);
        when(streamManager.getArrowStreamProvider(any(StreamTicket.class))).thenReturn(arrowStreamProvider);
        when(arrowStreamProvider.createTask()).thenReturn(task);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            ArrowStreamProvider.FlushSignal flushSignal = invocation.getArgument(1);
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
        }).when(task).run(any(VectorSchemaRoot.class), any(ArrowStreamProvider.FlushSignal.class));

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));

        assertNotNull(listener.getError());
        assertEquals("Stream deadline exceeded for consumption", listener.getError().getMessage());
        assertEquals(0, listener.getPutNextCount());
        assertEquals(0, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithClientCancel() throws Exception {
        StreamTicket streamTicket = new StreamTicket("testTicket");
        Ticket ticket = new Ticket(streamTicket.getBytes());
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getVectorSchemaRoot(any(StreamTicket.class))).thenReturn(root);
        when(streamManager.getStreamProvider(any(StreamTicket.class))).thenReturn(arrowStreamProvider);
        when(arrowStreamProvider.createTask()).thenReturn(task);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            ArrowStreamProvider.FlushSignal flushSignal = invocation.getArgument(1);
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
        }).when(task).run(any(VectorSchemaRoot.class), any(ArrowStreamProvider.FlushSignal.class));

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));
        assertNotNull(listener.getError());
        assertEquals("Stream cancelled by client", listener.getError().getMessage());
        assertEquals(4, listener.getPutNextCount());
        assertEquals(4, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithUnresponsiveClient() throws Exception {
        StreamTicket streamTicket = new StreamTicket("testTicket");
        Ticket ticket = new Ticket(streamTicket.getBytes());
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getVectorSchemaRoot(any(StreamTicket.class))).thenReturn(root);
        when(streamManager.getArrowStreamProvider(any(StreamTicket.class))).thenReturn(arrowStreamProvider);
        when(arrowStreamProvider.createTask()).thenReturn(task);

        AtomicInteger flushCount = new AtomicInteger(0);
        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            ArrowStreamProvider.FlushSignal flushSignal = invocation.getArgument(1);
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
        }).when(task).run(any(VectorSchemaRoot.class), any(ArrowStreamProvider.FlushSignal.class));

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));

        assertNotNull(listener.getError());
        assertEquals("Stream deadline exceeded for consumption", listener.getError().getMessage());
        assertEquals(0, listener.getPutNextCount());
        assertEquals(0, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithServerBackpressure() throws Exception {
        StreamTicket streamTicket = new StreamTicket("testTicket");
        Ticket ticket = new Ticket(streamTicket.getBytes());
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getVectorSchemaRoot(any(StreamTicket.class))).thenReturn(root);
        when(streamManager.getArrowStreamProvider(any(StreamTicket.class))).thenReturn(arrowStreamProvider);
        when(arrowStreamProvider.createTask()).thenReturn(task);

        TestServerStreamListener listener = new TestServerStreamListener();
        AtomicInteger flushCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            ArrowStreamProvider.FlushSignal flushSignal = invocation.getArgument(1);
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
        }).when(task).run(any(VectorSchemaRoot.class), any(ArrowStreamProvider.FlushSignal.class));

        baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener);

        assertNull(listener.getError());
        assertEquals(5, listener.getPutNextCount());
        assertEquals(5, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithServerError() throws Exception {
        StreamTicket streamTicket = new StreamTicket("testTicket");
        Ticket ticket = new Ticket(streamTicket.getBytes());
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getVectorSchemaRoot(any(StreamTicket.class))).thenReturn(root);
        when(streamManager.getArrowStreamProvider(any(StreamTicket.class))).thenReturn(arrowStreamProvider);
        when(arrowStreamProvider.createTask()).thenReturn(task);

        TestServerStreamListener listener = new TestServerStreamListener();
        AtomicInteger flushCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            ArrowStreamProvider.FlushSignal flushSignal = invocation.getArgument(1);
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
        }).when(task).run(any(VectorSchemaRoot.class), any(ArrowStreamProvider.FlushSignal.class));

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));

        assertNotNull(listener.getError());
        assertEquals("Server error", listener.getError().getMessage());
        assertEquals(4, listener.getPutNextCount());
        assertEquals(4, flushCount.get());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithMultipleConcurrentClients() throws Exception {
        // Arrange
        int numClients = 5;
        CountDownLatch startLatch = new CountDownLatch(numClients);
        CountDownLatch endLatch = new CountDownLatch(numClients);
        AtomicInteger successCount = new AtomicInteger(0);

        StreamTicket streamTicket = new StreamTicket("testTicket");
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        when(streamManager.getVectorSchemaRoot(any(StreamTicket.class))).thenReturn(mockRoot);
        when(streamManager.getArrowStreamProvider(any(StreamTicket.class))).thenReturn(arrowStreamProvider);
        when(arrowStreamProvider.createTask()).thenReturn(task);

        // Act
        for (int i = 0; i < numClients; i++) {
            new Thread(() -> {
                try {
                    startLatch.countDown();
                    startLatch.await(); // Ensure all threads start at the same time
                    Ticket ticket = new Ticket(streamTicket.getBytes());
                    TestServerStreamListener listener = new TestServerStreamListener();
                    baseFlightProducer.getStream(null, ticket, listener);
                    if (listener.getError() == null) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Count failed attempts
                } finally {
                    endLatch.countDown();
                }
            }).start();
        }

        // Assert
        assertTrue("All threads should finish within the timeout", endLatch.await(10, TimeUnit.SECONDS));
        assertEquals("All clients should successfully get the stream", numClients, successCount.get());
        verify(streamManager, times(numClients)).getVectorSchemaRoot(any(StreamTicket.class));
        verify(streamManager, times(numClients)).getArrowStreamProvider(any(StreamTicket.class));
        verify(arrowStreamProvider, times(numClients)).createTask();
        verify(task, times(numClients)).run(eq(mockRoot), any(ArrowStreamProvider.FlushSignal.class));
        verify(streamManager, times(numClients)).removeStreamProvider(any(StreamTicket.class));
        verify(mockRoot, times(numClients)).close();
    }

    public void testGetStream_StreamNotFound() throws Exception {
        // Arrange
        StreamTicket streamTicket = new StreamTicket("nonexistentTicket");
        when(streamManager.getVectorSchemaRoot(any(StreamTicket.class))).thenThrow(new IllegalArgumentException("Stream not found"));

        TestServerStreamListener listener = new TestServerStreamListener();
        Ticket ticket = new Ticket(streamTicket.getBytes());

        // Act
        baseFlightProducer.getStream(null, ticket, listener);

        // Assert
        verify(streamManager).getVectorSchemaRoot(any(StreamTicket.class));
        assertNotNull("Error should be set", listener.getError());
        assertTrue("Error should be IllegalArgumentException", listener.getError() instanceof IllegalArgumentException);
        assertEquals("Error message should match", "Stream not found", listener.getError().getMessage());
        verify(streamManager, never()).getArrowStreamProvider(any(StreamTicket.class));
        verify(arrowStreamProvider, never()).createTask();
        assertEquals(0, listener.getPutNextCount());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
    }

    public void testGetStream_EdgeCases() throws Exception {
        // Test with null ticket
        TestServerStreamListener listener = new TestServerStreamListener();
        assertThrows("Should throw NullPointerException for null ticket",
            NullPointerException.class,
            () -> baseFlightProducer.getStream(null, null, listener)
        );

        // Test with empty ticket
        Ticket emptyTicket = new Ticket(new byte[0]);
        baseFlightProducer.getStream(null, emptyTicket, listener);
        assertNotNull("Error should be set for empty ticket", listener.getError());
        assertTrue("Error should be IllegalArgumentException for empty ticket", listener.getError() instanceof IllegalArgumentException);
        assertEquals("Error message should match for empty ticket", "Invalid ticket format", listener.getError().getMessage());

        // Test with null listener
        StreamTicket streamTicket = new StreamTicket("testTicket");
        Ticket validTicket = new Ticket(streamTicket.getBytes());
        assertThrows("Should throw NullPointerException for null listener",
            NullPointerException.class,
            () -> baseFlightProducer.getStream(null, validTicket, null)
        );

        // Test with invalid ticket format
        Ticket invalidTicket = new Ticket("invalid".getBytes());
        listener = new TestServerStreamListener();
        baseFlightProducer.getStream(null, invalidTicket, listener);
        assertNotNull("Error should be set for invalid ticket", listener.getError());
        assertTrue("Error should be IllegalArgumentException for invalid ticket", listener.getError() instanceof IllegalArgumentException);
        assertEquals("Error message should match for invalid ticket", "Invalid ticket format", listener.getError().getMessage());

        // Verify that no stream provider is created or removed for invalid cases
        verify(streamManager, never()).getVectorSchemaRoot(any(StreamTicket.class));
        verify(streamManager, never()).getArrowStreamProvider(any(StreamTicket.class));
        verify(arrowStreamProvider, never()).createTask();
        verify(streamManager, never()).removeStreamProvider(any(StreamTicket.class));
    }

    public void testGetStream_WithInterruptedException() throws Exception {
        StreamTicket streamTicket = new StreamTicket("testTicket");
        Ticket ticket = new Ticket(streamTicket.getBytes());
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getVectorSchemaRoot(any(StreamTicket.class))).thenReturn(root);
        when(streamManager.getArrowStreamProvider(any(StreamTicket.class))).thenReturn(arrowStreamProvider);
        when(arrowStreamProvider.createTask()).thenReturn(task);

        TestServerStreamListener listener = new TestServerStreamListener();
        doAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return null;
        }).when(task).run(any(VectorSchemaRoot.class), any(ArrowStreamProvider.FlushSignal.class));

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));
        assertTrue(Thread.interrupted()); // Clear the interrupt flag
        assertNotNull(listener.getError());
        assertTrue(listener.getError() instanceof InterruptedException);

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

    public void testGetStream_WithFlushSignalTimeout() throws Exception {
        StreamTicket streamTicket = new StreamTicket("testTicket");
        Ticket ticket = new Ticket(streamTicket.getBytes());
        final VectorSchemaRoot root = mock(VectorSchemaRoot.class);

        when(streamManager.getVectorSchemaRoot(any(StreamTicket.class))).thenReturn(root);
        when(streamManager.getArrowStreamProvider(any(StreamTicket.class))).thenReturn(arrowStreamProvider);
        when(arrowStreamProvider.createTask()).thenReturn(task);

        TestServerStreamListener listener = new TestServerStreamListener();
        AtomicInteger flushCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            ArrowStreamProvider.FlushSignal flushSignal = invocation.getArgument(1);
            for (int i = 0; i < 3; i++) {
                assertFalse("Await consumption should timeout", flushSignal.awaitConsumption(1)); // Very short timeout
                flushCount.incrementAndGet();
            }
            return null;
        }).when(task).run(any(VectorSchemaRoot.class), any(ArrowStreamProvider.FlushSignal.class));

        assertThrows(RuntimeException.class, () -> baseFlightProducer.getStream(mock(FlightProducer.CallContext.class), ticket, listener));
        assertNotNull(listener.getError());
        assertEquals("Stream deadline exceeded for consumption", listener.getError().getMessage());
        assertEquals(3, flushCount.get());
        assertEquals(0, listener.getPutNextCount());

        verify(streamManager).removeStreamProvider(any(StreamTicket.class));
        verify(root).close();
    }

     */
}
