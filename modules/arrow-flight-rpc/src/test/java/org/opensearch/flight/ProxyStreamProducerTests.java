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
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyStreamProducerTests extends OpenSearchTestCase {

    private FlightStream mockRemoteStream;
    private BufferAllocator mockAllocator;
    private ProxyStreamProducer proxyStreamProvider;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockRemoteStream = mock(FlightStream.class);
        mockAllocator = mock(BufferAllocator.class);
        proxyStreamProvider = new ProxyStreamProducer(mockRemoteStream);
    }

    public void testCreateRoot() {
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        when(mockRemoteStream.getRoot()).thenReturn(mockRoot);

        VectorSchemaRoot result = proxyStreamProvider.createRoot(mockAllocator);

        assertEquals(mockRoot, result);
        verify(mockRemoteStream).getRoot();
    }

    public void testCreateJob() {
        StreamProducer.BatchedJob job = proxyStreamProvider.createJob(mockAllocator);

        assertNotNull(job);
        assertTrue(job instanceof ProxyStreamProducer.ProxyBatchedJob);
    }

    public void testProxyBatchedJob() throws Exception {
        StreamProducer.BatchedJob job = proxyStreamProvider.createJob(mockAllocator);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        StreamProducer.FlushSignal mockFlushSignal = mock(StreamProducer.FlushSignal.class);

        when(mockRemoteStream.next()).thenReturn(true, true, false);

        job.run(mockRoot, mockFlushSignal);

        verify(mockRemoteStream, times(3)).next();
        verify(mockFlushSignal, times(2)).awaitConsumption(1000);
        verify(mockRemoteStream).close();
    }

    public void testProxyBatchedJobWithException() throws Exception {
        StreamProducer.BatchedJob job = proxyStreamProvider.createJob(mockAllocator);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        StreamProducer.FlushSignal mockFlushSignal = mock(StreamProducer.FlushSignal.class);

        when(mockRemoteStream.next()).thenReturn(true, false);
        doThrow(new RuntimeException("Test exception")).when(mockRemoteStream).close();

        try {
            job.run(mockRoot, mockFlushSignal);
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertEquals("Test exception", e.getCause().getMessage());
        }

        verify(mockRemoteStream, times(2)).next();
        verify(mockFlushSignal).awaitConsumption(1000);
        verify(mockRemoteStream).close();
    }

    public void testProxyBatchedJobOnCancel() {
        StreamProducer.BatchedJob job = proxyStreamProvider.createJob(mockAllocator);

        // onCancel() method is empty, so we just call it to ensure it doesn't throw any exceptions
        job.onCancel();
    }
}
