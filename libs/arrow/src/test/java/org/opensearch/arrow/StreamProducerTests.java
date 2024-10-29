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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class StreamProducerTests extends OpenSearchTestCase {

    private StreamProducer streamProducer;
    private BufferAllocator mockAllocator;
    private VectorSchemaRoot mockRoot;
    private StreamProducer.BatchedJob mockBatchedJob;
    private StreamProducer.FlushSignal mockFlushSignal;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockAllocator = mock(BufferAllocator.class);
        mockRoot = mock(VectorSchemaRoot.class);
        mockFlushSignal = mock(StreamProducer.FlushSignal.class);
        mockBatchedJob =  new StreamProducer.BatchedJob() {
            @Override
            public void run(VectorSchemaRoot root, StreamProducer.FlushSignal flushSignal) {
                flushSignal.awaitConsumption(1000);
            }

            @Override
            public void onCancel() {
                // Do nothing for this test
            }
        };
        streamProducer = new StreamProducer() {
            @Override
            public VectorSchemaRoot createRoot(BufferAllocator allocator) {
                return mockRoot;
            }

            @Override
            public BatchedJob createJob(BufferAllocator allocator) {
                return mockBatchedJob;
            }
        };
    }

    public void testCreateRoot() {
        VectorSchemaRoot result = streamProducer.createRoot(mockAllocator);
        assertSame(mockRoot, result);
    }

    public void testCreateJob() {
        StreamProducer.BatchedJob result = streamProducer.createJob(mockAllocator);
        assertSame(mockBatchedJob, result);
    }

    public void testEstimatedRowCount() {
        assertEquals(-1, streamProducer.estimatedRowCount());
    }

    public void testGetAction() {
        assertEquals("", streamProducer.getAction());
    }


    public void testFlushSignalAwaitConsumption() {
        StreamProducer.FlushSignal mockFlushSignal = mock(StreamProducer.FlushSignal.class);
        StreamProducer.BatchedJob job = streamProducer.createJob(mockAllocator);

        job.run(mockRoot, mockFlushSignal);

        verify(mockFlushSignal).awaitConsumption(1000);
    }

    public void testCustomEstimatedRowCount() {
        StreamProducer customProducer = new StreamProducer() {
            @Override
            public VectorSchemaRoot createRoot(BufferAllocator allocator) {
                return mockRoot;
            }

            @Override
            public BatchedJob createJob(BufferAllocator allocator) {
                return mockBatchedJob;
            }

            @Override
            public int estimatedRowCount() {
                return 100;
            }
        };

        assertEquals(100, customProducer.estimatedRowCount());
    }

    public void testCustomGetAction() {
        StreamProducer customProducer = new StreamProducer() {
            @Override
            public VectorSchemaRoot createRoot(BufferAllocator allocator) {
                return mockRoot;
            }

            @Override
            public BatchedJob createJob(BufferAllocator allocator) {
                return mockBatchedJob;
            }

            @Override
            public String getAction() {
                return "CustomAction";
            }
        };

        assertEquals("CustomAction", customProducer.getAction());
    }
}
