/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.sink;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link StreamingIndexSink}: batch→bulk conversion, flush thresholds,
 * deterministic ids, immediate Arrow release, failure propagation, and finish semantics.
 */
public class StreamingIndexSinkTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private CapturingClient client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
        client = new CapturingClient(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        client.close();
        allocator.close();
        super.tearDown();
    }

    // ─── Conversion + flush thresholds ───────────────────────────────────

    public void testFeedConvertsRowsAndFlushesAtThreshold() {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of(), 2, 4);

        sink.feed(makeVsr(List.of("k", "v"), new Object[][] { { "a", "1" }, { "b", "2" }, { "c", "3" } }));

        // Threshold 2: exactly one bulk of two docs dispatched, one doc still pending.
        assertEquals(1, client.captured.size());
        client.respondSuccess(0);
        BulkRequest first = client.captured.get(0).request;
        assertEquals(2, first.requests().size());
        IndexRequest doc0 = (IndexRequest) first.requests().get(0);
        assertEquals("target", doc0.index());
        assertEquals("a", doc0.sourceAsMap().get("k"));
        assertEquals("1", doc0.sourceAsMap().get("v"));

        AtomicReference<StreamingIndexSink.Stats> stats = new AtomicReference<>();
        sink.finish(ActionListener.wrap(stats::set, e -> fail("finish failed: " + e)));
        // finish flushes the pending tail as a second bulk.
        assertEquals(2, client.captured.size());
        client.respondSuccess(1);

        assertNotNull("finish should complete once all bulks responded", stats.get());
        assertEquals(3, stats.get().rowsReceived());
        assertEquals(3, stats.get().docsIndexed());
        assertEquals(2, stats.get().bulksSent());
    }

    public void testArrowBuffersReleasedImmediatelyAfterFeed() {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of(), 100, 4);

        sink.feed(makeVsr(List.of("k"), new Object[][] { { "a" }, { "b" } }));

        // Nothing flushed yet (threshold 100), but the batch's off-heap buffers are freed:
        // the sink converts to IndexRequests and never retains Arrow memory.
        assertEquals(0, client.captured.size());
        assertEquals("sink must not retain Arrow memory", 0L, allocator.getAllocatedMemory());
        assertEquals(2, sink.getRowCount());

        sink.abort();
    }

    public void testReadResultIsAlwaysEmpty() {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of(), 100, 4);
        sink.feed(makeVsr(List.of("k"), new Object[][] { { "a" } }));
        assertFalse("batches are forwarded, never retained", sink.readResult().iterator().hasNext());
        sink.abort();
    }

    // ─── Document identity ───────────────────────────────────────────────

    public void testDeterministicIdsFromKeyColumns() {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of("k"), 2, 4);
        sink.feed(makeVsr(List.of("k", "v"), new Object[][] { { "a", "1" }, { "b", "2" } }));

        assertEquals(1, client.captured.size());
        List<DocWriteRequest<?>> docs = client.captured.get(0).request.requests();
        String idA = docs.get(0).id();
        String idB = docs.get(1).id();
        assertNotNull(idA);
        assertNotNull(idB);
        assertNotEquals("different key values must produce different ids", idA, idB);
        client.respondSuccess(0);

        // Re-materializing the same key value yields the same id (idempotent overwrite).
        StreamingIndexSink sink2 = new StreamingIndexSink(client, "target", List.of("k"), 2, 4);
        sink2.feed(makeVsr(List.of("k", "v"), new Object[][] { { "a", "999" }, { "z", "0" } }));
        assertEquals(2, client.captured.size());
        assertEquals(idA, client.captured.get(1).request.requests().get(0).id());
        client.respondSuccess(1);
        sink.abort();
        sink2.abort();
    }

    public void testAutoIdsWhenNoKeyColumns() {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of(), 1, 4);
        sink.feed(makeVsr(List.of("k"), new Object[][] { { "a" } }));
        assertEquals(1, client.captured.size());
        assertNull(client.captured.get(0).request.requests().get(0).id());
        client.respondSuccess(0);
        sink.abort();
    }

    // ─── Failure propagation ─────────────────────────────────────────────

    public void testBulkItemFailureFailsSubsequentFeedsAndFinish() {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of(), 1, 4);
        sink.feed(makeVsr(List.of("k"), new Object[][] { { "a" } }));
        assertEquals(1, client.captured.size());
        client.respondItemFailure(0, "mapping conflict");

        // Next feed must fail fast so the query terminal surfaces the write failure. The
        // ownership contract says a throwing feed leaves the batch with the caller.
        VectorSchemaRoot batch = makeVsr(List.of("k"), new Object[][] { { "b" } });
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> sink.feed(batch));
        assertTrue(e.getMessage().contains("target"));
        batch.close();

        AtomicReference<Exception> failure = new AtomicReference<>();
        sink.finish(ActionListener.wrap(s -> fail("finish must fail after a bulk item failure"), failure::set));
        assertNotNull(failure.get());
        assertTrue(failure.get().getMessage().contains("mapping conflict"));
    }

    public void testBulkTransportFailureFailsFinish() {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of(), 1, 4);
        sink.feed(makeVsr(List.of("k"), new Object[][] { { "a" } }));
        client.respondError(0, new RuntimeException("node disconnected"));

        AtomicReference<Exception> failure = new AtomicReference<>();
        sink.finish(ActionListener.wrap(s -> fail("finish must surface the transport failure"), failure::set));
        assertNotNull(failure.get());
        assertEquals("node disconnected", failure.get().getMessage());
    }

    // ─── Finish / close semantics ────────────────────────────────────────

    public void testFinishAwaitsInFlightBulks() {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of(), 1, 4);
        sink.feed(makeVsr(List.of("k"), new Object[][] { { "a" } }));
        assertEquals(1, client.captured.size());

        AtomicReference<StreamingIndexSink.Stats> stats = new AtomicReference<>();
        sink.finish(ActionListener.wrap(stats::set, e -> fail("finish failed: " + e)));
        assertNull("finish must not complete while a bulk is in flight", stats.get());

        client.respondSuccess(0);
        assertNotNull(stats.get());
        assertEquals(1, stats.get().docsIndexed());
    }

    public void testEngineCloseThenFinishStillFlushesPendingTail() {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of(), 100, 4);
        sink.feed(makeVsr(List.of("k"), new Object[][] { { "a" }, { "b" } }));
        // QueryExecution fires the terminal listener and then closes the terminal sink;
        // the owner's finish() runs around that close. Pending docs must survive close().
        sink.close();

        AtomicReference<StreamingIndexSink.Stats> stats = new AtomicReference<>();
        sink.finish(ActionListener.wrap(stats::set, e -> fail("finish failed: " + e)));
        assertEquals(1, client.captured.size());
        client.respondSuccess(0);
        assertNotNull(stats.get());
        assertEquals(2, stats.get().docsIndexed());
    }

    public void testFeedAfterCloseIsDroppedAndReleased() {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of(), 100, 4);
        sink.close();
        sink.feed(makeVsr(List.of("k"), new Object[][] { { "a" } }));
        assertEquals(0, sink.getRowCount());
        assertEquals("dropped batch must still be released", 0L, allocator.getAllocatedMemory());
    }

    public void testEmptyResultFinishCompletesWithZeroStats() {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of(), 100, 4);
        AtomicReference<StreamingIndexSink.Stats> stats = new AtomicReference<>();
        sink.finish(ActionListener.wrap(stats::set, e -> fail("finish failed: " + e)));
        assertNotNull(stats.get());
        assertEquals(0, stats.get().rowsReceived());
        assertEquals(0, stats.get().docsIndexed());
        assertEquals(0, stats.get().bulksSent());
    }

    /**
     * Regression: a feed saturating the in-flight permits must not hold the sink monitor
     * while waiting for a slot — bulk responses need that monitor to release permits.
     * (Deadlocked at multi-million-bucket cardinality before the fix: feed froze at
     * maxInFlight × maxDocsPerBulk docs and no response handler could ever run.)
     */
    public void testBackpressureWaitDoesNotBlockBulkCompletions() throws Exception {
        StreamingIndexSink sink = new StreamingIndexSink(client, "target", List.of(), 1, 2);

        // 5 rows, bulk size 1, 2 permits: the feeder must block after 2 dispatches.
        Thread feeder = new Thread(() -> sink.feed(makeVsr(List.of("k"), new Object[][] { { "a" }, { "b" }, { "c" }, { "d" }, { "e" } })));
        feeder.start();

        assertBusy(() -> assertEquals("feeder should dispatch up to the permit cap", 2, client.capturedCount()));
        assertTrue("feeder must still be waiting for a slot", feeder.isAlive());

        // Completing bulks from this thread must unblock the feeder — this is exactly the
        // path that deadlocked when the slot wait happened under the monitor.
        for (int i = 0; i < 5; i++) {
            final int idx = i;
            assertBusy(() -> assertTrue("bulk " + idx + " should be dispatched", client.capturedCount() > idx));
            client.respondSuccess(idx);
        }
        feeder.join(TimeUnit.SECONDS.toMillis(10));
        assertFalse("feeder must complete once permits recycle", feeder.isAlive());

        AtomicReference<StreamingIndexSink.Stats> stats = new AtomicReference<>();
        sink.finish(ActionListener.wrap(stats::set, e -> fail("finish failed: " + e)));
        assertNotNull(stats.get());
        assertEquals(5, stats.get().rowsReceived());
        assertEquals(5, stats.get().docsIndexed());
    }

    // ─── Helpers ─────────────────────────────────────────────────────────

    /** Client that captures bulk requests and lets the test complete them explicitly. */
    private static final class CapturingClient extends NoOpClient {
        record Call(BulkRequest request, ActionListener<BulkResponse> listener) {
        }

        final List<Call> captured = Collections.synchronizedList(new ArrayList<>());

        int capturedCount() {
            return captured.size();
        }

        CapturingClient(String testName) {
            super(testName);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof BulkRequest bulk) {
                captured.add(new Call(bulk, (ActionListener<BulkResponse>) listener));
            } else {
                super.doExecute(action, request, listener);
            }
        }

        void respondSuccess(int callIndex) {
            Call call = captured.get(callIndex);
            int n = call.request.requests().size();
            BulkItemResponse[] items = new BulkItemResponse[n];
            for (int i = 0; i < n; i++) {
                items[i] = new BulkItemResponse(
                    i,
                    DocWriteRequest.OpType.INDEX,
                    new IndexResponse(new ShardId("target", "_na_", 0), "id-" + i, 0, 0, 0, true)
                );
            }
            call.listener.onResponse(new BulkResponse(items, 1));
        }

        void respondItemFailure(int callIndex, String message) {
            Call call = captured.get(callIndex);
            BulkItemResponse[] items = new BulkItemResponse[] {
                new BulkItemResponse(
                    0,
                    DocWriteRequest.OpType.INDEX,
                    new BulkItemResponse.Failure("target", "id-0", new IllegalArgumentException(message))
                ) };
            call.listener.onResponse(new BulkResponse(items, 1));
        }

        void respondError(int callIndex, Exception e) {
            captured.get(callIndex).listener.onFailure(e);
        }
    }

    /** Builds a {@link VectorSchemaRoot} of varchar columns (mirrors RowProducingSinkTests). */
    private VectorSchemaRoot makeVsr(List<String> fieldNames, Object[][] rows) {
        List<Field> fields = new ArrayList<>();
        for (String name : fieldNames) {
            fields.add(new Field(name, FieldType.nullable(new ArrowType.Utf8()), null));
        }
        Schema schema = new Schema(fields);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        for (int col = 0; col < fieldNames.size(); col++) {
            FieldVector vector = root.getVector(col);
            VarCharVector varchar = (VarCharVector) vector;
            for (int row = 0; row < rows.length; row++) {
                byte[] bytes = rows[row][col].toString().getBytes(StandardCharsets.UTF_8);
                varchar.setSafe(row, bytes);
            }
        }
        root.setRowCount(rows.length);
        return root;
    }
}
