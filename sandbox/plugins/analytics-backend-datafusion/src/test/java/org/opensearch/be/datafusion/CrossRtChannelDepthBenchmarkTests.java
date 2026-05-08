/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.apache.arrow.c.Data.importField;

/**
 * End-to-end benchmark comparing CrossRtStream channel(1) vs channel(2).
 * Passes channel_depth and batch_size from Java through the WireDatafusionQueryConfig FFM struct.
 *
 * Uses batch_size=1024 to maximize the number of batches flowing through the channel,
 * amplifying the per-batch scheduling overhead that channel(2) eliminates.
 *
 * NOTE: The default Gradle test task runs with -XX:TieredStopAtLevel=1 (C2 disabled).
 * For production-grade numbers with full JIT, run directly:
 *
 *   ./gradlew :sandbox:plugins:analytics-backend-datafusion:test \
 *       --tests "*CrossRtChannelDepthBenchmarkTests*" -Dsandbox.enabled=true \
 *       -Dtests.jvm.argline="-XX:+UseG1GC"
 */
public class CrossRtChannelDepthBenchmarkTests extends OpenSearchTestCase {

    private static final int WARMUP_ITERATIONS = 5;
    private static final int MEASURED_ITERATIONS = 15;
    private static final int BATCH_SIZE = 1024;

    /**
     * Layout must match Rust's #[repr(C)] WireDatafusionQueryConfig exactly.
     */
    private static final MemoryLayout WIRE_CONFIG_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_LONG.withName("batch_size"),
        ValueLayout.JAVA_LONG.withName("target_partitions"),
        ValueLayout.JAVA_LONG.withName("min_skip_run_default"),
        ValueLayout.JAVA_DOUBLE.withName("min_skip_run_selectivity_threshold"),
        ValueLayout.JAVA_INT.withName("parquet_pushdown_filters"),
        ValueLayout.JAVA_INT.withName("indexed_pushdown_filters"),
        ValueLayout.JAVA_INT.withName("force_strategy"),
        ValueLayout.JAVA_INT.withName("force_pushdown"),
        ValueLayout.JAVA_INT.withName("cost_predicate"),
        ValueLayout.JAVA_INT.withName("cost_collector"),
        ValueLayout.JAVA_INT.withName("max_collector_parallelism"),
        ValueLayout.JAVA_INT.withName("single_collector_strategy"),
        ValueLayout.JAVA_INT.withName("tree_collector_strategy"),
        ValueLayout.JAVA_INT.withName("channel_depth")
    );

    private NativeRuntimeHandle runtimeHandle;
    private ReaderHandle readerHandle;
    private RootAllocator rootAllocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        NativeBridge.initTokioRuntimeManager(4);
        Path spillDir = createTempDir("datafusion-bench-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(
            1024L * 1024 * 1024,
            0L,
            spillDir.toString(),
            1024L * 1024 * 1024
        );
        runtimeHandle = new NativeRuntimeHandle(runtimePtr);
        rootAllocator = new RootAllocator(Long.MAX_VALUE);

        String dataDir = "/Users/abandeji/Public/work-dump/experiments/data/nodes/0/indices/ATTb8ViJT0mrLoeWpkEtrA/0/parquet";
        readerHandle = new ReaderHandle(dataDir, new String[]{"generation-1.parquet", "generation-2.parquet"});
    }

    @Override
    public void tearDown() throws Exception {
        readerHandle.close();
        runtimeHandle.close();
        rootAllocator.close();
        super.tearDown();
    }

    // ── Multi-batch: GROUP BY client_ip (high cardinality → many output batches) ──
    public void testGroupByHighCardinality() throws Exception {
        runBench("SELECT client_ip, COUNT(*), SUM(sent_bytes) FROM t GROUP BY client_ip");
    }

    // ── Multi-batch: scan 8.5M+ rows (batch_size=1024 → thousands of batches) ──
    public void testFullScan() throws Exception {
        runBench("SELECT client_ip, backend_ip, sent_bytes, received_bytes, elb_status_code FROM t");
    }

    // ── Single batch: scalar aggregation (1 output row) ──
    public void testScalarAggregation() throws Exception {
        runBench("SELECT SUM(sent_bytes), SUM(received_bytes), COUNT(*) FROM t");
    }

    // ── Multi-batch: filter + scan ──
    public void testFilterScan() throws Exception {
        runBench("SELECT client_ip, sent_bytes, elb_status_code FROM t WHERE elb_status_code >= 500");
    }

    private void runBench(String sql) throws Exception {
        int channelDepth = Integer.parseInt(System.getProperty("tests.channel.depth", "1"));

        byte[] substraitPlan = NativeBridge.sqlToSubstrait(
            readerHandle.getPointer(), "t", sql, runtimeHandle.get()
        );

        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════");
        System.out.println("  Query: " + sql);
        System.out.println("  batch_size=" + BATCH_SIZE + " | channel_depth=" + channelDepth);
        System.out.println("  warmup=" + WARMUP_ITERATIONS + " | measured=" + MEASURED_ITERATIONS);
        System.out.println("═══════════════════════════════════════════════════════════════════════");

        long[] results = benchmarkWithChannelDepth(substraitPlan, channelDepth);
        printResults("channel(" + channelDepth + ")", results);
    }

    private long[] benchmarkWithChannelDepth(byte[] substraitPlan, int channelDepth) throws Exception {
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            executeAndConsume(substraitPlan, channelDepth);
        }

        long[] durationsUs = new long[MEASURED_ITERATIONS];
        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            long start = System.nanoTime();
            executeAndConsume(substraitPlan, channelDepth);
            durationsUs[i] = (System.nanoTime() - start) / 1000;
        }
        return durationsUs;
    }

    private long executeAndConsume(byte[] substraitPlan, int channelDepth) throws Exception {
        long queryConfigPtr = allocateWireConfig(channelDepth);
        try {
            CompletableFuture<Long> future = new CompletableFuture<>();
            NativeBridge.executeQueryAsync(
                readerHandle.getPointer(),
                "t",
                substraitPlan,
                runtimeHandle.get(),
                0L,
                queryConfigPtr,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Long ptr) { future.complete(ptr); }
                    @Override
                    public void onFailure(Exception e) { future.completeExceptionally(e); }
                }
            );
            long streamPtr = future.join();
            StreamHandle streamHandle = new StreamHandle(streamPtr, runtimeHandle);

            BufferAllocator queryAllocator = rootAllocator.newChildAllocator("bench", 0, Long.MAX_VALUE);
            CDataDictionaryProvider dictProvider = new CDataDictionaryProvider();
            try {
                CompletableFuture<Long> schemaFuture = new CompletableFuture<>();
                NativeBridge.streamGetSchema(streamHandle.getPointer(), new ActionListener<>() {
                    @Override
                    public void onResponse(Long addr) { schemaFuture.complete(addr); }
                    @Override
                    public void onFailure(Exception e) { schemaFuture.completeExceptionally(e); }
                });
                Schema schema;
                try (ArrowSchema arrowSchema = ArrowSchema.wrap(schemaFuture.join())) {
                    Field structField = importField(queryAllocator, arrowSchema, dictProvider);
                    schema = new Schema(structField.getChildren(), structField.getMetadata());
                }

                long totalRows = 0;
                while (true) {
                    CompletableFuture<Long> nextFuture = new CompletableFuture<>();
                    NativeBridge.streamNext(runtimeHandle.get(), streamHandle.getPointer(), new ActionListener<>() {
                        @Override
                        public void onResponse(Long addr) { nextFuture.complete(addr); }
                        @Override
                        public void onFailure(Exception e) { nextFuture.completeExceptionally(e); }
                    });
                    long arrayAddr = nextFuture.join();
                    if (arrayAddr == 0) break;

                    VectorSchemaRoot root = VectorSchemaRoot.create(schema, queryAllocator);
                    try (ArrowArray arrowArray = ArrowArray.wrap(arrayAddr)) {
                        Data.importIntoVectorSchemaRoot(queryAllocator, arrowArray, root, dictProvider);
                    }
                    totalRows += root.getRowCount();
                    root.close();
                }
                return totalRows;
            } finally {
                streamHandle.close();
                dictProvider.close();
                queryAllocator.close();
            }
        } finally {
            freeWireConfig(queryConfigPtr);
        }
    }

    private long allocateWireConfig(int channelDepth) {
        Arena arena = Arena.ofAuto();
        MemorySegment seg = arena.allocate(WIRE_CONFIG_LAYOUT);
        seg.set(ValueLayout.JAVA_LONG, 0, (long) BATCH_SIZE); // batch_size
        seg.set(ValueLayout.JAVA_LONG, 8, 4L);                // target_partitions
        seg.set(ValueLayout.JAVA_LONG, 16, 1024L);            // min_skip_run_default
        seg.set(ValueLayout.JAVA_DOUBLE, 24, 0.03);           // min_skip_run_selectivity_threshold
        seg.set(ValueLayout.JAVA_INT, 32, 0);                 // parquet_pushdown_filters
        seg.set(ValueLayout.JAVA_INT, 36, 1);                 // indexed_pushdown_filters
        seg.set(ValueLayout.JAVA_INT, 40, -1);                // force_strategy (None)
        seg.set(ValueLayout.JAVA_INT, 44, -1);                // force_pushdown (None)
        seg.set(ValueLayout.JAVA_INT, 48, 1);                 // cost_predicate
        seg.set(ValueLayout.JAVA_INT, 52, 10);                // cost_collector
        seg.set(ValueLayout.JAVA_INT, 56, 1);                 // max_collector_parallelism
        seg.set(ValueLayout.JAVA_INT, 60, 2);                 // single_collector_strategy (PageRangeSplit)
        seg.set(ValueLayout.JAVA_INT, 64, 1);                 // tree_collector_strategy (TightenOuterBounds)
        seg.set(ValueLayout.JAVA_INT, 68, channelDepth);      // channel_depth
        return seg.address();
    }

    private void freeWireConfig(long ptr) {
        // Arena.ofAuto() handles GC cleanup
    }

    private void printResults(String label, long[] results) {
        java.util.Arrays.sort(results);

        int n = results.length;
        long p50 = results[n / 2];
        long p90 = results[(int)(n * 0.90)];
        long min = results[0];
        long max = results[n - 1];

        System.out.println();
        System.out.println("  ┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐");
        System.out.printf( "  │ %-12s │ %12s │ %12s │ %12s │ %12s │%n", "", "p50 (µs)", "p90 (µs)", "min (µs)", "max (µs)");
        System.out.println("  ├──────────────┼──────────────┼──────────────┼──────────────┼──────────────┤");
        System.out.printf( "  │ %-12s │ %,12d │ %,12d │ %,12d │ %,12d │%n", label, p50, p90, min, max);
        System.out.println("  └──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘");
        System.out.println();
    }
}
